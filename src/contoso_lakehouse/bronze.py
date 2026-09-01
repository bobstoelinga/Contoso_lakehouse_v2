"""Bronze ingest met Auto Loader.

Ontwerpbesluit: de stream detecteert bestanden, maar de *verwerking* gebeurt per
levering in ``foreachBatch``. Zo blijft de leverings-gate afdwingbaar, ook als
een micro-batch bestanden uit meerdere datumfolders bevat (backfill, storing).
"""

from __future__ import annotations

import re
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, functions as F

from contoso_lakehouse.audit import AuditLogger
from contoso_lakehouse.context import RunContext
from contoso_lakehouse.metadata import MetadataRepository, SourceObject, safe_identifier

_DELIVERY_DATE = re.compile(r"/(\d{4}-\d{2}-\d{2})/")


class SchemaDriftError(RuntimeError):
    """Een bronkolom voldoet niet aan het schema-driftbeleid."""


def _delivery_date_expr(path_col: str = "_metadata.file_path"):
    """Haalt de datumfolder uit het bestandspad: /Volumes/.../yyyy-MM-dd/x.parquet."""
    return F.to_date(F.regexp_extract(F.col(path_col), r"/(\d{4}-\d{2}-\d{2})/", 1), "yyyy-MM-dd")


class BronzeLoader:
    def __init__(self, spark: SparkSession, repo: MetadataRepository, ctx: RunContext) -> None:
        self.spark = spark
        self.repo = repo
        self.ctx = ctx
        self.audit = AuditLogger(spark, ctx)

    # -- stream opbouw ----------------------------------------------------
    def _read_stream(self, obj: SourceObject, landing_path: str) -> DataFrame:
        evolution_mode = {
            "STRICT": "failOnNewColumns",
            "RESCUE": "rescue",
        }.get(obj.schema_drift_policy, obj.schema_evolution_mode)
        reader = (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", obj.file_format)
            .option("cloudFiles.schemaLocation", obj.schema_location_path)
            .option("cloudFiles.schemaEvolutionMode", evolution_mode)
            .option("cloudFiles.maxFilesPerTrigger", obj.max_files_per_trigger)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .option("pathGlobFilter", obj.file_pattern)
            .option("recursiveFileLookup", "true")
        )
        for key, value in obj.reader_options.items():
            reader = reader.option(key, value)

        return (
            reader.load(landing_path)
            .withColumn("_source_file_path", F.col("_metadata.file_path"))
            .withColumn("_source_file_name", F.col("_metadata.file_name"))
            .withColumn("_source_file_size", F.col("_metadata.file_size"))
            .withColumn("_source_file_mtime", F.col("_metadata.file_modification_time"))
            .withColumn("_delivery_date", _delivery_date_expr())
            .withColumn(
                "_delivery_id",
                F.concat_ws("|", F.lit(obj.source_system_id), F.date_format("_delivery_date", "yyyy-MM-dd")),
            )
            .withColumn("_ingest_timestamp", F.current_timestamp())
            .withColumn("_batch_id", F.lit(self.ctx.batch_id))
            .withColumn("_record_source", F.lit(obj.source_object_id))
        )

    # -- micro-batch verwerking -------------------------------------------
    def _process_batch(self, obj: SourceObject):
        def handler(batch_df: DataFrame, _batch_id: int) -> None:
            if batch_df.isEmpty():
                return
            deliveries = [
                r["_delivery_id"]
                for r in batch_df.select("_delivery_id").distinct().collect()
            ]
            # Chronologisch verwerken; anders raakt SCD2-historie corrupt.
            for delivery_id in sorted(deliveries):
                slice_df = batch_df.where(F.col("_delivery_id") == delivery_id)
                rows = slice_df.count()
                files = slice_df.select("_source_file_path").distinct().count()
                self._register(obj, delivery_id, slice_df)
                new_columns = self._new_columns(obj, slice_df)
                try:
                    self._validate_schema_drift(obj, new_columns)
                except SchemaDriftError as exc:
                    self.audit.set_object_status(
                        delivery_id, obj.source_object_id, "FAILED", rows=rows, files=files,
                        new_columns=new_columns, error=str(exc),
                    )
                    self.audit.refresh_delivery_status(delivery_id)
                    raise
                self._merge_bronze_slice(obj, slice_df)
                self.audit.set_object_status(
                    delivery_id, obj.source_object_id, "SUCCESS",
                    rows=rows, files=files,
                    new_columns=new_columns,
                )
                self.audit.refresh_delivery_status(delivery_id)

        return handler

    def _merge_bronze_slice(self, obj: SourceObject, slice_df: DataFrame) -> None:
        """Schrijft een levering volgens de metadata-gedefinieerde laadstrategie."""
        source_view = f"_bronze_{obj.source_object_id.replace('.', '_').lower()}"
        keys = [safe_identifier(column) for column in obj.business_key_columns]
        dedupe_columns = ["_source_file_path", "_delivery_id", *keys]
        slice_df.dropDuplicates(dedupe_columns).createOrReplaceTempView(source_view)
        key_match = " AND ".join(f"t.{column} <=> s.{column}" for column in keys)

        if obj.load_strategy in {"INCREMENTAL_APPEND", "SNAPSHOT_SCD2"}:
            match_clause = (
                "t._source_file_path = s._source_file_path\n"
                "             AND t._delivery_id = s._delivery_id\n"
                f"             AND {key_match}"
            )
        elif obj.load_strategy == "INCREMENTAL_MERGE":
            match_clause = key_match
        elif obj.load_strategy == "FULL_OVERWRITE":
            self.spark.sql(
                f"CREATE OR REPLACE TABLE {obj.bronze_table_fqn} AS SELECT * FROM {source_view}"
            )
            return
        else:
            raise ValueError(
                f"Niet-ondersteunde load_strategy voor {obj.source_object_id}: {obj.load_strategy}"
            )

        if not key_match:
            raise ValueError(f"{obj.source_object_id}: load_strategy vereist minimaal één business key.")
        self.spark.sql(
            f"""
                        MERGE WITH SCHEMA EVOLUTION INTO {obj.bronze_table_fqn} t
            USING {source_view} s
              ON {match_clause}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )

    def _register(self, obj: SourceObject, delivery_id: str, df: DataFrame) -> None:
        delivery_date = delivery_id.split("|", 1)[1]
        expected = len(self.repo.mandatory_objects(obj.source_system_id))
        sequence = int(datetime.strptime(delivery_date, "%Y-%m-%d").strftime("%Y%m%d"))
        folder = df.select("_source_file_path").first()[0].rsplit("/", 1)[0]
        self.audit.register_delivery(
            delivery_id, obj.source_system_id, delivery_date, folder, expected, sequence
        )
        self.audit.set_object_status(delivery_id, obj.source_object_id, "RUNNING")

    def _new_columns(self, obj: SourceObject, df: DataFrame) -> list[str]:
        """Kolommen die in de bron zitten maar nog geen QUALITY-mapping hebben."""
        mapped = {m.source_column for m in self.repo.mappings(obj.source_object_id, "QUALITY")}
        return [c for c in df.columns if not c.startswith("_") and c not in mapped]

    def _validate_schema_drift(self, obj: SourceObject, new_columns: list[str]) -> None:
        if not new_columns:
            return
        if obj.schema_drift_policy == "RESCUE":
            return
        if obj.schema_drift_policy == "STRICT":
            raise SchemaDriftError(
                f"{obj.source_object_id}: onbekende bronkolommen bij STRICT: {', '.join(new_columns)}"
            )
        if obj.schema_drift_policy == "ALLOW_NEW_COLUMNS_WITH_APPROVAL":
            raise SchemaDriftError(
                f"{obj.source_object_id}: bronkolommen vereisen QUALITY-mappinggoedkeuring: "
                f"{', '.join(new_columns)}"
            )
        raise SchemaDriftError(
            f"{obj.source_object_id}: onbekend schema_drift_policy: {obj.schema_drift_policy}"
        )

    # -- publieke API -----------------------------------------------------
    def has_input_files(self, obj: SourceObject, landing_path: str) -> bool:
        """Voorkomt een Auto Loader-fout bij een lege landingzone.

        Een lege landing is bij file-arrival polling geen fout. De delivery-gate
        besluit vervolgens dat er geen complete levering beschikbaar is.
        """
        return bool(
            self.spark.read.format("binaryFile")
            .option("pathGlobFilter", obj.file_pattern)
            .option("recursiveFileLookup", "true")
            .load(landing_path)
            .limit(1)
            .count()
        )

    def load(self, source_object_id: str, landing_path: str, once: bool = True) -> None:
        obj = self.repo.source_object(source_object_id)
        if not self.has_input_files(obj, landing_path):
            return
        with self.audit.run("BRONZE", source_object_id):
            writer = (
                self._read_stream(obj, landing_path)
                .writeStream.foreachBatch(self._process_batch(obj))
                .option("checkpointLocation", obj.checkpoint_path)
                .queryName(f"bronze_{obj.object_name}")
            )
            writer = (
                writer.trigger(availableNow=True) if once
                else writer.trigger(processingTime="1 minute")
            )
            writer.start().awaitTermination()
