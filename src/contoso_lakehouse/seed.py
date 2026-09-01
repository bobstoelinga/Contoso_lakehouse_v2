"""Laadt de metadata seed-bestanden in de metadatatabellen.

De seed in ``metadata/seed`` is de bron van waarheid en wordt via Git/DAB
gedeployed. In productie zijn de metadatatabellen read-only voor gebruikers;
alleen deze loader (draaiend als service principal) schrijft ze.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from pyspark.sql import SparkSession

from contoso_lakehouse.context import Settings

_SEED_FILES = {
    "meta_source_system": "meta_source_system.json",
    "meta_source_object": "meta_source_object.json",
    "meta_dependency": "meta_dependency.json",
    "meta_quality_rule": "meta_quality_rule.json",
    "meta_mapping": "meta_mapping.json",
    "meta_dv_entity": "meta_dv_entity.json",
    "meta_dv_mapping": "meta_dv_mapping.json",
    "meta_gold_entity": "meta_gold_entity.json",
}

_KEY_COLUMNS = {
    "meta_source_system": "source_system_id",
    "meta_source_object": "source_object_id",
    "meta_dependency": "dependency_id",
    "meta_quality_rule": "rule_id",
    "meta_mapping": "mapping_id",
    "meta_dv_entity": "dv_entity_id",
    "meta_dv_mapping": "dv_mapping_id",
    "meta_gold_entity": "gold_entity_id",
}


def metadata_version(records_by_table: dict[str, list[dict]]) -> str:
    """Geeft een stabiele SHA-256-versie van de volledige metadatarelease."""
    payload = json.dumps(records_by_table, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_seed(spark: SparkSession, settings: Settings, seed_dir: str) -> dict[str, int]:
    """Synchroniseert de metadatatabellen met de seed-bestanden."""
    target_schema = f"{settings.meta_catalog}.metadata"
    counts: dict[str, int] = {}
    seed_records = {
        table: json.loads((Path(seed_dir) / filename).read_text(encoding="utf-8"))
        for table, filename in _SEED_FILES.items()
    }
    version = metadata_version(seed_records)

    for table, filename in _SEED_FILES.items():
        records = seed_records[table]
        if not records:
            continue

        target = spark.table(f"{target_schema}.{table}")
        df = spark.createDataFrame(
            [{k: v for k, v in rec.items()} for rec in records],
            schema=_projection_schema(target, records),
        )
        for column in target.schema.fields:
            if column.name not in df.columns:
                df = df.withColumn(column.name, _default_for(column))
        df = df.select([f.name for f in target.schema.fields])

        key = _KEY_COLUMNS[table]
        df.createOrReplaceTempView(f"_seed_{table}")
        spark.sql(
            f"""
            MERGE INTO {target_schema}.{table} t
            USING _seed_{table} s ON t.{key} = s.{key}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.is_active = false
            """
        )
        counts[table] = len(records)
    spark.sql(
        f"""
        MERGE INTO {settings.meta_catalog}.audit.audit_metadata_version t
        USING (SELECT '{version}' AS metadata_version) s
          ON t.metadata_version = s.metadata_version
        WHEN NOT MATCHED THEN INSERT (metadata_version, deployed_at)
        VALUES (s.metadata_version, current_timestamp())
        """
    )
    return counts


def _projection_schema(target, records):
    """Beperkt het doelschema tot de velden die in de seed voorkomen."""
    from pyspark.sql.types import StructType

    present = {k for rec in records for k in rec}
    return StructType([f for f in target.schema.fields if f.name in present])


def _default_for(field):
    from pyspark.sql import functions as F

    defaults = {
        "is_active": F.lit(True),
        "updated_at": F.current_timestamp(),
        "updated_by": F.expr("current_user()"),
        "valid_from": F.current_timestamp(),
        "evaluation_scope": F.lit("ROW"),
        "on_threshold_breach": F.lit("FAIL_BATCH"),
    }
    return defaults.get(field.name, F.lit(None).cast(field.dataType))
