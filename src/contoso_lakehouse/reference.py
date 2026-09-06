"""Versioned reference-data loads vanuit Quality naar Business Vault."""

from __future__ import annotations

from pyspark.sql import SparkSession

from contoso_lakehouse.audit import AuditLogger
from contoso_lakehouse.context import RunContext
from contoso_lakehouse.metadata import MetadataRepository, SourceObject
from contoso_lakehouse.sqlutil import safe_identifier


def reference_hash_expression(columns: list[str]) -> str:
    """Geeft een stabiele hashexpressie voor de inhoud van een referentierecord."""
    if not columns:
        raise ValueError("Referentiedata vereist minimaal één change-trackingkolom.")
    values = ", ".join(
        f"coalesce(cast({safe_identifier(column)} AS string), '^^')" for column in columns
    )
    return f"sha2(concat_ws('||', {values}), 256)"


class ReferenceDataLoader:
    """Onderhoudt een SCD2-historie voor objecten met route ``REFERENCE_DATA``."""

    def __init__(self, spark: SparkSession, repo: MetadataRepository, ctx: RunContext) -> None:
        self.spark = spark
        self.repo = repo
        self.ctx = ctx
        self.audit = AuditLogger(spark, ctx)

    def _source(self, obj: SourceObject) -> str:
        if obj.processing_route != "REFERENCE_DATA":
            raise ValueError(f"{obj.source_object_id} heeft geen REFERENCE_DATA-route.")
        if not obj.reference_table_fqn:
            raise ValueError(f"{obj.source_object_id} mist een reference-doeltabel.")
        return obj.quality_table_fqn

    def load(self, source_object_id: str) -> None:
        obj = self.repo.source_object(source_object_id)
        source = self._source(obj)
        business_keys = [safe_identifier(column) for column in obj.business_key_columns]
        if not business_keys:
            raise ValueError(f"{source_object_id} vereist minimaal één business key.")
        hash_expression = reference_hash_expression(obj.change_tracking_columns)
        view = f"_reference_{obj.source_object_id.replace('.', '_').lower()}"
        key_match = " AND ".join(f"t.{key} <=> s.{key}" for key in business_keys)

        with self.audit.run("BUSINESS_VAULT", source_object_id):
            self.spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {obj.reference_table_fqn} AS
                SELECT q.*, cast(NULL AS string) AS _reference_hash,
                       cast(NULL AS timestamp) AS valid_from,
                       cast(NULL AS timestamp) AS valid_to,
                       cast(NULL AS boolean) AS is_current
                FROM {source} q WHERE 1 = 0
                """
            )
            self.spark.sql(
                f"""
                CREATE OR REPLACE TEMP VIEW {view} AS
                SELECT q.*, {hash_expression} AS _reference_hash,
                       {self.ctx.load_date_literal} AS valid_from,
                       timestamp'9999-12-31 23:59:59' AS valid_to, true AS is_current
                FROM {source} q WHERE q._delivery_id = '{self.ctx.delivery_id}'
                """
            )
            self.spark.sql(
                f"""
                UPDATE {obj.reference_table_fqn} t SET
                  valid_to = {self.ctx.load_date_literal}, is_current = false
                WHERE t.is_current AND EXISTS (
                  SELECT 1 FROM {view} s
                  WHERE {key_match} AND t._reference_hash <> s._reference_hash
                )
                """
            )
            self.spark.sql(
                f"""
                INSERT INTO {obj.reference_table_fqn}
                SELECT s.* FROM {view} s WHERE NOT EXISTS (
                  SELECT 1 FROM {obj.reference_table_fqn} t
                  WHERE t.is_current AND {key_match} AND t._reference_hash = s._reference_hash
                )
                """
            )