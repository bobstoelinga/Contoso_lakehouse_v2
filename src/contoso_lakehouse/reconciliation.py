"""Reconciliaties tussen Lakehouse-lagen.

De eerste verplichte controle bewijst dat Quality geen Bronze-records verliest:
ieder record is na de Quality-filter precies eenmaal passed of rejected.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from pyspark.sql import SparkSession

from contoso_lakehouse.audit import AuditLogger
from contoso_lakehouse.context import RunContext
from contoso_lakehouse.metadata import MetadataRepository, SourceObject
from contoso_lakehouse.sqlutil import sql_string


@dataclass(frozen=True)
class ReconciliationResult:
    source_object_id: str
    expected_count: int
    actual_count: int

    @property
    def passed(self) -> bool:
        return self.expected_count == self.actual_count


class ReconciliationEngine:
    def __init__(self, spark: SparkSession, repo: MetadataRepository, ctx: RunContext) -> None:
        self.spark = spark
        self.repo = repo
        self.ctx = ctx
        self.audit = AuditLogger(spark, ctx)

    def reconcile_quality(self, obj: SourceObject) -> ReconciliationResult:
        """Vergelijkt Quality plus Reject met de gefilterde Bronze-invoer."""
        delivery = sql_string(self.ctx.delivery_id)
        source = sql_string(obj.source_object_id)
        filter_sql = f"AND ({obj.quality_filter_expression})" if obj.quality_filter_expression else ""
        with self.audit.run("RECONCILIATION", obj.source_object_id):
            expected = self.spark.sql(
                f"""
                SELECT count(*) AS n FROM {obj.bronze_table_fqn}
                WHERE _delivery_id = {delivery} {filter_sql}
                """
            ).collect()[0].n
            actual = self.spark.sql(
                f"""
                SELECT
                  (SELECT count(*) FROM {obj.quality_table_fqn} WHERE _delivery_id = {delivery})
                  +
                  (SELECT count(*) FROM {obj.reject_table_fqn}
                   WHERE _delivery_id = {delivery} AND source_object_id = {source}) AS n
                """
            ).collect()[0].n
            status = "PASSED" if expected == actual else "FAILED"
            self.spark.sql(
                f"""
                INSERT INTO {self.ctx.settings.meta_catalog}.audit.audit_reconciliation_result VALUES (
                  {sql_string(str(uuid.uuid4()))}, {delivery}, {source}, 'BRONZE_TO_QUALITY',
                  {expected}, {actual}, 0, '{status}', current_timestamp())
                """
            )
            if status == "FAILED":
                raise RuntimeError(
                    f"{obj.source_object_id}: Bronze-to-Quality reconciliatie faalde: "
                    f"{expected} verwacht, {actual} verwerkt."
                )
            return ReconciliationResult(obj.source_object_id, int(expected), int(actual))