"""Quality- en Reject-laag.

Alle regels van één bronobject worden in één enkele pass geëvalueerd: elke regel
wordt een extra boolean kolom. Dat voorkomt N full scans bij N regels.
"""

from __future__ import annotations

import json
import uuid

from pyspark.sql import DataFrame, SparkSession, functions as F

from contoso_lakehouse.audit import AuditLogger
from contoso_lakehouse.context import RunContext
from contoso_lakehouse.metadata import MetadataRepository, QualityRule, safe_identifier


class QualityThresholdBreached(RuntimeError):
    """Wordt opgeworpen wanneer een regel met FAIL_BATCH zijn drempel overschrijdt."""


class QualityEngine:
    def __init__(self, spark: SparkSession, repo: MetadataRepository, ctx: RunContext) -> None:
        self.spark = spark
        self.repo = repo
        self.ctx = ctx
        self.audit = AuditLogger(spark, ctx)

    # -- transformatie volgens meta_mapping -------------------------------
    def _typed_projection(self, source_object_id: str) -> str:
        mappings = self.repo.mappings(source_object_id, "QUALITY")
        if not mappings:
            raise ValueError(f"Geen QUALITY-mapping gevonden voor {source_object_id}")
        parts = []
        for m in mappings:
            expr = m.source_expression or m.source_column
            target = safe_identifier(m.target_column)
            parts.append(f"cast({expr} AS {m.target_data_type}) AS {target}")
        return ", ".join(parts)

    # -- regels -----------------------------------------------------------
    @staticmethod
    def _rule_column(rule: QualityRule) -> str:
        return f"_rule_{rule.rule_id.replace('-', '_').lower()}"

    def _evaluate(self, df: DataFrame, rules: list[QualityRule]) -> DataFrame:
        """Voegt per regel één boolean kolom toe; alle regels in één pass."""
        for rule in rules:
            df = df.withColumn(self._rule_column(rule), F.expr(rule.rule_expression))
        return df

    # -- publieke API -----------------------------------------------------
    def run(self, source_object_id: str, delivery_id: str) -> dict[str, int]:
        obj = self.repo.source_object(source_object_id)
        rules = self.repo.quality_rules(source_object_id)
        errors = [r for r in rules if r.severity == "ERROR"]
        warnings = [r for r in rules if r.severity == "WARNING"]

        with self.audit.run("QUALITY", source_object_id) as stats:
            source = self.spark.sql(
                f"""
                SELECT {self._typed_projection(source_object_id)},
                       _delivery_id, _delivery_date, _batch_id, _record_source,
                       to_json(struct(*)) AS _payload
                FROM {obj.bronze_table_fqn}
                WHERE _delivery_id = '{delivery_id}'
                """
            )
            evaluated = self._evaluate(source, rules).persist()
            total = evaluated.count()
            stats["rows_read"] = total

            breach = self._log_results(evaluated, rules, total, source_object_id)
            if breach:
                evaluated.unpersist()
                raise QualityThresholdBreached(
                    f"{source_object_id}: drempel overschreden voor {', '.join(breach)}"
                )

            error_cols = [self._rule_column(r) for r in errors]
            passed_expr = F.lit(True)
            for col in error_cols:
                passed_expr = passed_expr & F.coalesce(F.col(col), F.lit(False))

            business_key = F.concat_ws("|", *[F.col(c) for c in obj.business_key_columns])
            warning_codes = F.array_compact(
                F.array(*[
                    F.when(~F.coalesce(F.col(self._rule_column(r)), F.lit(False)), F.lit(r.reject_reason_code))
                    for r in warnings
                ])
            ) if warnings else F.array()

            passed = evaluated.where(passed_expr)
            rejected = evaluated.where(~passed_expr)

            target_columns = [
                c for c in source.columns if not c.startswith("_rule_") and c != "_payload"
            ]
            (
                passed.select(
                    *target_columns,
                    F.when(F.size(warning_codes) > 0, F.lit("PASSED_WITH_WARNINGS"))
                     .otherwise(F.lit("PASSED")).alias("_quality_status"),
                    warning_codes.alias("_warning_codes"),
                    F.current_timestamp().alias("_processed_at"),
                )
                .write.format("delta").mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(obj.quality_table_fqn)
            )

            failed_rules_expr = F.array_compact(
                F.array(*[
                    F.when(
                        ~F.coalesce(F.col(self._rule_column(r)), F.lit(False)),
                        F.struct(
                            F.lit(r.rule_id).alias("rule_id"),
                            F.lit(r.rule_name).alias("rule_name"),
                            F.lit(r.severity).alias("severity"),
                            F.lit(r.reject_reason_code).alias("reason_code"),
                            F.lit(r.reject_reason_text).alias("reason_text"),
                        ),
                    )
                    for r in errors
                ])
            ) if errors else F.array()

            (
                rejected.select(
                    F.expr("uuid()").alias("reject_id"),
                    "_delivery_id", "_delivery_date", "_batch_id",
                    F.lit(str(uuid.uuid4())).alias("_run_id"),
                    "_record_source",
                    F.lit(source_object_id).alias("source_object_id"),
                    business_key.alias("business_key"),
                    F.col("_payload").alias("payload"),
                    failed_rules_expr.alias("failed_rules"),
                    F.lit("OPEN").alias("reject_status"),
                    F.lit(None).cast("timestamp").alias("resolved_at"),
                    F.lit(None).cast("string").alias("resolved_by"),
                    F.lit(None).cast("string").alias("resubmitted_batch_id"),
                    F.current_timestamp().alias("rejected_at"),
                )
                .write.format("delta").mode("append").saveAsTable(obj.reject_table_fqn)
            )

            stats["rows_inserted"] = passed.count()
            stats["rows_rejected"] = total - stats["rows_inserted"]
            evaluated.unpersist()
            return dict(stats)

    def _log_results(self, df: DataFrame, rules, total: int, source_object_id: str) -> list[str]:
        """Meet elke regel in één aggregatie en registreert het resultaat."""
        if not rules:
            return []
        agg = df.agg(*[
            F.count_if(~F.coalesce(F.col(self._rule_column(r)), F.lit(False))).alias(self._rule_column(r))
            for r in rules
        ]).collect()[0]

        run_id = str(uuid.uuid4())
        breached: list[str] = []
        for rule in rules:
            failed = int(agg[self._rule_column(rule)] or 0)
            if self.audit.log_dq_result(run_id, source_object_id, rule, total, failed):
                breached.append(rule.rule_name)
        return breached
