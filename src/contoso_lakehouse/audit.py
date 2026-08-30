"""Audit- en statusregistratie.

Elke stap registreert start, einde en resultaat. De orchestratie leest
uitsluitend deze tabellen om te bepalen wat mag draaien.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from contoso_lakehouse.context import RunContext
from contoso_lakehouse.sqlutil import sql_string as _sql_str


class AuditLogger:
    def __init__(self, spark: SparkSession, ctx: RunContext) -> None:
        self.spark = spark
        self.ctx = ctx
        self.audit = f"{ctx.settings.meta_catalog}.audit"

    # -- load runs --------------------------------------------------------
    @contextmanager
    def run(self, layer: str, entity_id: str):
        """Context manager die een load run start en afsluit.

        Bij een exception wordt de run als FAILED weggeschreven en de fout
        opnieuw opgeworpen, zodat de Workflow-taak faalt.
        """
        run_id = str(uuid.uuid4())
        started = datetime.now(timezone.utc)
        self.spark.sql(
            f"""
            INSERT INTO {self.audit}.audit_load_run
              (run_id, batch_id, delivery_id, layer, entity_id, run_status,
               rows_read, rows_inserted, rows_updated, rows_rejected,
               load_date, started_at, ended_at, duration_seconds,
               databricks_job_run_id, error_message)
            VALUES
              ({_sql_str(run_id)}, {_sql_str(self.ctx.batch_id)},
               {_sql_str(self.ctx.delivery_id)}, {_sql_str(layer)}, {_sql_str(entity_id)},
               'RUNNING', 0, 0, 0, 0,
               {self.ctx.load_date_literal}, timestamp'{started:%Y-%m-%d %H:%M:%S}',
               NULL, NULL, {_sql_str(self.ctx.job_run_id)}, NULL)
            """
        )
        stats: dict[str, int] = {}
        try:
            yield stats
        except Exception as exc:  # noqa: BLE001 - fout moet altijd geregistreerd worden
            self._finish(run_id, started, "FAILED", stats, str(exc)[:4000])
            raise
        else:
            self._finish(run_id, started, "SUCCESS", stats, None)

    def _finish(self, run_id, started, status, stats, error) -> None:
        ended = datetime.now(timezone.utc)
        self.spark.sql(
            f"""
            UPDATE {self.audit}.audit_load_run SET
              run_status       = {_sql_str(status)},
              rows_read        = {stats.get('rows_read', 0)},
              rows_inserted    = {stats.get('rows_inserted', 0)},
              rows_updated     = {stats.get('rows_updated', 0)},
              rows_rejected    = {stats.get('rows_rejected', 0)},
              ended_at         = timestamp'{ended:%Y-%m-%d %H:%M:%S}',
              duration_seconds = {(ended - started).total_seconds():.3f},
              error_message    = {_sql_str(error)}
            WHERE run_id = {_sql_str(run_id)}
            """
        )

    # -- leveringen -------------------------------------------------------
    def register_delivery(
        self, delivery_id: str, source_system_id: str, delivery_date: str,
        folder: str, expected_objects: int, sequence_number: int,
    ) -> None:
        self.spark.sql(
            f"""
            MERGE INTO {self.audit}.audit_delivery t
            USING (SELECT {_sql_str(delivery_id)} AS delivery_id) s
              ON t.delivery_id = s.delivery_id
            WHEN NOT MATCHED THEN INSERT (
              delivery_id, source_system_id, delivery_date, delivery_folder,
              expected_object_count, loaded_object_count, delivery_status,
              delivery_sequence_number, first_seen_at, completed_at)
            VALUES (
              {_sql_str(delivery_id)}, {_sql_str(source_system_id)},
              date'{delivery_date}', {_sql_str(folder)},
              {expected_objects}, 0, 'DETECTED', {sequence_number},
              current_timestamp(), NULL)
            """
        )

    def set_object_status(
        self, delivery_id: str, source_object_id: str, status: str,
        rows: int = 0, files: int = 0, new_columns: list[str] | None = None,
        error: str | None = None,
    ) -> None:
        cols = "array()" if not new_columns else "array(" + ", ".join(_sql_str(c) for c in new_columns) + ")"
        self.spark.sql(
            f"""
            MERGE INTO {self.audit}.audit_delivery_object t
            USING (SELECT {_sql_str(delivery_id)} AS delivery_id,
                          {_sql_str(source_object_id)} AS source_object_id) s
              ON t.delivery_id = s.delivery_id AND t.source_object_id = s.source_object_id
            WHEN MATCHED THEN UPDATE SET
              object_status = {_sql_str(status)},
              files_processed = t.files_processed + {files},
              rows_ingested = t.rows_ingested + {rows},
              new_columns_detected = {cols},
              ended_at = current_timestamp(),
              error_message = {_sql_str(error)}
            WHEN NOT MATCHED THEN INSERT (
              delivery_id, source_object_id, object_status, files_processed,
              rows_ingested, bronze_table_version, new_columns_detected,
              started_at, ended_at, error_message)
            VALUES (
              {_sql_str(delivery_id)}, {_sql_str(source_object_id)}, {_sql_str(status)},
              {files}, {rows}, NULL, {cols},
              current_timestamp(), current_timestamp(), {_sql_str(error)})
            """
        )

    def refresh_delivery_status(self, delivery_id: str) -> None:
        self.spark.sql(
            f"""
            MERGE INTO {self.audit}.audit_delivery t
            USING (
              SELECT delivery_id, success_count, is_ready
              FROM {self.audit}.v_delivery_readiness
              WHERE delivery_id = {_sql_str(delivery_id)}
            ) s ON t.delivery_id = s.delivery_id
            WHEN MATCHED THEN UPDATE SET
              loaded_object_count = s.success_count,
              delivery_status = CASE WHEN s.is_ready THEN 'COMPLETE' ELSE 'IN_PROGRESS' END,
              completed_at = CASE WHEN s.is_ready THEN current_timestamp() ELSE NULL END
            """
        )

    # -- kwaliteit --------------------------------------------------------
    def log_dq_result(self, run_id: str, source_object_id: str, rule, evaluated: int,
                      failed: int) -> bool:
        passed = evaluated - failed
        pct = (failed / evaluated * 100.0) if evaluated else 0.0
        breached = rule.threshold_pct is not None and pct > rule.threshold_pct
        self.spark.sql(
            f"""
            INSERT INTO {self.audit}.audit_dq_result VALUES (
              {_sql_str(str(uuid.uuid4()))}, {_sql_str(run_id)}, {_sql_str(self.ctx.batch_id)},
              {_sql_str(self.ctx.delivery_id)}, {_sql_str(source_object_id)},
              {_sql_str(rule.rule_id)}, {_sql_str(rule.rule_name)}, {_sql_str(rule.severity)},
              {evaluated}, {passed}, {failed}, {pct:.6f},
              {rule.threshold_pct if rule.threshold_pct is not None else 'NULL'},
              {str(breached).lower()}, current_timestamp())
            """
        )
        return breached
