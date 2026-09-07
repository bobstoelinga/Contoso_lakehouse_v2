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


_ALLOWED_DELIVERY_TRANSITIONS = {
  "DETECTED": {"IN_PROGRESS", "QUARANTINED", "SUPERSEDED"},
  "IN_PROGRESS": {"COMPLETE", "QUARANTINED", "SUPERSEDED"},
  "COMPLETE": {"QUARANTINED", "SUPERSEDED"},
  "QUARANTINED": {"COMPLETE", "SUPERSEDED"},
}


class AuditLogger:
    def __init__(self, spark: SparkSession, ctx: RunContext) -> None:
        self.spark = spark
        self.ctx = ctx
        self.audit = f"{ctx.settings.meta_catalog}.audit"

    def _metadata_version(self) -> str:
      rows = self.spark.sql(
        f"""
        SELECT metadata_version FROM {self.audit}.audit_metadata_version
        ORDER BY deployed_at DESC LIMIT 1
        """
      ).collect()
      if not rows:
        raise RuntimeError("Geen actieve metadatarelease geregistreerd.")
      return rows[0].metadata_version

    # -- load runs --------------------------------------------------------
    @contextmanager
    def run(self, layer: str, entity_id: str):
        """Context manager die een load run start en afsluit.

        Bij een exception wordt de run als FAILED weggeschreven en de fout
        opnieuw opgeworpen, zodat de Workflow-taak faalt.
        """
        work_lease_id = None
        if self.ctx.delivery_id and layer != "BRONZE":
            self.plan_work_item(self.ctx.delivery_id, layer, entity_id)
            work_lease_id = self.claim_work_item(self.ctx.delivery_id, layer, entity_id)
            if work_lease_id is None:
                raise RuntimeError(
                    f"Geen uitvoerbaar work-item voor {layer}.{entity_id} in {self.ctx.delivery_id}."
                )

        run_id = str(uuid.uuid4())
        started = datetime.now(timezone.utc)
        metadata_version = self._metadata_version()
        self._write_run_event(run_id, metadata_version, layer, entity_id, "RUNNING", started, {}, None)
        stats: dict[str, int] = {}
        try:
            yield stats
        except Exception as exc:  # noqa: BLE001 - fout moet altijd geregistreerd worden
            self._finish(run_id, metadata_version, layer, entity_id, started, "FAILED", stats, str(exc)[:4000])
            if work_lease_id:
                self.finish_work_item(work_lease_id, succeeded=False, error=str(exc)[:4000])
            raise
        else:
            self._finish(run_id, metadata_version, layer, entity_id, started, "SUCCESS", stats, None)
            if work_lease_id:
                self.finish_work_item(work_lease_id, succeeded=True)

    def _finish(self, run_id, metadata_version, layer, entity_id, started, status, stats, error) -> None:
        self._write_run_event(run_id, metadata_version, layer, entity_id, status, started, stats, error)

    def _write_run_event(self, run_id, metadata_version, layer, entity_id, status, started, stats, error) -> None:
        ended = datetime.now(timezone.utc)
        self.spark.sql(
            f"""
            INSERT INTO {self.audit}.audit_load_run_event VALUES (
              {_sql_str(str(uuid.uuid4()))}, {_sql_str(run_id)}, {_sql_str(self.ctx.batch_id)},
              {_sql_str(self.ctx.delivery_id)}, {_sql_str(metadata_version)}, {_sql_str(layer)},
              {_sql_str(entity_id)}, {_sql_str(status)}, {stats.get('rows_read', 0)},
              {stats.get('rows_inserted', 0)}, {stats.get('rows_updated', 0)},
              {stats.get('rows_rejected', 0)}, {self.ctx.load_date_literal},
              timestamp'{started:%Y-%m-%d %H:%M:%S}', timestamp'{ended:%Y-%m-%d %H:%M:%S}',
              {(ended - started).total_seconds():.3f}, {_sql_str(self.ctx.job_run_id)}, {_sql_str(error)})
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

    def close_delivery_manifest(
        self, delivery_id: str, source_system_id: str, manifest_path: str,
        expected_objects: int, file_count: int, snapshot_complete: bool,
        source_watermark: str | None = None,
    ) -> None:
        """Registreert een pas atomisch gepubliceerde delivery als volledig."""
        self.spark.sql(
            f"""
            MERGE INTO {self.audit}.audit_delivery_manifest t
            USING (SELECT {_sql_str(delivery_id)} AS delivery_id) s
              ON t.delivery_id = s.delivery_id
            WHEN NOT MATCHED THEN INSERT (
              delivery_id, source_system_id, manifest_status, manifest_path,
              expected_object_count, expected_file_count, received_file_count,
              source_watermark, is_snapshot_complete, opened_at, closed_at)
            VALUES (
              {_sql_str(delivery_id)}, {_sql_str(source_system_id)}, 'CLOSED',
              {_sql_str(manifest_path)}, {expected_objects}, {file_count}, {file_count},
              {_sql_str(source_watermark)}, {str(snapshot_complete).lower()},
              current_timestamp(), current_timestamp())
            WHEN MATCHED AND t.manifest_status = 'OPEN' THEN UPDATE SET
              manifest_status = 'CLOSED', manifest_path = {_sql_str(manifest_path)},
              expected_object_count = {expected_objects}, expected_file_count = {file_count},
              received_file_count = {file_count}, source_watermark = {_sql_str(source_watermark)},
              is_snapshot_complete = {str(snapshot_complete).lower()}, closed_at = current_timestamp()
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

    def transition_delivery(
        self, delivery_id: str, to_status: str, changed_by: str, reason: str | None = None,
        approval_reference: str | None = None,
    ) -> None:
        """Voert een toegestane delivery-overgang uit en registreert deze append-only."""
        row = self.spark.sql(
            f"""SELECT delivery_status FROM {self.audit}.audit_delivery
                WHERE delivery_id = {_sql_str(delivery_id)}"""
        ).collect()
        if not row:
          raise ValueError(f"Onbekende delivery_id: {delivery_id}")
        from_status = row[0].delivery_status
        if to_status not in _ALLOWED_DELIVERY_TRANSITIONS.get(from_status, set()):
          raise ValueError(f"Niet-toegestane delivery-overgang: {from_status} -> {to_status}")
        if (
          to_status == "SUPERSEDED" or (from_status == "QUARANTINED" and to_status == "COMPLETE")
        ) and (not reason or not approval_reference):
          raise ValueError(f"{from_status} -> {to_status} vereist reden en approval_reference.")

        status_fields = {
            "QUARANTINED": (
                "quarantined_at = current_timestamp(), quarantine_reason = " + _sql_str(reason)
            ),
            "SUPERSEDED": (
                "superseded_at = current_timestamp(), superseded_by = " + _sql_str(changed_by)
                + ", supersede_reason = " + _sql_str(reason)
                + ", supersede_approval_reference = " + _sql_str(approval_reference)
            ),
            "COMPLETE": (
                "released_at = current_timestamp(), released_by = " + _sql_str(changed_by)
                + ", release_reason = " + _sql_str(reason)
                + ", release_approval_reference = " + _sql_str(approval_reference)
            ),
        }.get(to_status, "")
        suffix = f", {status_fields}" if status_fields else ""
        self.spark.sql(
            f"""
            UPDATE {self.audit}.audit_delivery
            SET delivery_status = {_sql_str(to_status)}{suffix}
            WHERE delivery_id = {_sql_str(delivery_id)}
              AND delivery_status = {_sql_str(from_status)}
            """
        )
        self.spark.sql(
            f"""
            INSERT INTO {self.audit}.audit_delivery_state_transition VALUES (
              {_sql_str(str(uuid.uuid4()))}, {_sql_str(delivery_id)}, {_sql_str(from_status)},
              {_sql_str(to_status)}, {_sql_str(reason)}, {_sql_str(changed_by)},
              {_sql_str(approval_reference)}, current_timestamp())
            """
        )

    def quarantine_delivery(self, delivery_id: str, reason: str) -> None:
        self.transition_delivery(delivery_id, "QUARANTINED", "system", reason)

    # -- control-plane work-items ----------------------------------------
    def plan_work_item(self, delivery_id: str, layer: str, entity_id: str, max_attempts: int = 3) -> None:
        """Plant idempotent een uitvoerbare stap voor een delivery."""
        if max_attempts < 1:
            raise ValueError("max_attempts moet minimaal 1 zijn.")
        self.spark.sql(
            f"""
            MERGE INTO {self.audit}.audit_work_item t
            USING (SELECT {_sql_str(delivery_id)} AS delivery_id,
                          {_sql_str(layer)} AS layer, {_sql_str(entity_id)} AS entity_id) s
              ON t.delivery_id = s.delivery_id AND t.layer = s.layer AND t.entity_id = s.entity_id
            WHEN NOT MATCHED THEN INSERT (
              work_item_id, delivery_id, layer, entity_id, work_status, attempt_count,
              max_attempts, next_attempt_at, lease_id, lease_expires_at, last_error, created_at, updated_at)
            VALUES (
              {_sql_str(str(uuid.uuid4()))}, s.delivery_id, s.layer, s.entity_id, 'PENDING', 0,
              {max_attempts}, current_timestamp(), NULL, NULL, NULL, current_timestamp(), current_timestamp())
            """
        )

    def claim_work_item(self, delivery_id: str, layer: str, entity_id: str) -> str | None:
        """Claim één uitvoerbare work-item met een gefencede lease van vier uur."""
        lease_id = str(uuid.uuid4())
        self.spark.sql(
            f"""
            MERGE INTO {self.audit}.audit_work_item t
            USING (SELECT {_sql_str(delivery_id)} AS delivery_id,
                          {_sql_str(layer)} AS layer, {_sql_str(entity_id)} AS entity_id,
                          {_sql_str(lease_id)} AS lease_id) s
              ON t.delivery_id = s.delivery_id AND t.layer = s.layer AND t.entity_id = s.entity_id
            WHEN MATCHED AND t.attempt_count < t.max_attempts
                 AND t.next_attempt_at <= current_timestamp()
                 AND (t.work_status IN ('PENDING', 'FAILED')
                      OR (t.work_status = 'RUNNING' AND t.lease_expires_at <= current_timestamp()))
              THEN UPDATE SET work_status = 'RUNNING', attempt_count = t.attempt_count + 1,
                              lease_id = s.lease_id,
                              lease_expires_at = current_timestamp() + INTERVAL 4 HOURS,
                              updated_at = current_timestamp()
            """
        )
        rows = self.spark.sql(
            f"""
            SELECT lease_id FROM {self.audit}.audit_work_item
            WHERE delivery_id = {_sql_str(delivery_id)} AND layer = {_sql_str(layer)}
              AND entity_id = {_sql_str(entity_id)} AND lease_id = {_sql_str(lease_id)}
              AND work_status = 'RUNNING' AND lease_expires_at > current_timestamp()
            """
        ).collect()
        return lease_id if rows else None

    def finish_work_item(self, lease_id: str, succeeded: bool, error: str | None = None) -> None:
        """Rond uitsluitend het work-item af dat bij de opgegeven lease hoort."""
        status = "SUCCESS" if succeeded else "FAILED"
        self.spark.sql(
            f"""
            UPDATE {self.audit}.audit_work_item
            SET work_status = CASE
                  WHEN {str(succeeded).lower()} THEN 'SUCCESS'
                  WHEN attempt_count >= max_attempts THEN 'DEAD_LETTER'
                  ELSE '{status}' END,
                lease_id = NULL, lease_expires_at = NULL, last_error = {_sql_str(error)},
                next_attempt_at = CASE WHEN {str(succeeded).lower()} THEN next_attempt_at
                  ELSE current_timestamp() + INTERVAL 5 MINUTES END,
                updated_at = current_timestamp()
            WHERE lease_id = {_sql_str(lease_id)} AND work_status = 'RUNNING'
            """
        )

    def requeue_dead_letter(
        self, delivery_id: str, layer: str, entity_id: str, changed_by: str,
        reason: str, approval_reference: str,
    ) -> None:
        """Heropent uitsluitend een goedgekeurd DEAD_LETTER work-item."""
        if not all((changed_by, reason, approval_reference)):
            raise ValueError("changed_by, reason en approval_reference zijn verplicht.")
        rows = self.spark.sql(
            f"""
            SELECT work_item_id, work_status FROM {self.audit}.audit_work_item
            WHERE delivery_id = {_sql_str(delivery_id)} AND layer = {_sql_str(layer)}
              AND entity_id = {_sql_str(entity_id)}
            """
        ).collect()
        if not rows:
            raise ValueError(f"Work-item bestaat niet: {delivery_id} {layer}.{entity_id}")
        work_item = rows[0]
        if work_item.work_status != "DEAD_LETTER":
            raise ValueError(f"Alleen DEAD_LETTER mag worden heropend, huidige status: {work_item.work_status}")

        self.spark.sql(
            f"""
            UPDATE {self.audit}.audit_work_item
            SET work_status = 'PENDING', attempt_count = 0, next_attempt_at = current_timestamp(),
              lease_id = NULL, lease_expires_at = NULL, updated_at = current_timestamp()
            WHERE work_item_id = {_sql_str(work_item.work_item_id)} AND work_status = 'DEAD_LETTER'
            """
        )
        self.spark.sql(
            f"""
            INSERT INTO {self.audit}.audit_work_item_transition VALUES (
              {_sql_str(str(uuid.uuid4()))}, {_sql_str(work_item.work_item_id)}, {_sql_str(delivery_id)},
              {_sql_str(layer)}, {_sql_str(entity_id)}, 'DEAD_LETTER', 'PENDING', {_sql_str(reason)},
              {_sql_str(changed_by)}, {_sql_str(approval_reference)}, current_timestamp())
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
