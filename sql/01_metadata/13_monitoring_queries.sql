-- =============================================================================
-- 13_monitoring_queries.sql
-- Operationele monitoring voor de metadata-gedreven pipeline.
-- Vervang ${env} met dev, tst of prd wanneer dit script buiten DAB draait.
-- =============================================================================
USE CATALOG contoso_meta_${env};
USE SCHEMA audit;

-- 1. Leveringen per dag: status van de datumfolder en de delivery gate.
SELECT
  delivery_date, delivery_id, source_system_id, delivery_status,
  expected_object_count, loaded_object_count, completed_at
FROM audit_delivery
ORDER BY delivery_date DESC, source_system_id;

-- 1b. De eerstvolgende levering per bron die de chronologische verwerking blokkeert.
SELECT
  delivery_id, source_system_id, delivery_date, delivery_status,
  expected_object_count, success_count, failed_count, pending_count, is_ready
FROM v_next_processable_delivery
ORDER BY source_system_id, delivery_sequence_number;

-- 2. Objectstatus binnen een gekozen levering.
-- Vervang SALES|2026-08-29 door de gewenste delivery_id.
SELECT
  delivery_id, source_object_id, object_status, files_processed, rows_ingested,
  new_columns_detected, started_at, ended_at, error_message
FROM audit_delivery_object
WHERE delivery_id = 'SALES|2026-08-29'
ORDER BY source_object_id;

-- 3. Huidige voortgang per laag en entiteit voor een gekozen levering.
SELECT
  delivery_id, batch_id, layer, entity_id, run_status, rows_read, rows_inserted,
  rows_rejected, started_at, ended_at, round(duration_seconds, 2) AS duration_seconds,
  databricks_job_run_id, error_message
FROM v_load_run_status
WHERE delivery_id = 'SALES|2026-08-29'
ORDER BY started_at, layer, entity_id;

-- 4. Mislukte stappen van de afgelopen zeven dagen, met job-run-id.
SELECT
  delivery_id, batch_id, layer, entity_id, databricks_job_run_id,
  started_at, ended_at, error_message
FROM v_load_run_status
WHERE run_status = 'FAILED'
  AND started_at >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY started_at DESC;

-- 5. Quality-resultaten per levering, object en regel.
SELECT
  delivery_id, source_object_id, rule_id, rule_name, severity, rows_evaluated,
  rows_failed, round(failed_pct, 2) AS failed_pct, threshold_pct,
  threshold_breached, evaluated_at
FROM audit_dq_result
WHERE delivery_id = 'SALES|2026-08-29'
ORDER BY source_object_id, threshold_breached DESC, rule_id;

-- 6. Openstaande rejects per dag en reden.
SELECT
  _delivery_date, source_object_id, reason_code, reason_text, reject_count
FROM contoso_reject_${env}.sales.v_open_rejects
ORDER BY _delivery_date DESC, reject_count DESC, source_object_id;

-- 7. Actieve Gold-publicatie: welke levering voedt momenteel de datamart?
SELECT
  g.publication_group_id, g.batch_id, g.delivery_id, g.published_at,
  p.gold_entity_id, p.physical_slot, p.row_count
FROM audit_gold_publication_group g
JOIN audit_gold_publication p ON p.batch_id = g.batch_id
WHERE g.release_status = 'ACTIVE'
  AND p.publication_status = 'ACTIVE'
ORDER BY g.published_at DESC, p.gold_entity_id;

-- 8. Actualiteit van de publieke actuele datamart.
SELECT *
FROM contoso_gold_${env}.current.v_gold_freshness
ORDER BY entity;

-- 9. Control-plane queue: handmatige actie vereist voor dead letters of verlopen leases.
SELECT delivery_id, layer, entity_id, work_status, attempt_count, max_attempts,
     lease_expires_at, last_error, updated_at
FROM audit_work_item
WHERE work_status = 'DEAD_LETTER'
  OR (work_status = 'RUNNING' AND lease_expires_at < current_timestamp())
ORDER BY updated_at DESC;

-- 10. Verlopen Gold-publicatieleases.
SELECT publication_group_id, batch_id, lease_id, acquired_at, expires_at
FROM audit_gold_publication_lease
WHERE released_at IS NULL AND expires_at < current_timestamp()
ORDER BY expires_at;

-- 11. Mislukte laagreconciliaties; blokkeren vervolgverwerking naar de Vault.
SELECT delivery_id, source_object_id, reconciliation_name, expected_count,
       actual_count, tolerance_count, evaluated_at
FROM audit_reconciliation_result
WHERE reconciliation_status = 'FAILED'
ORDER BY evaluated_at DESC;