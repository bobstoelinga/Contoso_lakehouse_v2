"""Tests op de logica die zonder Spark te controleren is."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from contoso_lakehouse.context import Settings
from contoso_lakehouse.audit import AuditLogger
from contoso_lakehouse.bronze import BronzeLoader, SchemaDriftError
from contoso_lakehouse.gold import GoldLoader
from contoso_lakehouse.hashing import hash_key, hashdiff
from contoso_lakehouse.metadata import DvEntity, GoldEntity, MetadataRepository, SourceObject
from contoso_lakehouse.datavault import VaultLoader
from contoso_lakehouse.orchestration import (
    DependencyCycleError,
    GateNotOpenError,
    Orchestrator,
    parallel_execution_waves,
)
from contoso_lakehouse.quality import QualityBatchQuarantined, QualityEngine
from contoso_lakehouse.reconciliation import ReconciliationResult
from contoso_lakehouse.release_check import validate_seed_release
from contoso_lakehouse.seed import metadata_version
from contoso_lakehouse.release_check import validate_onboarding_package
from contoso_lakehouse.sqlutil import safe_identifier
from contoso_lakehouse.validation import MetadataValidator

SEED_DIR = Path(__file__).resolve().parents[1] / "metadata" / "seed"
METADATA_DDL = (
    Path(__file__).resolve().parents[1] / "sql" / "01_metadata" / "10_metadata_model.sql"
)
METADATA_MIGRATIONS = (
    Path(__file__).resolve().parents[1] / "sql" / "01_metadata" / "12_metadata_migrations.sql"
)
PIPELINE_WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "pipeline.job.yml"
CBS_LOAD_WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "cbs_jeugdzorg_wijk_load.job.yml"
ECB_LOAD_WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "ecb_exchange_rates_load.job.yml"
FABRIC_LOAD_WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "fabric_sales_order_lines_load.job.yml"
NAGER_LOAD_WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "nager_holidays_nl_load.job.yml"
ACTIVE_SOURCES_ACCEPTANCE_WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "active_sources_acceptance.job.yml"
SETUP_NOTEBOOK = Path(__file__).resolve().parents[1] / "notebooks" / "00_setup_lakehouse.py"
SETUP_WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "setup.job.yml"
CATALOGS_DDL = (
    Path(__file__).resolve().parents[1] / "sql" / "00_unity_catalog" / "00_catalogs_schemas_volumes.sql"
)
BUNDLE_CONFIG = Path(__file__).resolve().parents[1] / "databricks.yml"
GOLD_CONSUMER_GRANTS = (
    Path(__file__).resolve().parents[1] / "sql" / "00_unity_catalog" / "02_gold_consumer_grants.sql"
)


def _seed(name: str):
    return json.loads((SEED_DIR / f"{name}.json").read_text(encoding="utf-8"))


# -- hashing ---------------------------------------------------------------
def test_hash_key_is_deterministic():
    assert hash_key(["customer_key"], "SALES") == hash_key(["customer_key"], "SALES")


def test_hash_key_includes_collision_code():
    assert "'SALES'" in hash_key(["customer_key"], "SALES")


def test_hash_key_normalises_nulls():
    assert "'^^'" in hash_key(["customer_key"])


def test_hash_key_rejects_empty_input():
    with pytest.raises(ValueError):
        hash_key([])


def test_hashdiff_is_order_sensitive():
    assert hashdiff(["a", "b"]) != hashdiff(["b", "a"])


def test_satellite_state_table_is_derived_from_the_physical_history_table():
    entity = DvEntity(
        "SAT_CUSTOMER", "SATELLITE", "RAW_VAULT", "vault.raw.sat_customer",
        "vault.raw.sat_customer_h", "hk_customer", ["HUB_CUSTOMER"], [], "hashdiff", "'SALES'", 10,
    )

    assert VaultLoader._satellite_state_table(entity) == "vault.raw.sat_customer_h__current_state"


def test_satellite_loader_uses_compact_current_state_for_daily_hashdiff_comparison():
    loader = (Path(__file__).resolve().parents[1] / "src" / "contoso_lakehouse" / "datavault.py").read_text(encoding="utf-8")

    assert "__current_state" in loader
    assert "SELECT {entity.hash_key_column}, hashdiff FROM {state}" in loader
    assert "MERGE INTO {state} t" in loader


def test_reconciliation_result_requires_equal_expected_and_actual_counts():
    assert ReconciliationResult("SALES.ORDERS", 10, 10).passed
    assert not ReconciliationResult("SALES.ORDERS", 10, 9).passed


# -- identifiers -----------------------------------------------------------
@pytest.mark.parametrize("value", ["contoso_gold.current.dim_customer", "hk_customer"])
def test_safe_identifier_accepts_valid(value):
    assert safe_identifier(value) == value


@pytest.mark.parametrize("value", ["a; DROP TABLE x", "a b", "", "a'b"])
def test_safe_identifier_rejects_injection(value):
    with pytest.raises(ValueError):
        safe_identifier(value)


# -- settings --------------------------------------------------------------
def test_settings_resolve_placeholders():
    s = Settings(env="tst")
    assert s.resolve("contoso_bronze_${env}.sales") == "contoso_bronze_tst.sales"
    assert s.resolve("{gold_catalog}.current") == "contoso_gold_tst.current"


def test_quality_rule_expressions_resolve_all_placeholders():
    settings = Settings(env="tst")

    for rule in _seed("meta_quality_rule"):
        expression = settings.resolve(rule["rule_expression"])
        assert not re.search(r"\{[a-z_]+\}", expression), rule["rule_id"]


def test_quality_threshold_failure_requires_a_blocking_fail_batch_rule():
    warning = type("Rule", (), {"rule_name": "warning_rule", "is_blocking": False, "on_threshold_breach": "FAIL_BATCH"})()
    warn_only = type("Rule", (), {"rule_name": "warn_only_rule", "is_blocking": True, "on_threshold_breach": "WARN_ONLY"})()
    blocking = type("Rule", (), {"rule_name": "blocking_rule", "is_blocking": True, "on_threshold_breach": "FAIL_BATCH"})()

    assert QualityEngine._failing_threshold_rules([warning, warn_only, blocking]) == ["blocking_rule"]


def test_quarantine_threshold_policy_is_excluded_from_failure_filter():
    quarantined = type("Rule", (), {
        "rule_name": "quarantine_rule", "is_blocking": True, "on_threshold_breach": "QUARANTINE_BATCH"
    })()

    assert QualityEngine._failing_threshold_rules([quarantined]) == []
    assert issubclass(QualityBatchQuarantined, RuntimeError)


def test_metadata_version_is_deterministic_across_json_key_order():
    first = {"meta_source_object": [{"object_name": "orders", "load_order": 10}]}
    reordered = {"meta_source_object": [{"load_order": 10, "object_name": "orders"}]}

    assert metadata_version(first) == metadata_version(reordered)


def test_scoped_onboarding_package_validation_keeps_partials_safe(tmp_path):
    (tmp_path / "meta_source_system.json").write_text('[{"source_system_id":"CRM","is_active":false}]', encoding="utf-8")
    (tmp_path / "meta_source_object.json").write_text('[{"source_object_id":"CRM.CUSTOMERS","is_active":false}]', encoding="utf-8")
    for filename in ("meta_source_connector.json", "meta_mapping.json", "meta_quality_rule.json"):
        (tmp_path / filename).write_text("[]", encoding="utf-8")
    assert validate_onboarding_package(tmp_path, "BRON_ONLY") == []
    assert validate_onboarding_package(tmp_path, "BRON_AND_VAULT")


def test_local_release_check_accepts_the_complete_git_seed_release():
    version, issues = validate_seed_release(SEED_DIR)

    assert len(version) == 64
    assert issues == []


def test_local_release_check_reports_invalid_maintenance_retention(tmp_path):
    for path in SEED_DIR.glob("*.json"):
        (tmp_path / path.name).write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    policy_path = tmp_path / "meta_table_maintenance_policy.json"
    policies = json.loads(policy_path.read_text(encoding="utf-8"))
    policies[0]["vacuum_retain_hours"] = 24
    policy_path.write_text(json.dumps(policies), encoding="utf-8")

    _, issues = validate_seed_release(tmp_path)

    assert any("korter dan 168 uur" in issue for issue in issues)


def test_governance_policies_cover_active_sources_and_use_valid_classifications():
    source_systems = _seed("meta_source_system")
    policies = {policy["source_system_id"]: policy for policy in _seed("meta_data_governance_policy")}
    active_sources = {source["source_system_id"] for source in source_systems if source["is_active"]}

    assert active_sources <= policies.keys()
    assert {policy["pii_classification"] for policy in policies.values()} <= {
        "PUBLIC", "INTERNAL", "CONFIDENTIAL", "RESTRICTED",
    }
    assert all(policy["retention_days"] > 0 for policy in policies.values())


def test_gold_data_products_cover_active_current_publication_groups():
    entities = _seed("meta_gold_entity")
    products = {product["publication_group_id"]: product for product in _seed("meta_gold_data_product")}
    current_groups = {
        entity["publication_group_id"] for entity in entities
        if entity["is_active"] and entity["gold_layer"] == "CURRENT" and entity.get("publication_group_id")
    }

    assert current_groups <= products.keys()
    assert all(product["refresh_sla_hours"] > 0 for product in products.values())


def test_parallel_execution_waves_respect_dependencies_and_worker_limit():
    dependencies = {
        "HUB_CUSTOMER": set(),
        "HUB_PRODUCT": set(),
        "LNK_ORDER_PRODUCT": {"HUB_CUSTOMER", "HUB_PRODUCT"},
        "SAT_ORDER_LINE": {"LNK_ORDER_PRODUCT"},
    }

    assert parallel_execution_waves(dependencies, max_parallelism=2) == [
        ["HUB_CUSTOMER", "HUB_PRODUCT"],
        ["LNK_ORDER_PRODUCT"],
        ["SAT_ORDER_LINE"],
    ]


def test_parallel_execution_waves_prioritise_ready_work_and_reject_cycles():
    assert parallel_execution_waves(
        {"LOW": set(), "HIGH": set()}, max_parallelism=1, priorities={"LOW": 20, "HIGH": 10}
    ) == [["HIGH"], ["LOW"]]

    with pytest.raises(DependencyCycleError):
        parallel_execution_waves({"A": {"B"}, "B": {"A"}}, max_parallelism=2)

    with pytest.raises(ValueError, match="minimaal 1"):
        parallel_execution_waves({"A": set()}, max_parallelism=0)


def test_vault_planning_excludes_reference_data_and_other_source_systems():
    sales_source = SourceObject(
        "SALES.CUSTOMERS", "SALES", "customers", "*.parquet", "parquet", {},
        "SNAPSHOT_SCD2", ["customer_key"], ["customer_name"], None, True,
    )
    reference_source = SourceObject(
        "CBS.GEMEENTEN", "CBS", "gemeenten", "*.parquet", "parquet", {},
        "SNAPSHOT_SCD2", ["gemeente_code"], ["gemeente_naam"], None, True,
        processing_route="REFERENCE_DATA",
    )
    entity = DvEntity(
        "HUB_CUSTOMER", "HUB", "RAW_VAULT", "vault.raw.hub_customer",
        "vault.raw.hub_customer", "hk_customer", [], ["customer_key"], None, "'SALES'", 10,
    )
    repository = object.__new__(MetadataRepository)
    repository.dv_entities = lambda: (entity,)
    repository.dv_mappings = lambda _: [type("Mapping", (), {"source_object_id": "SALES.CUSTOMERS"})()]
    repository.source_object = lambda source_id: {
        sales_source.source_object_id: sales_source,
        reference_source.source_object_id: reference_source,
    }[source_id]

    assert repository.vault_entities_for_source_system("SALES", "RAW_VAULT") == (entity,)

    repository.dv_mappings = lambda _: [type("Mapping", (), {"source_object_id": "CBS.GEMEENTEN"})()]
    assert repository.vault_entities_for_source_system("CBS", "RAW_VAULT") == ()


def test_gold_metadata_is_scoped_to_the_delivery_source_system():
    sales_gold = GoldEntity(
        gold_entity_id="GC_SALES", gold_layer="CURRENT", entity_type="DIMENSION",
        target_table_fqn="gold.current.sales", target_catalog="gold", target_schema="current",
        target_table="sales", select_sql="SELECT 1", business_key_columns=["id"],
        scd_type="SNAPSHOT", publish_mode="ATOMIC_SWAP", publication_group_id="SALES_MART",
        depends_on_gold_entity_ids=[], load_order=1, source_system_id="SALES",
    )
    cbs_gold = GoldEntity(
        gold_entity_id="GC_CBS", gold_layer="CURRENT", entity_type="FACT",
        target_table_fqn="gold.current.cbs", target_catalog="gold", target_schema="current",
        target_table="cbs", select_sql="SELECT 1", business_key_columns=["id"],
        scd_type="SNAPSHOT", publish_mode="ATOMIC_SWAP", publication_group_id="CBS_MART",
        depends_on_gold_entity_ids=[], load_order=1, source_system_id="CBS",
    )
    repository = object.__new__(MetadataRepository)
    repository.gold_entities = lambda: (sales_gold, cbs_gold)

    assert repository.gold_entities_for_source_system("SALES") == (sales_gold,)
    assert repository.gold_entities_for_source_system("CBS") == (cbs_gold,)


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows


class _RecordingSpark:
    def __init__(self, rows_by_fragment=None):
        self.rows_by_fragment = rows_by_fragment or {}
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)
        for fragment, rows in self.rows_by_fragment.items():
            if fragment in statement:
                return _Rows(rows)
        return _Rows([])


def _orchestrator(rows_by_fragment=None):
    context = type("Context", (), {
        "settings": Settings(env="tst"), "delivery_id": "SALES|2026-09-01", "batch_id": "batch-1"
    })()
    repo = type("Repository", (), {"dependencies_for": lambda *_args: []})()
    return Orchestrator(_RecordingSpark(rows_by_fragment), repo, context)


def test_orchestrator_delivery_gates_stop_incomplete_or_unknown_deliveries():
    orchestrator = _orchestrator({
        "v_next_processable_delivery": [type("Delivery", (), {"delivery_id": "SALES|2026-09-01", "is_ready": False})()],
        "v_delivery_readiness": [type("Readiness", (), {"is_ready": False, "success_count": 2, "expected_object_count": 3, "failed_count": 0})()],
    })

    assert orchestrator.next_delivery("SALES") is None
    with pytest.raises(GateNotOpenError, match="2/3 geladen"):
        orchestrator.require_delivery_complete("SALES|2026-09-01")

    with pytest.raises(GateNotOpenError, match="niet geregistreerd"):
        _orchestrator().require_delivery_complete("SALES|2026-09-01")


def test_orchestrator_requires_each_blocking_upstream_success():
    dependency = type("Dependency", (), {
        "dependency_type": "ENTITY", "depends_on_entity_id": "HUB_CUSTOMER", "depends_on_layer": "RAW_VAULT"
    })()
    repo = type("Repository", (), {"dependencies_for": lambda *_args: [dependency]})()
    context = type("Context", (), {
        "settings": Settings(env="tst"), "delivery_id": "SALES|2026-09-01", "batch_id": "batch-1"
    })()
    spark = _RecordingSpark({"v_load_run_status": [type("Count", (), {"n": 0})()]})

    with pytest.raises(GateNotOpenError, match="RAW_VAULT.HUB_CUSTOMER"):
        Orchestrator(spark, repo, context).require_upstream_success("SAT_CUSTOMER", "RAW_VAULT")


def test_same_delivery_dependency_scopes_the_upstream_status_to_delivery():
    dependency = type("Dependency", (), {
        "dependency_type": "SAME_DELIVERY", "depends_on_entity_id": "HUB_CUSTOMER",
        "depends_on_layer": "RAW_VAULT",
    })()
    repo = type("Repository", (), {"dependencies_for": lambda *_args: [dependency]})()
    context = type("Context", (), {
        "settings": Settings(env="tst"), "delivery_id": "SALES|2026-09-01", "batch_id": "batch-1",
    })()
    spark = _RecordingSpark({"v_load_run_status": [type("Count", (), {"n": 1})()]})

    Orchestrator(spark, repo, context).require_upstream_success("LNK_ORDER_CUSTOMER", "RAW_VAULT")

    assert "AND delivery_id = 'SALES|2026-09-01'" in spark.statements[-1]


def test_gold_group_marks_new_publications_active_before_switching_pointer():
    loader = object.__new__(GoldLoader)
    loader.spark = _RecordingSpark({
        "building_count": [type("Count", (), {"building_count": 2})()],
        "active_count": [type("Count", (), {"active_count": 1})()],
    })
    loader.ctx = type("Context", (), {
        "settings": Settings(env="tst"), "batch_id": "batch-1", "delivery_id": "SALES|2026-09-01",
    })()
    entities = [
        GoldEntity("GC_CUSTOMER", "CURRENT", "DIMENSION", "gold.current.customer", "gold", "current",
                   "customer", "SELECT 1", ["id"], "SNAPSHOT", "ATOMIC_SWAP", "SALES_MART", [], 1),
        GoldEntity("GC_SALES", "CURRENT", "FACT", "gold.current.sales", "gold", "current", "sales",
                   "SELECT 1", ["id"], "SNAPSHOT", "ATOMIC_SWAP", "SALES_MART", [], 2),
    ]

    loader.publish_group(
        {"GC_CUSTOMER": "pub-customer", "GC_SALES": "pub-sales"}, entities, "lease-1"
    )

    statements = loader.spark.statements
    assert "SET publication_status = 'ACTIVE'" in statements[2]
    assert "MERGE INTO contoso_meta_tst.audit.audit_gold_publication_group" in statements[3]
    assert "expires_at > current_timestamp()" in statements[3]
    assert "SET publication_status = 'SUPERSEDED'" in statements[4]


def test_audit_run_records_failure_and_preserves_the_original_exception():
    spark = _RecordingSpark({
        "audit_metadata_version": [type("Version", (), {"metadata_version": "metadata-v1"})()],
        "SELECT lease_id": [type("Lease", (), {"lease_id": "lease-1"})()],
    })
    context = type("Context", (), {
        "settings": Settings(env="tst"), "batch_id": "batch-1", "delivery_id": "SALES|2026-09-01",
        "load_date_literal": "timestamp'2026-09-01 00:00:00.000'", "job_run_id": "job-1"
    })()

    with pytest.raises(ValueError, match="expected failure"):
        with AuditLogger(spark, context).run("QUALITY", "SALES.ORDERS"):
            raise ValueError("expected failure")

    statements = "\n".join(spark.statements)
    assert "'RUNNING'" in statements
    assert "'FAILED'" in statements
    assert "expected failure" in statements
    assert "audit_work_item" in statements


def test_audit_logger_quarantines_a_delivery_with_its_reason():
    spark = _RecordingSpark()
    context = type("Context", (), {
        "settings": Settings(env="tst"), "batch_id": "batch-1", "delivery_id": "SALES|2026-09-01",
        "load_date_literal": "timestamp'2026-09-01 00:00:00.000'", "job_run_id": "job-1"
    })()

    spark.rows_by_fragment["SELECT delivery_status"] = [type("Delivery", (), {"delivery_status": "IN_PROGRESS"})()]
    AuditLogger(spark, context).quarantine_delivery("SALES|2026-09-01", "DQ threshold exceeded")

    statement = spark.statements[-2]
    assert "delivery_status = 'QUARANTINED'" in statement
    assert "quarantine_reason = 'DQ threshold exceeded'" in statement
    assert "delivery_id = 'SALES|2026-09-01'" in statement


def test_delivery_state_machine_rejects_invalid_transition_and_audits_valid_transition():
    spark = _RecordingSpark({"SELECT delivery_status": [
        type("Delivery", (), {"delivery_status": "IN_PROGRESS"})()
    ]})
    context = type("Context", (), {"settings": Settings(env="tst")})()
    audit = AuditLogger(spark, context)

    audit.transition_delivery("SALES|2026-09-01", "COMPLETE", "operator", "Validated")
    assert "audit_delivery_state_transition" in spark.statements[-1]
    assert "'IN_PROGRESS'" in spark.statements[-1]
    assert "'COMPLETE'" in spark.statements[-1]

    with pytest.raises(ValueError, match="Niet-toegestane"):
        audit.transition_delivery("SALES|2026-09-01", "DETECTED", "operator")

    with pytest.raises(ValueError, match="vereist reden en approval_reference"):
        audit.transition_delivery("SALES|2026-09-01", "SUPERSEDED", "operator")


def test_work_item_control_plane_plans_claims_and_finishes_with_a_lease():
    spark = _RecordingSpark({"SELECT lease_id": [type("Lease", (), {"lease_id": "any"})()]})
    audit = AuditLogger(spark, type("Context", (), {"settings": Settings(env="tst")})())

    audit.plan_work_item("SALES|2026-09-01", "QUALITY", "SALES.ORDERS")
    lease_id = audit.claim_work_item("SALES|2026-09-01", "QUALITY", "SALES.ORDERS")
    audit.finish_work_item(lease_id, succeeded=False, error="temporary failure")

    statements = "\n".join(spark.statements)
    assert "audit_work_item" in statements
    assert "attempt_count = t.attempt_count + 1" in statements
    assert "INTERVAL 4 HOURS" in statements
    assert "DEAD_LETTER" in statements
    with pytest.raises(ValueError, match="minimaal 1"):
        audit.plan_work_item("SALES|2026-09-01", "QUALITY", "SALES.ORDERS", max_attempts=0)


def test_dead_letter_requeue_requires_approval_and_writes_transition_audit():
    spark = _RecordingSpark({"SELECT work_item_id": [
        type("WorkItem", (), {"work_item_id": "work-1", "work_status": "DEAD_LETTER"})()
    ]})
    audit = AuditLogger(spark, type("Context", (), {"settings": Settings(env="tst")})())

    audit.requeue_dead_letter(
        "SALES|2026-09-01", "QUALITY", "SALES.ORDERS", "operator", "Source corrected", "CHG-123"
    )

    statements = "\n".join(spark.statements)
    assert "work_status = 'PENDING'" in statements
    assert "audit_work_item_transition" in statements
    assert "'CHG-123'" in statements
    with pytest.raises(ValueError, match="verplicht"):
        audit.requeue_dead_letter("SALES|2026-09-01", "QUALITY", "SALES.ORDERS", "", "", "")


def test_audit_logger_closes_a_delivery_manifest_after_atomic_publication():
    spark = _RecordingSpark()
    context = type("Context", (), {
        "settings": Settings(env="tst"), "batch_id": "batch-1", "delivery_id": "SALES|2026-09-01",
        "load_date_literal": "timestamp'2026-09-01 00:00:00.000'", "job_run_id": "job-1",
    })()

    AuditLogger(spark, context).close_delivery_manifest(
        "SALES|2026-09-01", "SALES", "/Volumes/raw_tst/sales/landing/2026-09-01/_manifest.json",
        expected_objects=3, file_count=4, snapshot_complete=True, source_watermark="run-42",
    )

    statement = spark.statements[-1]
    assert "audit_delivery_manifest" in statement
    assert "'CLOSED'" in statement
    assert "is_snapshot_complete" in statement
    assert "'run-42'" in statement


def test_quality_retries_replace_outputs_only_for_the_same_delivery_and_source_object():
    spark = _RecordingSpark()
    loader = object.__new__(QualityEngine)
    loader.spark = spark
    source = type("Source", (), {
        "source_object_id": "SALES.ORDERS", "quality_table_fqn": "quality.orders",
        "reject_table_fqn": "reject.orders",
    })()

    loader._clear_delivery_outputs(source, "SALES|2026-09-01")

    statements = "\n".join(spark.statements)
    assert "DELETE FROM quality.orders" in statements
    assert "DELETE FROM reject.orders" in statements
    assert "_delivery_id = 'SALES|2026-09-01'" in statements
    assert "_record_source = 'SALES.ORDERS'" in statements
    assert "source_object_id = 'SALES.ORDERS'" in statements


def test_metadata_validator_reports_invalid_metadata_without_stopping_at_first_issue():
    source = type("Source", (), {
        "source_object_id": "SALES.ORDERS", "bronze_table_fqn": "bronze.orders", "quality_table_fqn": "quality.orders"
    })()
    entity = GoldEntity(
        gold_entity_id="GC_BAD", gold_layer="CURRENT", entity_type="DIMENSION",
        target_table_fqn="gold.dim_bad", target_catalog="gold", target_schema="current", target_table="dim_bad",
        select_sql="SELECT broken_column", business_key_columns=["id"], scd_type="SNAPSHOT",
        publish_mode="ATOMIC_SWAP", publication_group_id=None, depends_on_gold_entity_ids=["GC_MISSING"], load_order=1,
        pointer_table=None, staging_table="gold.current_internal.dim_bad",
    )
    repo = type("Repository", (), {
        "source_objects": lambda *_args: (source,), "mappings": lambda *_args: [],
        "quality_rules": lambda *_args: [], "dv_entities": lambda *_args: (),
        "gold_entities": lambda *_args: (entity,),
        "dependencies": lambda *_args: (),
    })()
    spark = _RecordingSpark()
    validator = MetadataValidator(spark, repo, Settings(env="tst"))

    issues = validator.validate_all()

    assert {(issue.category, issue.entity) for issue in issues} >= {
        ("MAPPING", "SALES.ORDERS"), ("GOLD", "GC_BAD"),
    }
    messages = " ".join(issue.message for issue in issues)
    assert "Onbekende afhankelijkheid" in messages
    assert "publication_group_id" in messages
    assert "pointer_table" in messages
    assert "staging_table" in messages


def test_metadata_validator_rejects_invalid_snapshot_and_delete_contracts():
    source = type("Source", (), {
        "source_object_id": "SALES.ORDERS", "load_strategy": "PARTIAL_SNAPSHOT",
        "delete_semantics": "SNAPSHOT_ABSENCE", "absence_means_delete": True,
        "deleted_flag_column": None,
    })()
    repo = type("Repository", (), {"source_objects": lambda *_args: (source,)})()
    validator = MetadataValidator(_RecordingSpark(), repo, Settings(env="tst"))

    issues = validator.validate_source_objects()

    assert "absence_means_delete vereist SNAPSHOT_SCD2" in issues[0].message


def test_approved_schema_drift_columns_are_accepted():
    loader = object.__new__(BronzeLoader)
    loader.repo = type("Repository", (), {
        "schema_drift_is_approved": lambda *_args: True,
    })()
    source = type("Source", (), {
        "source_object_id": "SALES.ORDERS", "schema_drift_policy": "ALLOW_NEW_COLUMNS_WITH_APPROVAL",
    })()

    loader._validate_schema_drift(source, ["new_attribute"])


def test_historical_gold_merge_inserts_only_new_scd2_versions():
    class NoOpContextManager:
        def __enter__(self):
            return {}

        def __exit__(self, *_args):
            return False

    class EmptyResult:
        def collect(self):
            return []

    class RecordingSpark:
        def __init__(self):
            self.statements = []

        def sql(self, statement):
            self.statements.append(statement)
            return EmptyResult()

    entity = GoldEntity(
        gold_entity_id="GH_TEST", gold_layer="HISTORICAL", entity_type="DIMENSION",
        target_table_fqn="gold.historical.dim_test", target_catalog="gold", target_schema="historical",
        target_table="dim_test", select_sql="SELECT id, valid_from FROM vault.sat_test",
        business_key_columns=["id", "valid_from"], scd_type="SCD2", publish_mode="MERGE",
        publication_group_id=None, depends_on_gold_entity_ids=[], load_order=1,
        pointer_table=None, staging_table=None,
    )
    loader = object.__new__(GoldLoader)
    loader.spark = RecordingSpark()
    loader.ctx = type("Context", (), {
        "batch_id": "batch-1", "load_date_literal": "timestamp'2026-09-01 00:00:00'"
    })()
    loader.audit = type("Audit", (), {
        "run": lambda *_args: NoOpContextManager()
    })()

    loader.load_historical(entity)

    statement = loader.spark.statements[0]
    assert "WHEN NOT MATCHED THEN INSERT *" in statement
    assert "WHEN MATCHED THEN UPDATE" not in statement


def test_bronze_empty_landing_is_detected_before_autoloader_schema_inference():
    class EmptyFiles:
        def option(self, *_args):
            return self

        def load(self, _path):
            return self

        def limit(self, _count):
            return self

        def count(self):
            return 0

    class EmptyReader:
        def format(self, _format):
            return EmptyFiles()

    loader = object.__new__(BronzeLoader)
    loader.spark = type("Spark", (), {"read": EmptyReader()})()
    source = type("Source", (), {"file_pattern": "orders*.parquet"})()

    assert not loader.has_input_files(source, "/Volumes/raw_tst/sales/landing")


@pytest.mark.parametrize(
    ("strategy", "expected_match"),
    [
        ("INCREMENTAL_APPEND", "t._source_file_path = s._source_file_path"),
        ("SNAPSHOT_SCD2", "t._delivery_id = s._delivery_id"),
        ("INCREMENTAL_MERGE", "ON t.order_key <=> s.order_key"),
    ],
)
def test_bronze_loader_uses_the_configured_merge_strategy(strategy, expected_match):
    class Slice:
        def dropDuplicates(self, columns):
            self.dedupe_columns = columns
            return self

        def createOrReplaceTempView(self, name):
            self.view_name = name

    class RecordingSpark:
        def __init__(self):
            self.statements = []

        def sql(self, statement):
            self.statements.append(statement)

    loader = object.__new__(BronzeLoader)
    loader.spark = RecordingSpark()
    source = type("Source", (), {
        "source_object_id": "SALES.ORDERS", "bronze_table_fqn": "bronze.orders",
        "business_key_columns": ["order_key"], "load_strategy": strategy,
    })()

    slice_df = Slice()
    loader._merge_bronze_slice(source, slice_df)

    statement = loader.spark.statements[0]
    assert "MERGE WITH SCHEMA EVOLUTION" in statement
    assert expected_match in statement
    assert slice_df.dedupe_columns == ["_source_file_path", "_delivery_id", "order_key"]


def test_bronze_loader_overwrites_only_for_full_overwrite_and_rejects_unknown_strategies():
    class Slice:
        def dropDuplicates(self, columns):
            self.dedupe_columns = columns
            return self

        def createOrReplaceTempView(self, _name):
            pass

    class RecordingSpark:
        def __init__(self):
            self.statements = []

        def sql(self, statement):
            self.statements.append(statement)

    loader = object.__new__(BronzeLoader)
    loader.spark = RecordingSpark()
    overwrite_source = type("Source", (), {
        "source_object_id": "SALES.REFERENCE", "bronze_table_fqn": "bronze.reference",
        "business_key_columns": [], "load_strategy": "FULL_OVERWRITE",
    })()
    loader._merge_bronze_slice(overwrite_source, Slice())

    assert loader.spark.statements == [
        "CREATE OR REPLACE TABLE bronze.reference AS SELECT * FROM _bronze_sales_reference"
    ]

    invalid_source = type("Source", (), {
        "source_object_id": "SALES.CDC", "bronze_table_fqn": "bronze.cdc",
        "business_key_columns": ["id"], "load_strategy": "INCREMENTAL_CDC",
    })()
    # CDC is nu een ondersteunde strategie: de loader mag géén ValueError geven,
    # maar moet wel een MERGE opbouwen met de _cdc_op-kolom in de match.
    loader._merge_bronze_slice(invalid_source, Slice())
    cdc_statement = loader.spark.statements[-1]
    assert "MERGE WITH SCHEMA EVOLUTION" in cdc_statement
    assert "_cdc_op" in cdc_statement

    # Een echt onbekende strategie moet nog steeds falen.
    unknown_source = type("Source", (), {
        "source_object_id": "SALES.UNKNOWN", "bronze_table_fqn": "bronze.unknown",
        "business_key_columns": ["id"], "load_strategy": "TELEPORT",
    })()
    with pytest.raises(ValueError, match="Niet-ondersteunde load_strategy"):
        loader._merge_bronze_slice(unknown_source, Slice())


def test_parallel_bronze_tasks_do_not_refresh_the_shared_delivery_row():
    bronze_loader = (
        Path(__file__).resolve().parents[1] / "src" / "contoso_lakehouse" / "bronze.py"
    ).read_text(encoding="utf-8")

    assert "self.audit.refresh_delivery_status" not in bronze_loader
    assert "registration_owner = min(item.source_object_id for item in mandatory_objects)" in bronze_loader
    assert "if obj.source_object_id == registration_owner:" in bronze_loader


@pytest.mark.parametrize(
    ("policy", "columns", "expected_error"),
    [
        ("RESCUE", ["new_attribute"], None),
        ("STRICT", ["new_attribute"], "STRICT"),
        ("ALLOW_NEW_COLUMNS_WITH_APPROVAL", ["new_attribute"], "schema-driftgoedkeuring"),
        ("UNDEFINED", ["new_attribute"], "onbekend schema_drift_policy"),
        ("STRICT", [], None),
    ],
)
def test_bronze_schema_drift_policy_is_enforced_before_merge(policy, columns, expected_error):
    loader = object.__new__(BronzeLoader)
    loader.repo = type("Repository", (), {
        "schema_drift_is_approved": lambda *_args: False,
    })()
    source = type("Source", (), {"source_object_id": "SALES.ORDERS", "schema_drift_policy": policy})()

    if expected_error:
        with pytest.raises(SchemaDriftError, match=expected_error):
            loader._validate_schema_drift(source, columns)
    else:
        loader._validate_schema_drift(source, columns)


def test_metadata_ddl_includes_seeded_enterprise_fields():
    ddl = METADATA_DDL.read_text(encoding="utf-8")
    setup = SETUP_NOTEBOOK.read_text(encoding="utf-8")
    for field in (
        "delete_semantics", "absence_means_delete", "schema_contract_version",
        "late_arrival_window_days", "freshness_sla_hours", "backfill_strategy",
        "schema_drift_approval_required", "schema_drift_policy", "owner_team", "criticality",
        "priority", "retry_policy", "max_retries",
        "is_blocking", "rule_group",
        "publish_status", "pointer_table", "staging_table",
    ):
        assert field in ddl, field
        assert field in setup, field
    assert "ALTER TABLE {fqn} ADD COLUMNS" in setup


def test_delta_tables_with_defaults_enable_the_column_defaults_feature():
    metadata_ddl = METADATA_DDL.read_text(encoding="utf-8")
    audit_ddl = (
        Path(__file__).resolve().parents[1] / "sql" / "01_metadata" / "11_audit_model.sql"
    ).read_text(encoding="utf-8")

    for table in (
        "meta_table_maintenance_policy", "meta_data_governance_policy", "meta_gold_data_product",
    ):
        definition = metadata_ddl.split(f"CREATE TABLE IF NOT EXISTS {table}", 1)[1].split(
            "CREATE TABLE IF NOT EXISTS", 1
        )[0]
        assert "delta.feature.allowColumnDefaults" in definition


def test_metadata_and_audit_ddl_keep_schema_drift_and_maintenance_statements_separate():
    metadata_ddl = METADATA_DDL.read_text(encoding="utf-8")
    audit_ddl = (
        Path(__file__).resolve().parents[1] / "sql" / "01_metadata" / "11_audit_model.sql"
    ).read_text(encoding="utf-8")
    schema_drift_definition = metadata_ddl.split(
        "CREATE TABLE IF NOT EXISTS meta_schema_drift_approval", 1
    )[1].split("CREATE TABLE IF NOT EXISTS meta_dependency", 1)[0]

    assert "REFERENCES meta_source_object(source_object_id) RELY\n)\nUSING DELTA" in schema_drift_definition
    assert "CREATE TABLE IF NOT EXISTS audit_maintenance_run" not in metadata_ddl
    assert "CREATE TABLE IF NOT EXISTS audit_maintenance_run" in audit_ddl
    for table in ("audit_delivery_manifest", "audit_work_item", "audit_reconciliation_result"):
        definition = audit_ddl.split(f"CREATE TABLE IF NOT EXISTS {table}", 1)[1].split(
            "CREATE TABLE IF NOT EXISTS", 1
        )[0]
        assert "delta.feature.allowColumnDefaults" in definition


def test_maintenance_policies_are_seeded_and_have_safe_retention():
    policies = _seed("meta_table_maintenance_policy")
    assert {policy["catalog_name"] for policy in policies} >= {
        "contoso_bronze_${env}", "contoso_quality_${env}", "contoso_vault_${env}",
        "contoso_gold_${env}", "contoso_meta_${env}", "contoso_reject_${env}",
    }
    assert all(policy["vacuum_retain_hours"] >= 168 for policy in policies)
    assert any(policy["maintenance_tier"] == "AUDIT" and policy["optimize_mode"] == "DISABLED"
               for policy in policies)


def test_maintenance_is_policy_driven_auditable_and_supports_dry_run():
    notebook = (Path(__file__).resolve().parents[1] / "notebooks" / "90_maintenance.py").read_text(encoding="utf-8")
    workflow = (Path(__file__).resolve().parents[1] / "workflows" / "maintenance.job.yml").read_text(encoding="utf-8")

    for required in (
        "meta_table_maintenance_policy", "audit_maintenance_run", "audit_maintenance_action",
        "dry_run", "DESCRIBE DETAIL", "min_files_before_optimize", "Policy due", "Policy disabled",
    ):
        assert required in notebook
    assert "vacuum_retain_hours" not in workflow
    assert 'dry_run: "false"' in workflow


def test_landing_location_is_environment_specific_and_passed_to_setup():
    bundle = BUNDLE_CONFIG.read_text(encoding="utf-8")
    ddl = CATALOGS_DDL.read_text(encoding="utf-8")
    setup_notebook = SETUP_NOTEBOOK.read_text(encoding="utf-8")
    setup_workflow = SETUP_WORKFLOW.read_text(encoding="utf-8")

    assert "landing_path:" in bundle
    assert "landing_path: sales/tst" in bundle
    assert "landing_path: sales/prd" in bundle
    assert "${landing_path}" in ddl
    assert '"${landing_path}": landing_path' in setup_notebook
    assert "landing_path: ${var.landing_path}" in setup_workflow


def test_pipeline_parallelism_is_configurable_and_keeps_source_delivery_serial():
    bundle = BUNDLE_CONFIG.read_text(encoding="utf-8")
    workflow = PIPELINE_WORKFLOW.read_text(encoding="utf-8")

    assert "pipeline_parallelism:" in bundle
    assert "default: 3" in bundle
    assert "max_concurrent_runs: ${var.pipeline_parallelism}" in workflow
    assert "chronologische, seriele verwerking binnen elk bronsysteem" in workflow


def test_fabric_sales_schemas_and_landing_volumes_are_provisioned():
    ddl = CATALOGS_DDL.read_text(encoding="utf-8")
    reject_ddl = (
        Path(__file__).resolve().parents[1] / "sql" / "03_quality_reject" / "31_reject_tables.sql"
    ).read_text(encoding="utf-8")

    for location in (
        "raw_${env}.fabric_sales.landing",
        "raw_${env}.fabric_sales.checkpoints",
        "raw_${env}.fabric_sales.quarantine",
        "contoso_bronze_${env}.fabric_sales",
        "contoso_quality_${env}.fabric_sales",
        "contoso_reject_${env}.fabric_sales",
    ):
        assert location in ddl
    assert "CREATE TABLE IF NOT EXISTS rj_order_lines" in reject_ddl
    assert "contoso_reject_${env}.fabric_sales.rj_order_lines" in reject_ddl


def test_gold_consumption_requires_explicit_table_and_view_grants():
    base_grants = (
        Path(__file__).resolve().parents[1] / "sql" / "00_unity_catalog" / "01_grants.sql"
    ).read_text(encoding="utf-8")
    consumer_grants = GOLD_CONSUMER_GRANTS.read_text(encoding="utf-8")
    setup = SETUP_NOTEBOOK.read_text(encoding="utf-8")

    assert "SELECT ON SCHEMA contoso_gold_${env}" not in base_grants
    assert "GRANT USE SCHEMA ON SCHEMA contoso_gold_${env}.current" in base_grants
    for object_type, object_name in (
        ("TABLE", "historical.dim_customer_hist"),
        ("TABLE", "historical.dim_product_hist"),
        ("TABLE", "historical.fct_sales_hist"),
        ("VIEW", "current.dim_customer"),
        ("VIEW", "current.dim_product"),
        ("VIEW", "current.fct_sales"),
        ("VIEW", "current.v_gold_freshness"),
    ):
        assert f"GRANT SELECT ON {object_type} contoso_gold_${{env}}.{object_name}" in consumer_grants
    assert "02_gold_consumer_grants.sql" in setup


def test_delivery_supersede_is_auditable_and_requires_approval():
    audit_ddl = (
        Path(__file__).resolve().parents[1] / "sql" / "01_metadata" / "11_audit_model.sql"
    ).read_text(encoding="utf-8")
    remediation = (
        Path(__file__).resolve().parents[1] / "notebooks" / "07_supersede_delivery.py"
    ).read_text(encoding="utf-8")
    audit = (Path(__file__).resolve().parents[1] / "src" / "contoso_lakehouse" / "audit.py").read_text(encoding="utf-8")

    for field in ("superseded_at", "superseded_by", "supersede_reason", "supersede_approval_reference"):
        assert field in audit_ddl
        assert field in audit
    assert "audit_gold_publication_group" in remediation
    assert "g.release_status = 'ACTIVE'" in remediation
    assert "if row.gold_published:" in remediation
    assert 'audit.transition_delivery(' in remediation
    assert 'sys.path.insert(0, f"{dbutils.widgets.get(\'repo_root\')}/src")' in remediation


def test_quarantine_release_is_auditable_and_limited_to_quarantined_deliveries():
    audit_ddl = (
        Path(__file__).resolve().parents[1] / "sql" / "01_metadata" / "11_audit_model.sql"
    ).read_text(encoding="utf-8")
    release = (
        Path(__file__).resolve().parents[1] / "notebooks" / "08_release_quarantined_delivery.py"
    ).read_text(encoding="utf-8")
    workflow = (
        Path(__file__).resolve().parents[1] / "workflows" / "quarantine_remediation.job.yml"
    ).read_text(encoding="utf-8")
    audit = (Path(__file__).resolve().parents[1] / "src" / "contoso_lakehouse" / "audit.py").read_text(encoding="utf-8")

    for field in ("quarantined_at", "quarantine_reason"):
        assert field in audit_ddl
    for field in ("released_at", "released_by", "release_reason", "release_approval_reference"):
        assert field in audit_ddl
        assert field in audit
    assert 'row.delivery_status != "QUARANTINED"' in release
    assert 'audit.transition_delivery(' in release
    assert "release_quarantined_delivery" in workflow


def test_serverless_pipeline_uses_shared_bronze_compute():
    workflow = PIPELINE_WORKFLOW.read_text(encoding="utf-8")
    planner = (
        Path(__file__).resolve().parents[1] / "notebooks" / "06_plan_bronze_fanout.py"
    ).read_text(encoding="utf-8")
    bronze = (
        Path(__file__).resolve().parents[1] / "notebooks" / "10_bronze_autoloader.py"
    ).read_text(encoding="utf-8")

    assert "task_key: plan_bronze_fanout" in workflow
    assert "task_key: register_delivery_manifests" in workflow
    assert "notebook_path: ../notebooks/04_register_delivery_manifests.py" in workflow
    assert "depends_on: [{ task_key: register_delivery_manifests }]" in workflow
    assert "for_each_task:" not in workflow
    assert 'bronze_object_ids: "{{tasks.plan_bronze_fanout.values.bronze_object_ids}}"' in workflow
    assert "max_retries: 3" in workflow
    assert "name: source_system_id" in workflow
    assert 'source_system_id: "{{job.parameters.source_system_id}}"' in workflow
    assert "currentRunId" not in planner
    assert 'dbutils.widgets.text("bronze_object_ids", "[]")' in bronze
    assert "json.loads" in bronze
    assert "for object_id in object_ids" in bronze


def test_metadata_preflight_is_fingerprint_bound_and_full_by_default():
    workflow = PIPELINE_WORKFLOW.read_text(encoding="utf-8")
    validator = (
        Path(__file__).resolve().parents[1] / "notebooks" / "99_validate_metadata.py"
    ).read_text(encoding="utf-8")
    audit_ddl = (
        Path(__file__).resolve().parents[1] / "sql" / "01_metadata" / "11_audit_model.sql"
    ).read_text(encoding="utf-8")

    assert "name: metadata_validation_mode" in workflow
    assert "default: FULL" in workflow
    assert 'dbutils.widgets.dropdown("validation_mode", "FULL"' in validator
    assert "audit_metadata_validation" in validator
    assert "metadata_version" in validator
    assert "audit_metadata_validation" in audit_ddl


def test_pipeline_reconciles_quality_before_vault_processing():
    workflow = PIPELINE_WORKFLOW.read_text(encoding="utf-8")
    notebook = (
        Path(__file__).resolve().parents[1] / "notebooks" / "21_reconcile_quality.py"
    ).read_text(encoding="utf-8")

    assert "task_key: reconcile_quality" in workflow
    assert "notebook_path: ../notebooks/21_reconcile_quality.py" in workflow
    assert "depends_on: [{ task_key: reconcile_quality }]" in workflow
    assert "ReconciliationEngine" in notebook
    assert "expected_count" in notebook


def test_standalone_load_jobs_pull_before_pipeline_processing():
    public_api_extracts = (
        (CBS_LOAD_WORKFLOW, "extract_cbs", "source_object_id: CBS.JEUGDZORG_WIJK_2025"),
        (ECB_LOAD_WORKFLOW, "extract_ecb", "source_object_id: ECB.EXCHANGE_RATE"),
        (NAGER_LOAD_WORKFLOW, "extract_nager", "source_object_id: NAGER.HOLIDAYS_NL"),
    )
    for workflow_path, extract_task, source_object_parameter in public_api_extracts:
        workflow = workflow_path.read_text(encoding="utf-8")

        assert "schedule:" in workflow
        assert "pause_status: ${var.pull_schedule_pause_status}" in workflow
        assert f"task_key: {extract_task}" in workflow
        assert "job_id: ${resources.jobs.extract_public_api.id}" in workflow
        assert source_object_parameter in workflow
        assert f"depends_on: [{{ task_key: {extract_task} }}]" in workflow

    fabric_workflow = FABRIC_LOAD_WORKFLOW.read_text(encoding="utf-8")

    assert "schedule:" in fabric_workflow
    assert "pause_status: ${var.pull_schedule_pause_status}" in fabric_workflow
    assert "task_key: extract_fabric_sql" in fabric_workflow
    assert "notebook_path: ../notebooks/11_extract_fabric_sql.py" in fabric_workflow
    assert "source_object_id: FABRIC_SALES.ORDER_LINES" in fabric_workflow
    assert "depends_on: [{ task_key: extract_fabric_sql }]" in fabric_workflow


def test_active_sources_acceptance_job_loads_all_sources_and_monitors_gold():
    workflow = ACTIVE_SOURCES_ACCEPTANCE_WORKFLOW.read_text(encoding="utf-8")
    monitor = (
        Path(__file__).resolve().parents[1] / "notebooks" / "42_monitor_active_source_gold.py"
    ).read_text(encoding="utf-8")

    for task_key in (
        "generate_sales_delivery",
        "load_sales_to_gold",
        "load_cbs_to_gold",
        "load_ecb_to_gold",
        "load_nager_to_gold",
        "load_fabric_sales_to_gold",
        "monitor_active_sources_gold",
    ):
        assert f"task_key: {task_key}" in workflow

    assert "source_system_id: SALES" in workflow
    for job_reference in (
        "job_id: ${resources.jobs.load_cbs_jeugdzorg_wijk.id}",
        "job_id: ${resources.jobs.load_ecb_exchange_rates.id}",
        "job_id: ${resources.jobs.load_nager_holidays_nl.id}",
        "job_id: ${resources.jobs.load_fabric_sales_order_lines.id}",
    ):
        assert job_reference in workflow

    assert "notebook_path: ../notebooks/42_monitor_active_source_gold.py" in workflow
    assert "expected_delivery_date: \"{{job.parameters.delivery_date}}\"" in workflow
    assert "MISSING_PUBLICATION" in monitor
    assert "EMPTY_PUBLICATION" in monitor
    assert "raise RuntimeError" in monitor


def test_operations_monitor_covers_control_plane_slo_breaches():
    monitor = (
        Path(__file__).resolve().parents[1] / "notebooks" / "43_monitor_operations.py"
    ).read_text(encoding="utf-8")
    workflow = (
        Path(__file__).resolve().parents[1] / "workflows" / "operations_monitoring.job.yml"
    ).read_text(encoding="utf-8")

    for breach_type in (
        "MANIFEST_SLA", "DELIVERY_SLA", "DEAD_LETTER", "EXPIRED_WORK_LEASE", "EXPIRED_GOLD_LEASE",
    ):
        assert breach_type in monitor
    assert "raise RuntimeError" in monitor
    assert "task_key: monitor_operations" in workflow
    assert "notebook_path: ../notebooks/43_monitor_operations.py" in workflow


def test_dead_letter_remediation_workflow_requires_approval_context():
    notebook = (
        Path(__file__).resolve().parents[1] / "notebooks" / "12_requeue_dead_letter.py"
    ).read_text(encoding="utf-8")
    workflow = (
        Path(__file__).resolve().parents[1] / "workflows" / "work_item_remediation.job.yml"
    ).read_text(encoding="utf-8")

    for value in ("delivery_id", "layer", "entity_id", "reason", "approved_by", "approval_reference"):
        assert value in notebook
        assert f"name: {value}" in workflow
    assert "audit.requeue_dead_letter(" in notebook
    assert "task_key: requeue_dead_letter" in workflow


def test_demo_generator_creates_files_matching_source_object_patterns():
    generator = (
        Path(__file__).resolve().parents[1] / "notebooks" / "01_generate_demo_delivery.py"
    ).read_text(encoding="utf-8")

    assert "dbutils.fs.mv(part_file" in generator
    for object_name in ("customers", "products", "employees", "orders", "returns"):
        assert f'write_delivery_file({object_name}' in generator
    assert "audit.close_delivery_manifest(" in generator
    assert 'f"{landing_path}/_manifest.json"' in generator
    assert "sys.path.insert(0" in generator
    assert '"is_snapshot_complete\\": true' in generator


def test_manifest_registration_validates_closed_push_manifests_before_bronze():
    manifest_notebook = (
        Path(__file__).resolve().parents[1] / "notebooks" / "04_register_delivery_manifests.py"
    ).read_text(encoding="utf-8")

    for required in (
        "_manifest.json", "manifest.get(\"status\") != \"CLOSED\"",
        "is_snapshot_complete", "audit.close_delivery_manifest(", "file_count",
    ):
        assert required in manifest_notebook


def test_pull_extractors_close_a_central_delivery_manifest_after_publish():
    public_api = (Path(__file__).resolve().parents[1] / "notebooks" / "09_extract_public_api.py").read_text(encoding="utf-8")
    fabric = (Path(__file__).resolve().parents[1] / "notebooks" / "11_extract_fabric_sql.py").read_text(encoding="utf-8")

    for extractor in (public_api, fabric):
        assert "dbutils.fs.mv(staging, target, True)" in extractor
        assert "audit.close_delivery_manifest(" in extractor
        assert "snapshot_complete=row.load_strategy == \"SNAPSHOT_SCD2\"" in extractor

    stress = (Path(__file__).resolve().parents[1] / "notebooks" / "02_generate_stress_delivery.py").read_text(encoding="utf-8")
    assert "repo_root" in stress
    assert "sys.path.insert(0" in stress


def test_demo_generator_keeps_business_dates_valid_for_future_delivery_folders():
    generator = (
        Path(__file__).resolve().parents[1] / "notebooks" / "01_generate_demo_delivery.py"
    ).read_text(encoding="utf-8")

    assert "business_date = min(delivery_day, date.today()).isoformat()" in generator
    assert '"O-3001", 1, "C-1001", "P-2001", "E-5001", business_date, business_date, business_date' in generator
    assert '"O-3002", 1, "C-1002", "P-2002", "E-5001", business_date, None, None' in generator
    assert '"R-4001", "O-3001", 1, "P-2001", "E-5002", business_date' in generator


def test_stress_generator_uses_spark_connect_safe_date_arithmetic():
    generator = (
        Path(__file__).resolve().parents[1] / "notebooks" / "02_generate_stress_delivery.py"
    ).read_text(encoding="utf-8")

    assert 'F.lit(business_date).alias("order_date")' in generator
    assert 'F.lit(business_date).alias("customer_since")' in generator
    assert 'F.lit(business_date).alias("hire_date")' in generator


def test_stress_generator_supports_deterministic_phase_two_changes():
    generator = (
        Path(__file__).resolve().parents[1] / "notebooks" / "02_generate_stress_delivery.py"
    ).read_text(encoding="utf-8")
    workflow = (
        Path(__file__).resolve().parents[1] / "workflows" / "stress_delivery.job.yml"
    ).read_text(encoding="utf-8")

    assert 'dbutils.widgets.text("change_set", "0")' in generator
    assert "change_set = widget_change_set()" in generator
    assert '((F.col("id") % 50) == 0)' in generator
    assert '((F.col("id") % 100) == 0)' in generator
    assert '((F.col("id") % 200) == 0)' in generator
    assert ')).alias("is_deleted")' in generator
    assert 'F.when((F.lit(change_set) > 0) & ((F.col("id") % 50) == 0), F.format_string("Customer %06d v%d", F.col("id") + 1, F.lit(change_set)))' in generator
    assert 'F.when((F.lit(change_set) > 0) & ((F.col("id") % 100) == 0), F.lit(change_set)).otherwise(0)' in generator
    assert 'F.when((F.lit(change_set) > 0) & ((F.col("id") % 200) == 0), F.format_string("Employee%05d v%d", F.col("id") + 1, F.lit(change_set)))' in generator
    assert "change_set: \"{{job.parameters.change_set}}\"" in workflow


def test_setup_migrates_gold_fact_columns_used_by_current_gold():
    setup = (
        Path(__file__).resolve().parents[1] / "notebooks" / "00_setup_lakehouse.py"
    ).read_text(encoding="utf-8")

    assert '"historical.fct_sales_hist"' in setup
    for column in ("employee_hk", "order_date_key", "ship_date_key", "delivery_date_key"):
        assert f'"{column}":' in setup


def test_setup_reports_the_failing_ddl_statement():
    setup = (
        Path(__file__).resolve().parents[1] / "notebooks" / "00_setup_lakehouse.py"
    ).read_text(encoding="utf-8")

    assert "enumerate(split_sql_statements(text), start=1)" in setup
    assert "DDL faalde in {relative_path}, statement {statement_number}" in setup
    assert "raise RuntimeError(" in setup
    assert ") from exc" in setup


def test_setup_migrates_gold_source_before_creating_audit_views():
    setup = (
        Path(__file__).resolve().parents[1] / "notebooks" / "00_setup_lakehouse.py"
    ).read_text(encoding="utf-8")

    assert "def apply_pre_audit_migrations()" in setup
    assert '"metadata.meta_source_object"' in setup
    assert '"schema_contract_version": "STRING"' in setup
    assert '"metadata.meta_gold_entity"' in setup
    assert 'if script == "sql/01_metadata/10_metadata_model.sql":' in setup


def test_public_api_extractor_preserves_an_explicit_empty_records_key():
    extractor = (
        Path(__file__).resolve().parents[1] / "notebooks" / "09_extract_public_api.py"
    ).read_text(encoding="utf-8")

    assert 'records_key = row.records_key if row.records_key is not None else "value"' in extractor
    assert "records_key=records_key" in extractor
    assert "sys.landing_volume_path" in extractor
    assert "metadata.meta_source_system sys" in extractor


def test_fabric_extractor_uses_the_service_principal_login_format():
    extractor = (
        Path(__file__).resolve().parents[1] / "notebooks" / "11_extract_fabric_sql.py"
    ).read_text(encoding="utf-8")
    connector = _seed("meta_source_connector")

    assert 'f"{dbutils.secrets.get(secret_scope, \'client-id\')}@"' in extractor
    assert 'f"{dbutils.secrets.get(secret_scope, \'tenant-id\')}"' in extractor
    fabric_connector = next(
        row for row in connector if row["source_object_id"] == "FABRIC_SALES.ORDER_LINES"
    )
    assert "authentication=ActiveDirectoryServicePrincipal" in fabric_connector["endpoint_url"]


def test_fabric_total_due_mapping_has_a_deterministic_component_fallback():
    mappings = _seed("meta_mapping")
    total_due = next(row for row in mappings if row["mapping_id"] == "M-QA-FAB-05")

    assert total_due["target_column"] == "total_due_amount"
    assert "coalesce(total_due_amount, subtotal_amount + tax_amount + freight_amount)" in total_due["source_expression"]


def test_fabric_sales_gold_is_source_bound_and_atomically_published():
    entities = {
        entity["gold_entity_id"]: entity
        for entity in _seed("meta_gold_entity")
        if entity.get("source_system_id") == "FABRIC_SALES"
    }

    historical = entities["GH_FCT_FABRIC_SALES_ORDER_LINE"]
    current = entities["GC_FCT_FABRIC_SALES_ORDER_LINE"]
    assert historical["target_table"] == "fct_fabric_sales_order_line_hist"
    assert "ref_fabric_sales_order_lines_h" in historical["select_sql"]
    assert current["publication_group_id"] == "FABRIC_SALES_MART"
    assert current["publish_mode"] == "ATOMIC_SWAP"
    assert current["depends_on_gold_entity_ids"] == [historical["gold_entity_id"]]


def test_delivery_gate_requires_an_active_complete_gold_publication_group():
    audit_ddl = (
        Path(__file__).resolve().parents[1] / "sql" / "01_metadata" / "11_audit_model.sql"
    ).read_text(encoding="utf-8")
    processable_view = audit_ddl.split("CREATE OR REPLACE VIEW v_next_processable_delivery", 1)[1]
    processable_view = processable_view.split("-- Laatste succesvolle business load", 1)[0]

    assert "audit_gold_publication_group pg" in processable_view
    assert "pg.release_status = 'ACTIVE'" in processable_view
    assert "layer = 'GOLD_CURR' AND lr.run_status = 'SUCCESS'" not in processable_view
    assert "r.expected_object_count = (" in processable_view
    assert "meta_source_object" in processable_view


def test_reference_only_delivery_is_completed_by_business_vault_success():
    audit_ddl = (
        Path(__file__).resolve().parents[1] / "sql" / "01_metadata" / "11_audit_model.sql"
    ).read_text(encoding="utf-8")
    processable_view = audit_ddl.split("CREATE OR REPLACE VIEW v_next_processable_delivery", 1)[1]
    processable_view = processable_view.split("-- Laatste succesvolle business load", 1)[0]

    assert "meta_gold_entity ge" in processable_view
    assert "ge.gold_layer = 'CURRENT'" in processable_view
    assert "lr.layer = 'BUSINESS_VAULT'" in processable_view
    assert "lr.run_status = 'SUCCESS'" in processable_view


def test_append_only_audit_event_table_has_no_column_defaults():
    audit_ddl = (
        Path(__file__).resolve().parents[1] / "sql" / "01_metadata" / "11_audit_model.sql"
    ).read_text(encoding="utf-8")
    event_table = audit_ddl.split("CREATE TABLE IF NOT EXISTS audit_load_run_event", 1)[1]
    event_table = event_table.split("CREATE OR REPLACE VIEW v_load_run_status", 1)[0]

    assert "DEFAULT" not in event_table


def test_quarantined_deliveries_are_not_selected_by_the_next_delivery_gate():
    audit_ddl = (
        Path(__file__).resolve().parents[1] / "sql" / "01_metadata" / "11_audit_model.sql"
    ).read_text(encoding="utf-8")
    audit = (
        Path(__file__).resolve().parents[1] / "src" / "contoso_lakehouse" / "audit.py"
    ).read_text(encoding="utf-8")

    assert "delivery_status NOT IN ('QUARANTINED', 'SUPERSEDED')" in audit_ddl
    assert 'transition_delivery(delivery_id, "QUARANTINED", "system", reason)' in audit


def test_bronze_loader_avoids_serverless_unsupported_persistence():
    bronze = (
        Path(__file__).resolve().parents[1] / "src" / "contoso_lakehouse" / "bronze.py"
    ).read_text(encoding="utf-8")

    assert ".persist()" not in bronze
    assert ".unpersist()" not in bronze
    assert "MERGE WITH SCHEMA EVOLUTION INTO {obj.bronze_table_fqn}" in bronze
    assert "spark.databricks.delta.schema.autoMerge.enabled" not in bronze
    assert "t._source_file_path = s._source_file_path" in bronze


def test_quality_engine_avoids_serverless_unsupported_persistence():
    quality = (
        Path(__file__).resolve().parents[1] / "src" / "contoso_lakehouse" / "quality.py"
    ).read_text(encoding="utf-8")

    assert ".persist()" not in quality
    assert ".unpersist()" not in quality


# -- metadata consistentie -------------------------------------------------
def test_dependency_ids_are_unique():
    ids = [d["dependency_id"] for d in _seed("meta_dependency")]
    assert len(ids) == len(set(ids))


def test_dependencies_reference_known_entities():
    known = (
        {o["source_object_id"] for o in _seed("meta_source_object")}
        | {o["source_system_id"] for o in _seed("meta_source_object")}
        | {e["dv_entity_id"] for e in _seed("meta_dv_entity")}
        | {e["gold_entity_id"] for e in _seed("meta_gold_entity")}
    )
    for dep in _seed("meta_dependency"):
        assert dep["entity_id"] in known, dep["dependency_id"]
        assert dep["depends_on_entity_id"] in known, dep["dependency_id"]


def test_dependency_graph_has_no_cycles():
    edges = {}
    for dep in _seed("meta_dependency"):
        edges.setdefault((dep["entity_layer"], dep["entity_id"]), set()).add(
            (dep["depends_on_layer"], dep["depends_on_entity_id"])
        )

    state: dict[tuple[str, str], int] = {}

    def visit(node) -> None:
        if state.get(node) == 1:
            raise AssertionError(f"Cyclus via {node}")
        if state.get(node) == 2:
            return
        state[node] = 1
        for child in edges.get(node, ()):
            visit(child)
        state[node] = 2

    for node in list(edges):
        visit(node)


def test_mapping_ordinal_positions_are_unique_per_target():
    seen: dict[tuple[str, str], set[int]] = {}
    for m in _seed("meta_mapping"):
        key = (m["source_object_id"], m["target_entity"])
        positions = seen.setdefault(key, set())
        assert m["ordinal_position"] not in positions, key
        positions.add(m["ordinal_position"])


def test_dv_entities_reference_known_parents():
    known = {e["dv_entity_id"] for e in _seed("meta_dv_entity")}
    for entity in _seed("meta_dv_entity"):
        for parent in entity["parent_entity_ids"] or []:
            assert parent in known, entity["dv_entity_id"]


def test_satellites_have_hashdiff_columns():
    mappings = _seed("meta_dv_mapping")
    for entity in _seed("meta_dv_entity"):
        if "SATELLITE" not in entity["dv_entity_type"]:
            continue
        assert entity["hashdiff_column"], entity["dv_entity_id"]
        in_scope = [
            m for m in mappings
            if m["dv_entity_id"] == entity["dv_entity_id"] and m["is_in_hashdiff"]
        ]
        assert in_scope, entity["dv_entity_id"]


def test_atomic_swap_entities_have_publication_group():
    for entity in _seed("meta_gold_entity"):
        if entity["publish_mode"] == "ATOMIC_SWAP":
            assert entity.get("publication_group_id"), entity["gold_entity_id"]


def test_public_api_extract_selects_every_connector_field_it_uses():
    notebook = (
        Path(__file__).resolve().parents[1] / "notebooks" / "09_extract_public_api.py"
    ).read_text(encoding="utf-8")
    select_clause = notebook.split("\nSELECT ", 1)[1].split("\nFROM", 1)[0]

    for field in re.findall(r"\brow\.(\w+)", notebook):
        assert field in select_clause, field


def test_public_reference_sources_have_historical_and_current_gold_products():
    entities = _seed("meta_gold_entity")
    by_source = {}
    for entity in entities:
        by_source.setdefault(entity.get("source_system_id", "SALES"), set()).add(entity["gold_entity_id"])

    assert {"GH_DIM_ECB_EXCHANGE_RATE", "GC_DIM_ECB_EXCHANGE_RATE"} <= by_source["ECB"]
    assert {"GH_DIM_NAGER_HOLIDAY", "GC_DIM_NAGER_HOLIDAY"} <= by_source["NAGER"]

    historical_ddl = (Path(__file__).resolve().parents[1] / "sql" / "05_gold" / "50_gold_historical.sql").read_text(encoding="utf-8")
    current_ddl = (Path(__file__).resolve().parents[1] / "sql" / "05_gold" / "51_gold_current.sql").read_text(encoding="utf-8")
    assert "dim_ecb_exchange_rate_hist" in historical_ddl
    assert "dim_nager_holiday_hist" in historical_ddl
    assert "CREATE OR REPLACE VIEW dim_ecb_exchange_rate" in current_ddl
    assert "CREATE OR REPLACE VIEW dim_nager_holiday" in current_ddl


def test_quality_rules_have_valid_severity():
    for rule in _seed("meta_quality_rule"):
        assert rule["severity"] in {"ERROR", "WARNING"}, rule["rule_id"]


def test_quality_rules_explicitly_define_blocking_and_group():
    for rule in _seed("meta_quality_rule"):
        assert "is_blocking" in rule, rule["rule_id"]
        assert isinstance(rule["is_blocking"], bool), rule["rule_id"]
        assert rule.get("rule_group"), rule["rule_id"]


def test_source_objects_define_enterprise_metadata():
    for obj in _seed("meta_source_object"):
        assert obj.get("schema_drift_policy"), obj["source_object_id"]
        assert obj.get("owner_team"), obj["source_object_id"]
        assert obj.get("criticality") in {"LOW", "MEDIUM", "HIGH"}, obj["source_object_id"]


def test_rescue_mode_evolves_bronze_schema_without_dropping_unmapped_columns():
    cbs = next(obj for obj in _seed("meta_source_object") if obj["source_object_id"] == "CBS.JEUGDZORG_WIJK_2025")
    assert cbs["schema_drift_policy"] == "RESCUE"
    bronze_loader = (Path(__file__).resolve().parents[1] / "src" / "contoso_lakehouse" / "bronze.py").read_text(encoding="utf-8")
    assert '"RESCUE": "addNewColumns"' in bronze_loader
    assert ".drop(*extra_columns)" not in bronze_loader


def test_dependency_entries_define_priority_and_retry_policy():
    for dep in _seed("meta_dependency"):
        assert dep.get("priority") is not None, dep["dependency_id"]
        assert dep.get("retry_policy"), dep["dependency_id"]
        assert dep.get("max_retries", 0) >= 1, dep["dependency_id"]


def test_current_publish_entities_define_atomic_swap_metadata():
    for entity in _seed("meta_gold_entity"):
        if entity.get("publish_mode") == "ATOMIC_SWAP":
            assert entity.get("publication_group_id"), entity["gold_entity_id"]
            assert entity.get("publish_status") in {"READY", "ACTIVE", "FAILED"}, entity["gold_entity_id"]
            assert entity.get("pointer_table"), entity["gold_entity_id"]
            assert entity.get("staging_table", "").endswith(("_v1", "_v2")), entity["gold_entity_id"]


def test_current_sales_fact_select_matches_the_gold_contract():
    entity = next(e for e in _seed("meta_gold_entity") if e["gold_entity_id"] == "GC_FCT_SALES")

    for column in (
        "sales_line_hk", "order_hk", "product_hk", "customer_hk", "order_key",
        "order_line_number", "order_date", "order_status", "currency_code",
        "quantity", "unit_price", "discount_amount", "gross_amount", "net_amount",
        "discount_rate", "lead_time_days", "is_cancelled", "ship_date", "delivery_date",
    ):
        assert column in entity["select_sql"], column


def test_returns_employee_and_date_gold_contracts_are_complete():
    entities = {entity["gold_entity_id"]: entity for entity in _seed("meta_gold_entity")}
    historical_ddl = (
        Path(__file__).resolve().parents[1] / "sql" / "05_gold" / "50_gold_historical.sql"
    ).read_text(encoding="utf-8")
    current_ddl = (
        Path(__file__).resolve().parents[1] / "sql" / "05_gold" / "51_gold_current.sql"
    ).read_text(encoding="utf-8")

    assert {"GH_DIM_EMPLOYEE", "GH_FCT_RETURNS", "GC_DIM_EMPLOYEE", "GC_DIM_DATE", "GC_FCT_RETURNS"} <= set(entities)
    assert "employee_hk" in entities["GC_FCT_SALES"]["select_sql"]
    assert "order_date_key" in entities["GC_FCT_SALES"]["select_sql"]
    assert "return_date_key" in entities["GC_FCT_RETURNS"]["select_sql"]
    for contract in ("dim_employee_hist", "fct_returns_hist", "dim_employee_v1", "dim_date_v1", "fct_returns_v1"):
        assert contract in historical_ddl or contract in current_ddl


def test_atomic_swap_slot_selection_uses_configured_staging_table():
    class EmptyResult:
        def collect(self):
            return []

    class EmptySpark:
        def sql(self, statement):
            return EmptyResult()

    entity = GoldEntity(
        gold_entity_id="GC_TEST",
        gold_layer="CURRENT",
        entity_type="DIMENSION",
        target_table_fqn="contoso_gold_tst.current.dim_test",
        target_catalog="contoso_gold_tst",
        target_schema="current",
        target_table="dim_test",
        select_sql="SELECT 1",
        business_key_columns=["test_key"],
        scd_type="SNAPSHOT",
        publish_mode="ATOMIC_SWAP",
        publication_group_id="TEST_MART",
        depends_on_gold_entity_ids=[],
        load_order=1,
        staging_table="contoso_gold_tst.current_internal.dim_test_candidate_v2",
    )
    loader = object.__new__(GoldLoader)
    loader.spark = EmptySpark()
    loader.ctx = type("Context", (), {"settings": Settings(env="tst")})()

    assert loader._target_slot(entity) == "dim_test_candidate_v2"


def test_atomic_swap_publication_uses_configured_pointer_and_activates_build():
    class EmptyResult:
        def collect(self):
            return []

    class BuildingPublicationResult:
        def collect(self):
            return [type("Count", (), {"building_count": 1})()]

    class RecordingSpark:
        def __init__(self):
            self.statements = []

        def sql(self, statement):
            self.statements.append(statement)
            if "building_count" in statement:
                return BuildingPublicationResult()
            if "active_count" in statement:
                return type("ActiveLeaseResult", (), {"collect": lambda _: [type("Count", (), {"active_count": 1})()]})()
            return EmptyResult()

    entity = GoldEntity(
        gold_entity_id="GC_TEST",
        gold_layer="CURRENT",
        entity_type="DIMENSION",
        target_table_fqn="contoso_gold_tst.current.dim_test",
        target_catalog="contoso_gold_tst",
        target_schema="current",
        target_table="dim_test",
        select_sql="SELECT 1",
        business_key_columns=["test_key"],
        scd_type="SNAPSHOT",
        publish_mode="ATOMIC_SWAP",
        publication_group_id="TEST_MART",
        depends_on_gold_entity_ids=[],
        load_order=1,
        pointer_table="contoso_gold_tst.published.dim_test_pointer",
        staging_table="contoso_gold_tst.current_internal.dim_test_candidate_v2",
    )
    loader = object.__new__(GoldLoader)
    loader.spark = RecordingSpark()
    loader.ctx = type("Context", (), {
        "settings": Settings(env="tst"), "batch_id": "batch-1", "delivery_id": "delivery-1"
    })()

    loader.publish_group({"GC_TEST": "publication-1"}, [entity], "lease-1")

    statements = "\n".join(loader.spark.statements)
    assert "publication_status = 'BUILDING'" in statements
    assert "building_count" in statements
    assert "audit_gold_publication_group" in statements
    assert "MERGE INTO" in statements
    assert "publication_status = 'ACTIVE'" in statements
    assert "'publication-1'" in statements


def test_atomic_swap_refuses_incomplete_publication_group_before_pointer_switch():
    class IncompletePublicationResult:
        def collect(self):
            return [type("Count", (), {"building_count": 0})()]

    class RecordingSpark:
        def __init__(self):
            self.statements = []

        def sql(self, statement):
            self.statements.append(statement)
            return IncompletePublicationResult()

    entity = GoldEntity(
        gold_entity_id="GC_TEST",
        gold_layer="CURRENT",
        entity_type="DIMENSION",
        target_table_fqn="contoso_gold_tst.current.dim_test",
        target_catalog="contoso_gold_tst",
        target_schema="current",
        target_table="dim_test",
        select_sql="SELECT 1",
        business_key_columns=["test_key"],
        scd_type="SNAPSHOT",
        publish_mode="ATOMIC_SWAP",
        publication_group_id="TEST_MART",
        depends_on_gold_entity_ids=[],
        load_order=1,
        pointer_table="contoso_gold_tst.published.dim_test_pointer",
        staging_table="contoso_gold_tst.current_internal.dim_test_candidate_v2",
    )
    loader = object.__new__(GoldLoader)
    loader.spark = RecordingSpark()
    loader.ctx = type("Context", (), {"settings": Settings(env="tst")})()

    with pytest.raises(RuntimeError, match="BUILDING-publicaties"):
        loader.publish_group({"GC_TEST": "publication-1"}, [entity], "lease-1")

    statements = "\n".join(loader.spark.statements)
    assert "CREATE OR REPLACE VIEW" not in statements


def test_atomic_swap_promotes_one_group_release_pointer():
    class EmptyResult:
        def collect(self):
            return []

    class BuildingPublicationResult:
        def collect(self):
            return [type("Count", (), {"building_count": 1})()]

    class RecordingSpark:
        def __init__(self):
            self.statements = []

        def sql(self, statement):
            self.statements.append(statement)
            if "physical_slot" in statement:
                return EmptyResult()
            if "active_count" in statement:
                return type("ActiveLeaseResult", (), {"collect": lambda _: [type("Count", (), {"active_count": 1})()]})()
            return BuildingPublicationResult()

    entity = GoldEntity(
        gold_entity_id="GC_TEST",
        gold_layer="CURRENT",
        entity_type="DIMENSION",
        target_table_fqn="contoso_gold_tst.current.dim_test",
        target_catalog="contoso_gold_tst",
        target_schema="current",
        target_table="dim_test",
        select_sql="SELECT 1",
        business_key_columns=["test_key"],
        scd_type="SNAPSHOT",
        publish_mode="ATOMIC_SWAP",
        publication_group_id="TEST_MART",
        depends_on_gold_entity_ids=[],
        load_order=1,
        pointer_table="contoso_gold_tst.current.dim_test",
        staging_table="contoso_gold_tst.current_internal.dim_test_v2",
    )
    loader = object.__new__(GoldLoader)
    loader.spark = RecordingSpark()
    loader.ctx = type("Context", (), {
        "settings": Settings(env="tst"), "batch_id": "batch-1", "delivery_id": "delivery-1"
    })()

    loader.publish_group({"GC_TEST": "publication-1"}, [entity], "lease-1")

    statements = "\n".join(loader.spark.statements)
    assert "audit_gold_publication_group" in statements
    assert "MERGE INTO" in statements
    assert "'TEST_MART'" in statements
    assert "'batch-1'" in statements
    assert "CREATE OR REPLACE VIEW" not in statements


# -- Review 2026-09-07: gedragstests voor nieuwe laadstrategieen -------------


def _bronze_slice_loader():
    class Slice:
        def dropDuplicates(self, columns):
            self.dedupe_columns = columns
            return self

        def createOrReplaceTempView(self, _name):
            pass

    class RecordingSpark:
        def __init__(self):
            self.statements = []

        def sql(self, statement):
            self.statements.append(statement)

    loader = object.__new__(BronzeLoader)
    loader.spark = RecordingSpark()
    return loader, Slice()


def test_bronze_cdc_matches_on_key_and_cdc_op():
    loader, slice_df = _bronze_slice_loader()
    source = type("Source", (), {
        "source_object_id": "SALES.ORDERS_CDC", "bronze_table_fqn": "bronze.orders_cdc",
        "business_key_columns": ["order_key"], "load_strategy": "INCREMENTAL_CDC",
    })()

    loader._merge_bronze_slice(source, slice_df)

    statement = loader.spark.statements[-1]
    assert "MERGE WITH SCHEMA EVOLUTION" in statement
    assert "t._cdc_op <=> s._cdc_op" in statement
    assert "t.order_key <=> s.order_key" in statement


def test_bronze_partial_snapshot_uses_append_semantics_not_delete():
    loader, slice_df = _bronze_slice_loader()
    source = type("Source", (), {
        "source_object_id": "SALES.PARTIAL", "bronze_table_fqn": "bronze.partial",
        "business_key_columns": ["order_key"], "load_strategy": "PARTIAL_SNAPSHOT",
    })()

    loader._merge_bronze_slice(source, slice_df)

    statement = loader.spark.statements[-1]
    assert "MERGE WITH SCHEMA EVOLUTION" in statement
    # append-semantiek: match op bestand + levering + sleutel, géén delete-clausule
    assert "t._source_file_path = s._source_file_path" in statement
    assert "t._delivery_id = s._delivery_id" in statement
    assert "DELETE" not in statement


def _gold_historical_loader():
    class NoOpContextManager:
        def __enter__(self):
            return {}

        def __exit__(self, *_args):
            return False

    class EmptyResult:
        def collect(self):
            return []

    class RecordingSpark:
        def __init__(self):
            self.statements = []

        def sql(self, statement):
            self.statements.append(statement)
            return EmptyResult()

    loader = object.__new__(GoldLoader)
    loader.spark = RecordingSpark()
    loader.ctx = type("Context", (), {
        "batch_id": "batch-1", "load_date_literal": "timestamp'2026-09-01 00:00:00'"
    })()
    loader.audit = type("Audit", (), {"run": lambda *_args: NoOpContextManager()})()
    return loader


def _historical_entity():
    return GoldEntity(
        gold_entity_id="GH_TEST", gold_layer="HISTORICAL", entity_type="DIMENSION",
        target_table_fqn="gold.historical.dim_test", target_catalog="gold", target_schema="historical",
        target_table="dim_test", select_sql="SELECT id, valid_from FROM vault.sat_test",
        business_key_columns=["id", "valid_from"], scd_type="SCD2", publish_mode="MERGE",
        publication_group_id=None, depends_on_gold_entity_ids=[], load_order=1,
        pointer_table=None, staging_table=None,
    )


def test_gold_historical_incremental_window_filters_on_load_date():
    loader = _gold_historical_loader()
    loader.load_historical(_historical_entity(), incremental_since="2026-09-01")

    statement = loader.spark.statements[0]
    assert "WHERE load_date >= timestamp'2026-09-01'" in statement
    assert "MERGE INTO gold.historical.dim_test" in statement


def test_gold_historical_without_window_falls_back_to_full_scan():
    loader = _gold_historical_loader()
    loader.load_historical(_historical_entity())

    statement = loader.spark.statements[0]
    assert "WHERE load_date >=" not in statement
    assert "MERGE INTO gold.historical.dim_test" in statement


def test_orchestrator_critical_gate_blocks_only_high_criticality():
    # Twee niet-kritieke objecten nog niet SUCCESS -> gate moet open blijven.
    orchestrator = _orchestrator({
        "audit_delivery_object": [type("Count", (), {"n": 0})()],
    })
    orchestrator.require_delivery_critical_complete("SALES|2026-09-01")

    # Eén HIGH-object niet SUCCESS -> gate moet blokkeren.
    blocking = _orchestrator({
        "audit_delivery_object": [type("Count", (), {"n": 1})()],
    })
    with pytest.raises(GateNotOpenError, match="kritieke objecten"):
        blocking.require_delivery_critical_complete("SALES|2026-09-01")
