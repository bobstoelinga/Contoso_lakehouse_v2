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
from contoso_lakehouse.metadata import GoldEntity
from contoso_lakehouse.orchestration import (
    DependencyCycleError,
    GateNotOpenError,
    Orchestrator,
    parallel_execution_waves,
)
from contoso_lakehouse.quality import QualityBatchQuarantined, QualityEngine
from contoso_lakehouse.seed import metadata_version
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
SETUP_NOTEBOOK = Path(__file__).resolve().parents[1] / "notebooks" / "00_setup_lakehouse.py"


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


def test_audit_run_records_failure_and_preserves_the_original_exception():
    spark = _RecordingSpark({"audit_metadata_version": [type("Version", (), {"metadata_version": "metadata-v1"})()]})
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
    with pytest.raises(ValueError, match="Niet-ondersteunde load_strategy"):
        loader._merge_bronze_slice(invalid_source, Slice())


@pytest.mark.parametrize(
    ("policy", "columns", "expected_error"),
    [
        ("RESCUE", ["new_attribute"], None),
        ("STRICT", ["new_attribute"], "STRICT"),
        ("ALLOW_NEW_COLUMNS_WITH_APPROVAL", ["new_attribute"], "mappinggoedkeuring"),
        ("UNDEFINED", ["new_attribute"], "onbekend schema_drift_policy"),
        ("STRICT", [], None),
    ],
)
def test_bronze_schema_drift_policy_is_enforced_before_merge(policy, columns, expected_error):
    loader = object.__new__(BronzeLoader)
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
        "schema_drift_policy", "owner_team", "criticality",
        "priority", "retry_policy", "max_retries",
        "is_blocking", "rule_group",
        "publish_status", "pointer_table", "staging_table",
    ):
        assert field in ddl, field
        assert field in setup, field
    assert "ALTER TABLE {fqn} ADD COLUMNS" in setup


def test_delivery_supersede_is_auditable_and_requires_approval():
    audit_ddl = (
        Path(__file__).resolve().parents[1] / "sql" / "01_metadata" / "11_audit_model.sql"
    ).read_text(encoding="utf-8")
    remediation = (
        Path(__file__).resolve().parents[1] / "notebooks" / "07_supersede_delivery.py"
    ).read_text(encoding="utf-8")

    for field in ("superseded_at", "superseded_by", "supersede_reason", "supersede_approval_reference"):
        assert field in audit_ddl
        assert field in remediation
    assert "audit_gold_publication_group" in remediation
    assert "g.release_status = 'ACTIVE'" in remediation
    assert "if row.gold_published:" in remediation
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

    for field in ("quarantined_at", "quarantine_reason"):
        assert field in audit_ddl
    for field in ("released_at", "released_by", "release_reason", "release_approval_reference"):
        assert field in audit_ddl
        assert field in release
    assert 'row.delivery_status != "QUARANTINED"' in release
    assert "AND delivery_status = 'QUARANTINED'" in release
    assert "release_quarantined_delivery" in workflow


def test_serverless_pipeline_fans_out_bronze_with_bounded_concurrency():
    workflow = PIPELINE_WORKFLOW.read_text(encoding="utf-8")
    planner = (
        Path(__file__).resolve().parents[1] / "notebooks" / "06_plan_bronze_fanout.py"
    ).read_text(encoding="utf-8")
    bronze = (
        Path(__file__).resolve().parents[1] / "notebooks" / "10_bronze_autoloader.py"
    ).read_text(encoding="utf-8")

    assert "task_key: plan_bronze_fanout" in workflow
    assert "for_each_task:" in workflow
    assert "{{tasks.plan_bronze_fanout.values.bronze_inputs}}" in workflow
    assert "concurrency: ${var.bronze_parallelism}" in workflow
    assert "name: source_system_id" in workflow
    assert 'source_system_id: "{{job.parameters.source_system_id}}"' in workflow
    assert "currentRunId" not in planner
    assert "taskValues.set" not in bronze


def test_demo_generator_creates_files_matching_source_object_patterns():
    generator = (
        Path(__file__).resolve().parents[1] / "notebooks" / "01_generate_demo_delivery.py"
    ).read_text(encoding="utf-8")

    assert "dbutils.fs.mv(part_file" in generator
    for object_name in ("customers", "products", "orders"):
        assert f'write_delivery_file({object_name}' in generator


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
    assert "SET delivery_status = 'QUARANTINED'" in audit


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

    loader.publish_group({"GC_TEST": "publication-1"}, [entity])

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
        loader.publish_group({"GC_TEST": "publication-1"}, [entity])

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

    loader.publish_group({"GC_TEST": "publication-1"}, [entity])

    statements = "\n".join(loader.spark.statements)
    assert "audit_gold_publication_group" in statements
    assert "MERGE INTO" in statements
    assert "'TEST_MART'" in statements
    assert "'batch-1'" in statements
    assert "CREATE OR REPLACE VIEW" not in statements
