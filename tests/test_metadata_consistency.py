"""Tests op de logica die zonder Spark te controleren is."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from contoso_lakehouse.context import Settings
from contoso_lakehouse.bronze import BronzeLoader
from contoso_lakehouse.gold import GoldLoader
from contoso_lakehouse.hashing import hash_key, hashdiff
from contoso_lakehouse.metadata import GoldEntity
from contoso_lakehouse.orchestration import parallel_execution_waves
from contoso_lakehouse.seed import metadata_version
from contoso_lakehouse.sqlutil import safe_identifier

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
