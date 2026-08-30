"""Tests op de logica die zonder Spark te controleren is."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from contoso_lakehouse.context import Settings
from contoso_lakehouse.hashing import hash_key, hashdiff
from contoso_lakehouse.sqlutil import safe_identifier

SEED_DIR = Path(__file__).resolve().parents[1] / "metadata" / "seed"


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
