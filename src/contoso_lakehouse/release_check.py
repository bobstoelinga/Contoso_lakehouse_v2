"""Lokale releasevalidatie voor Git-beheerde metadata.

Deze module vereist geen Spark en kan daarom als eerste CI-gate draaien vóór
een Databricks Asset Bundle wordt gedeployed.
"""

from __future__ import annotations

import json
from pathlib import Path

from contoso_lakehouse.seed import _KEY_COLUMNS, _SEED_FILES, metadata_version


def validate_seed_release(seed_dir: str | Path) -> tuple[str, list[str]]:
    """Geeft de releasefingerprint en alle onafhankelijke validatiefouten terug."""
    directory = Path(seed_dir)
    records_by_table: dict[str, list[dict]] = {}
    issues: list[str] = []
    for table, filename in _SEED_FILES.items():
        path = directory / filename
        try:
            records = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            issues.append(f"{filename}: niet leesbare JSON: {exc}")
            continue
        if not isinstance(records, list):
            issues.append(f"{filename}: root moet een JSON-array zijn.")
            continue
        if any(not isinstance(record, dict) for record in records):
            issues.append(f"{filename}: iedere record moet een JSON-object zijn.")
            continue
        records_by_table[table] = records
        key = _KEY_COLUMNS[table]
        values = [record.get(key) for record in records]
        if None in values:
            issues.append(f"{filename}: ontbrekende sleutel {key}.")
        if len(values) != len(set(values)):
            issues.append(f"{filename}: dubbele waarde voor sleutel {key}.")

    source_objects = {record.get("source_object_id") for record in records_by_table.get("meta_source_object", [])}
    source_systems = {record.get("source_system_id") for record in records_by_table.get("meta_source_system", [])}
    active_source_systems = {
        record.get("source_system_id") for record in records_by_table.get("meta_source_system", [])
        if record.get("is_active")
    }
    governance_by_source = {
        record.get("source_system_id"): record
        for record in records_by_table.get("meta_data_governance_policy", [])
        if record.get("is_active")
    }
    active_gold_groups = {
        record.get("publication_group_id") for record in records_by_table.get("meta_gold_entity", [])
        if record.get("is_active") and record.get("gold_layer") == "CURRENT" and record.get("publication_group_id")
    }
    products_by_group = {
        record.get("publication_group_id"): record
        for record in records_by_table.get("meta_gold_data_product", [])
        if record.get("is_active")
    }
    dv_entities = {record.get("dv_entity_id") for record in records_by_table.get("meta_dv_entity", [])}
    gold_entities = {record.get("gold_entity_id") for record in records_by_table.get("meta_gold_entity", [])}
    known_entities = source_objects | source_systems | dv_entities | gold_entities
    for dependency in records_by_table.get("meta_dependency", []):
        for field in ("entity_id", "depends_on_entity_id"):
            if dependency.get(field) not in known_entities:
                issues.append(
                    f"meta_dependency {dependency.get('dependency_id')}: onbekende {field}={dependency.get(field)!r}."
                )

    for policy in records_by_table.get("meta_table_maintenance_policy", []):
        if policy.get("vacuum_retain_hours", 0) < 168:
            issues.append(f"maintenance policy {policy.get('policy_id')}: VACUUM-retentie is korter dan 168 uur.")
        if policy.get("optimize_mode") not in {"SCHEDULED", "DISABLED"}:
            issues.append(f"maintenance policy {policy.get('policy_id')}: ongeldige optimize_mode.")

    classifications = {"PUBLIC", "INTERNAL", "CONFIDENTIAL", "RESTRICTED"}
    sla_tiers = {"CRITICAL", "STANDARD", "BEST_EFFORT"}
    for source_system_id in active_source_systems:
        policy = governance_by_source.get(source_system_id)
        if policy is None:
            issues.append(f"actief bronsysteem {source_system_id}: governancepolicy ontbreekt.")
            continue
        if policy.get("pii_classification") not in classifications:
            issues.append(f"governance policy {source_system_id}: ongeldige pii_classification.")
        if policy.get("sla_tier") not in sla_tiers:
            issues.append(f"governance policy {source_system_id}: ongeldige sla_tier.")
        if not isinstance(policy.get("retention_days"), int) or policy["retention_days"] < 1:
            issues.append(f"governance policy {source_system_id}: retention_days moet positief zijn.")

    compatibility_policies = {"BACKWARD_COMPATIBLE", "VERSIONED_BREAKING_CHANGE"}
    for group in active_gold_groups:
        product = products_by_group.get(group)
        if product is None:
            issues.append(f"actieve Gold-publicatiegroep {group}: data-productcontract ontbreekt.")
            continue
        if not isinstance(product.get("refresh_sla_hours"), int) or product["refresh_sla_hours"] < 1:
            issues.append(f"Gold data product {group}: refresh_sla_hours moet positief zijn.")
        if product.get("compatibility_policy") not in compatibility_policies:
            issues.append(f"Gold data product {group}: ongeldige compatibility_policy.")

    return metadata_version(records_by_table), issues


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    version, issues = validate_seed_release(root / "metadata" / "seed")
    if issues:
        print("Metadatarelease afgekeurd:")
        for issue in issues:
            print(f"- {issue}")
        raise SystemExit(1)
    print(f"Metadatarelease geldig: {version}")


if __name__ == "__main__":
    main()