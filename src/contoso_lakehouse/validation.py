"""Validatie van het metadata model.

Compileert elke gegenereerde expressie met EXPLAIN zonder data te lezen. Dit is
de belangrijkste kwaliteitsmaatregel bij honderden tabellen: metadata-fouten
worden hier gevonden in plaats van halverwege een productie-run.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession

from contoso_lakehouse.context import Settings
from contoso_lakehouse.metadata import MetadataRepository


@dataclass(frozen=True)
class ValidationIssue:
    category: str
    entity: str
    message: str


class MetadataValidator:
    def __init__(self, spark: SparkSession, repo: MetadataRepository, settings: Settings) -> None:
        self.spark = spark
        self.repo = repo
        self.settings = settings

    def _explain(self, sql: str) -> str | None:
        try:
            self.spark.sql(f"EXPLAIN {sql}")
            return None
        except Exception as exc:  # noqa: BLE001 - de foutmelding is het resultaat
            return str(exc).splitlines()[0][:500]

    # -- mappings ---------------------------------------------------------
    def validate_source_objects(self) -> list[ValidationIssue]:
        """Blokkeert onuitvoerbare laad- en deletecontracten vóór de runtime."""
        issues: list[ValidationIssue] = []
        strategies = {
            "INCREMENTAL_APPEND", "INCREMENTAL_MERGE", "INCREMENTAL_CDC",
            "SNAPSHOT_SCD2", "PARTIAL_SNAPSHOT", "FULL_OVERWRITE",
        }
        delete_semantics = {"NONE", "SOFT_DELETE_FLAG", "SNAPSHOT_ABSENCE", "CDC_TOMBSTONE"}
        for obj in self.repo.source_objects():
            strategy = getattr(obj, "load_strategy", "")
            deletion = getattr(obj, "delete_semantics", "NONE")
            absence_deletes = bool(getattr(obj, "absence_means_delete", False))
            if strategy not in strategies:
                issues.append(ValidationIssue("SOURCE_OBJECT", obj.source_object_id,
                    f"Niet-ondersteunde load_strategy: {strategy}"))
            if deletion not in delete_semantics:
                issues.append(ValidationIssue("SOURCE_OBJECT", obj.source_object_id,
                    f"Ongeldige delete_semantics: {deletion}"))
            if absence_deletes and strategy != "SNAPSHOT_SCD2":
                issues.append(ValidationIssue("SOURCE_OBJECT", obj.source_object_id,
                    "absence_means_delete vereist SNAPSHOT_SCD2 en een complete snapshot."))
            if deletion == "SOFT_DELETE_FLAG" and not getattr(obj, "deleted_flag_column", None):
                issues.append(ValidationIssue("SOURCE_OBJECT", obj.source_object_id,
                    "SOFT_DELETE_FLAG vereist deleted_flag_column."))
            if deletion == "SNAPSHOT_ABSENCE" and not absence_deletes:
                issues.append(ValidationIssue("SOURCE_OBJECT", obj.source_object_id,
                    "SNAPSHOT_ABSENCE vereist absence_means_delete = true."))
            if deletion == "CDC_TOMBSTONE" and strategy != "INCREMENTAL_CDC":
                issues.append(ValidationIssue("SOURCE_OBJECT", obj.source_object_id,
                    "CDC_TOMBSTONE vereist INCREMENTAL_CDC."))
        return issues

    def validate_dependencies(self) -> list[ValidationIssue]:
        allowed = {"DELIVERY_COMPLETE", "UPSTREAM_SUCCESS", "SAME_DELIVERY"}
        return [
            ValidationIssue("DEPENDENCY", dependency.dependency_id,
                f"Ongeldig dependency_type: {dependency.dependency_type}")
            for dependency in self.repo.dependencies()
            if dependency.dependency_type not in allowed
        ]

    def validate_mappings(self) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        for obj in self.repo.source_objects():
            mappings = self.repo.mappings(obj.source_object_id, "QUALITY")
            if not mappings:
                issues.append(ValidationIssue(
                    "MAPPING", obj.source_object_id, "Geen QUALITY-mapping gedefinieerd."
                ))
                continue
            positions = [m.ordinal_position for m in mappings]
            if len(positions) != len(set(positions)):
                issues.append(ValidationIssue(
                    "MAPPING", obj.source_object_id, "Dubbele ordinal_position."
                ))
            for m in mappings:
                expr = m.source_expression or m.source_column
                err = self._explain(
                    f"SELECT cast({expr} AS {m.target_data_type}) FROM {obj.bronze_table_fqn} LIMIT 0"
                )
                if err:
                    issues.append(ValidationIssue("MAPPING", m.mapping_id, err))
        return issues

    # -- kwaliteitsregels -------------------------------------------------
    def validate_quality_rules(self) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        for obj in self.repo.source_objects():
            for rule in self.repo.quality_rules(obj.source_object_id):
                clause = (
                    f"SELECT {rule.rule_expression} FROM {obj.quality_table_fqn} LIMIT 0"
                    if rule.is_set_level
                    else f"SELECT * FROM {obj.quality_table_fqn} WHERE {rule.rule_expression} LIMIT 0"
                )
                err = self._explain(clause)
                if err:
                    issues.append(ValidationIssue("DQ_RULE", rule.rule_id, err))
                if rule.severity not in {"ERROR", "WARNING"}:
                    issues.append(ValidationIssue(
                        "DQ_RULE", rule.rule_id, f"Ongeldige severity: {rule.severity}"
                    ))
        return issues

    # -- data vault -------------------------------------------------------
    def validate_dv(self) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        known = {e.dv_entity_id for e in self.repo.dv_entities()}
        for entity in self.repo.dv_entities():
            for parent in entity.parent_entity_ids:
                if parent not in known:
                    issues.append(ValidationIssue(
                        "DV_ENTITY", entity.dv_entity_id, f"Onbekende parent: {parent}"
                    ))
            mappings = self.repo.dv_mappings(entity.dv_entity_id)
            if not mappings and entity.dv_entity_type != "PIT":
                issues.append(ValidationIssue(
                    "DV_ENTITY", entity.dv_entity_id, "Geen kolommapping gedefinieerd."
                ))
            if entity.hashdiff_column and not any(m.is_in_hashdiff for m in mappings):
                issues.append(ValidationIssue(
                    "DV_ENTITY", entity.dv_entity_id,
                    "Satellite heeft een hashdiff maar geen enkele kolom in de hashdiff-scope."
                ))
            for m in mappings:
                source = self.repo.source_object(m.source_object_id).quality_table_fqn
                err = self._explain(f"SELECT {m.source_expression} FROM {source} LIMIT 0")
                if err:
                    issues.append(ValidationIssue("DV_MAPPING", m.dv_mapping_id, err))
        return issues

    # -- gold -------------------------------------------------------------
    def validate_gold(self) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        known = {e.gold_entity_id for e in self.repo.gold_entities()}
        group_sources: dict[str, set[str]] = {}
        for entity in self.repo.gold_entities():
            if entity.publication_group_id:
                group_sources.setdefault(entity.publication_group_id, set()).add(entity.source_system_id)
            for dep in entity.depends_on_gold_entity_ids:
                if dep not in known:
                    issues.append(ValidationIssue(
                        "GOLD", entity.gold_entity_id, f"Onbekende afhankelijkheid: {dep}"
                    ))
            if entity.publish_mode == "ATOMIC_SWAP" and not entity.publication_group_id:
                issues.append(ValidationIssue(
                    "GOLD", entity.gold_entity_id,
                    "ATOMIC_SWAP zonder publication_group_id: cross-entity consistentie niet gegarandeerd."
                ))
            if entity.publish_mode == "ATOMIC_SWAP" and not entity.pointer_table:
                issues.append(ValidationIssue(
                    "GOLD", entity.gold_entity_id,
                    "ATOMIC_SWAP zonder pointer_table: veilige publicatie is niet mogelijk."
                ))
            if entity.publish_mode == "ATOMIC_SWAP" and not (
                entity.staging_table or ""
            ).endswith(("_v1", "_v2")):
                issues.append(ValidationIssue(
                    "GOLD", entity.gold_entity_id,
                    "ATOMIC_SWAP staging_table moet eindigen op _v1 of _v2."
                ))
            err = self._explain(entity.select_sql)
            if err:
                issues.append(ValidationIssue("GOLD", entity.gold_entity_id, err))
        for group, source_systems in group_sources.items():
            if len(source_systems) > 1:
                issues.append(ValidationIssue(
                    "GOLD", group,
                    f"Publication group bevat meerdere bronsystemen: {sorted(source_systems)}",
                ))
        return issues

    def validate_all(self) -> list[ValidationIssue]:
        return (
            self.validate_source_objects()
            + self.validate_dependencies()
            + self.validate_mappings()
            + self.validate_quality_rules()
            + self.validate_dv()
            + self.validate_gold()
        )
