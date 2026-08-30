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
        for entity in self.repo.gold_entities():
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
            err = self._explain(entity.select_sql)
            if err:
                issues.append(ValidationIssue("GOLD", entity.gold_entity_id, err))
        return issues

    def validate_all(self) -> list[ValidationIssue]:
        return (
            self.validate_mappings()
            + self.validate_quality_rules()
            + self.validate_dv()
            + self.validate_gold()
        )
