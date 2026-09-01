"""Toegang tot het metadata model.

Alle configuratie wordt hier gelezen. Geen andere module bevat kennis van
bronobjecten, mappings, regels of afhankelijkheden.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Iterable

from pyspark.sql import SparkSession

from contoso_lakehouse.context import Settings
from contoso_lakehouse.sqlutil import safe_identifier

__all__ = ["MetadataRepository", "safe_identifier"]


@dataclass(frozen=True)
class SourceObject:
    source_object_id: str
    source_system_id: str
    object_name: str
    file_pattern: str
    file_format: str
    reader_options: dict[str, str]
    load_strategy: str
    business_key_columns: list[str]
    change_tracking_columns: list[str]
    deleted_flag_column: str | None
    is_mandatory_in_delivery: bool
    schema_drift_policy: str = "STRICT"
    owner_team: str = "unknown"
    criticality: str = "MEDIUM"
    bronze_table_fqn: str = ""
    bronze_partition_columns: list[str] = field(default_factory=list)
    checkpoint_path: str = ""
    schema_location_path: str = ""
    schema_evolution_mode: str = "addNewColumns"
    max_files_per_trigger: int = 1000
    quality_table_fqn: str = ""
    reject_table_fqn: str = ""
    load_order: int = 0


@dataclass(frozen=True)
class QualityRule:
    rule_id: str
    source_object_id: str
    rule_name: str
    rule_type: str
    target_columns: list[str]
    rule_expression: str
    severity: str
    reject_reason_code: str
    reject_reason_text: str
    execution_order: int
    threshold_pct: float | None
    is_blocking: bool = True
    rule_group: str = "CORE"

    @property
    def is_set_level(self) -> bool:
        """Regels met window-functies of subqueries vereisen een dataset-pass."""
        return self.rule_type in {"UNIQUE", "REFERENTIAL"} or " OVER (" in self.rule_expression.upper()


@dataclass(frozen=True)
class DvEntity:
    dv_entity_id: str
    dv_entity_type: str
    dv_zone: str
    target_table_fqn: str
    physical_table_fqn: str
    hash_key_column: str
    parent_entity_ids: list[str]
    business_key_columns: list[str]
    hashdiff_column: str | None
    record_source_expr: str
    load_order: int


@dataclass(frozen=True)
class GoldEntity:
    gold_entity_id: str
    gold_layer: str
    entity_type: str
    target_table_fqn: str
    target_catalog: str
    target_schema: str
    target_table: str
    select_sql: str
    business_key_columns: list[str]
    scd_type: str
    publish_mode: str
    publication_group_id: str | None
    depends_on_gold_entity_ids: list[str]
    load_order: int
    publish_status: str = "READY"
    pointer_table: str | None = None
    staging_table: str | None = None


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    return list(value)


class MetadataRepository:
    """Leest en cachet het metadata model."""

    def __init__(self, spark: SparkSession, settings: Settings) -> None:
        self.spark = spark
        self.settings = settings
        self._meta = f"{settings.meta_catalog}.metadata"
        self._audit = f"{settings.meta_catalog}.audit"

    # -- helpers ----------------------------------------------------------
    def _rows(self, table: str, where: str = "is_active") -> list[Any]:
        return self.spark.sql(f"SELECT * FROM {table} WHERE {where}").collect()

    def _fqn(self, catalog: str, schema: str, table: str) -> str:
        return ".".join(
            safe_identifier(self.settings.resolve(p)) for p in (catalog, schema, table)
        )

    # -- bronobjecten -----------------------------------------------------
    @lru_cache(maxsize=1)
    def source_objects(self) -> tuple[SourceObject, ...]:
        rows = self._rows(f"{self._meta}.meta_source_object")
        objects = [
            SourceObject(
                source_object_id=r.source_object_id,
                source_system_id=r.source_system_id,
                object_name=r.object_name,
                file_pattern=r.file_pattern,
                file_format=r.file_format,
                reader_options=dict(r.reader_options or {}),
                load_strategy=r.load_strategy,
                business_key_columns=_as_list(r.business_key_columns),
                change_tracking_columns=_as_list(r.change_tracking_columns),
                deleted_flag_column=r.deleted_flag_column,
                is_mandatory_in_delivery=r.is_mandatory_in_delivery,
                schema_drift_policy=getattr(r, "schema_drift_policy", "STRICT"),
                owner_team=getattr(r, "owner_team", "unknown"),
                criticality=getattr(r, "criticality", "MEDIUM"),
                bronze_table_fqn=self._fqn(r.bronze_catalog, r.bronze_schema, r.bronze_table),
                bronze_partition_columns=_as_list(r.bronze_partition_columns),
                checkpoint_path=self.settings.resolve(r.checkpoint_path),
                schema_location_path=self.settings.resolve(r.schema_location_path),
                schema_evolution_mode=r.schema_evolution_mode,
                max_files_per_trigger=r.max_files_per_trigger or 1000,
                quality_table_fqn=self._fqn(r.quality_catalog, r.quality_schema, r.quality_table),
                reject_table_fqn=self._fqn(r.reject_catalog, r.reject_schema, r.reject_table),
                load_order=r.load_order,
            )
            for r in rows
        ]
        return tuple(sorted(objects, key=lambda o: o.load_order))

    def source_object(self, source_object_id: str) -> SourceObject:
        for obj in self.source_objects():
            if obj.source_object_id == source_object_id:
                return obj
        raise KeyError(f"Onbekend bronobject: {source_object_id}")

    def mandatory_objects(self, source_system_id: str) -> tuple[SourceObject, ...]:
        return tuple(
            o for o in self.source_objects()
            if o.source_system_id == source_system_id and o.is_mandatory_in_delivery
        )

    # -- kwaliteitsregels -------------------------------------------------
    def quality_rules(self, source_object_id: str) -> list[QualityRule]:
        rows = self.spark.sql(
            f"""
            SELECT * FROM {self._meta}.meta_quality_rule
            WHERE is_active AND source_object_id = '{safe_identifier(source_object_id)}'
            ORDER BY execution_order
            """
        ).collect()
        return [
            QualityRule(
                rule_id=r.rule_id,
                source_object_id=r.source_object_id,
                rule_name=r.rule_name,
                rule_type=r.rule_type,
                target_columns=_as_list(r.target_columns),
                rule_expression=self.settings.resolve(r.rule_expression),
                severity=r.severity,
                reject_reason_code=r.reject_reason_code,
                reject_reason_text=r.reject_reason_text,
                execution_order=r.execution_order,
                threshold_pct=r.threshold_pct,
                is_blocking=bool(getattr(r, "is_blocking", True)),
                rule_group=getattr(r, "rule_group", "CORE"),
            )
            for r in rows
        ]

    # -- mappings ---------------------------------------------------------
    def mappings(self, source_object_id: str, target_layer: str) -> list[Any]:
        return self.spark.sql(
            f"""
            SELECT * FROM {self._meta}.meta_mapping
            WHERE is_active
              AND source_object_id = '{safe_identifier(source_object_id)}'
              AND target_layer     = '{safe_identifier(target_layer)}'
            ORDER BY ordinal_position
            """
        ).collect()

    # -- data vault -------------------------------------------------------
    @lru_cache(maxsize=1)
    def dv_entities(self) -> tuple[DvEntity, ...]:
        rows = self._rows(f"{self._meta}.meta_dv_entity")
        entities = []
        for r in rows:
            view_fqn = self._fqn(r.target_catalog, r.target_schema, r.target_table)
            # Satellites zijn insert-only: de fysieke tabel heeft het suffix _h.
            physical = view_fqn + "_h" if "SATELLITE" in r.dv_entity_type else view_fqn
            entities.append(
                DvEntity(
                    dv_entity_id=r.dv_entity_id,
                    dv_entity_type=r.dv_entity_type,
                    dv_zone=r.dv_zone,
                    target_table_fqn=view_fqn,
                    physical_table_fqn=physical,
                    hash_key_column=safe_identifier(r.hash_key_column),
                    parent_entity_ids=_as_list(r.parent_entity_ids),
                    business_key_columns=_as_list(r.business_key_columns),
                    hashdiff_column=r.hashdiff_column,
                    record_source_expr=r.record_source_expr,
                    load_order=r.load_order,
                )
            )
        return tuple(sorted(entities, key=lambda e: e.load_order))

    def dv_mappings(self, dv_entity_id: str) -> list[Any]:
        return self.spark.sql(
            f"""
            SELECT * FROM {self._meta}.meta_dv_mapping
            WHERE is_active AND dv_entity_id = '{safe_identifier(dv_entity_id)}'
            ORDER BY ordinal_position
            """
        ).collect()

    # -- gold -------------------------------------------------------------
    @lru_cache(maxsize=1)
    def gold_entities(self) -> tuple[GoldEntity, ...]:
        rows = self._rows(f"{self._meta}.meta_gold_entity")
        entities = [
            GoldEntity(
                gold_entity_id=r.gold_entity_id,
                gold_layer=r.gold_layer,
                entity_type=r.entity_type,
                target_table_fqn=self._fqn(r.target_catalog, r.target_schema, r.target_table),
                target_catalog=self.settings.resolve(r.target_catalog),
                target_schema=r.target_schema,
                target_table=r.target_table,
                select_sql=self.settings.resolve(r.select_sql),
                business_key_columns=_as_list(r.business_key_columns),
                scd_type=r.scd_type,
                publish_mode=r.publish_mode,
                publication_group_id=r.publication_group_id,
                publish_status=getattr(r, "publish_status", "READY"),
                pointer_table=self.settings.resolve(getattr(r, "pointer_table", "")) or None,
                staging_table=self.settings.resolve(getattr(r, "staging_table", "")) or None,
                depends_on_gold_entity_ids=_as_list(r.depends_on_gold_entity_ids),
                load_order=r.load_order,
            )
            for r in rows
        ]
        return tuple(sorted(entities, key=lambda e: e.load_order))

    # -- afhankelijkheden -------------------------------------------------
    def dependencies(self) -> list[Any]:
        return self._rows(f"{self._meta}.meta_dependency")

    def dependencies_for(self, entity_id: str, layer: str) -> list[Any]:
        return [
            d for d in self.dependencies()
            if d.entity_id == entity_id and d.entity_layer == layer and d.is_blocking
        ]
