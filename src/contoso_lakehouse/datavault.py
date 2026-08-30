"""Metadata-gedreven Data Vault 2.0 loader.

Genereert per entiteit de SQL op basis van ``meta_dv_entity`` en
``meta_dv_mapping``. Er is geen entiteit-specifieke code.

Alle inserts zijn idempotent op ``_batch_id``: een herstart van een gefaalde run
levert geen dubbele rijen op.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from contoso_lakehouse.audit import AuditLogger
from contoso_lakehouse.context import RunContext
from contoso_lakehouse.hashing import hash_key, hashdiff
from contoso_lakehouse.metadata import DvEntity, MetadataRepository, safe_identifier


class VaultLoader:
    def __init__(self, spark: SparkSession, repo: MetadataRepository, ctx: RunContext) -> None:
        self.spark = spark
        self.repo = repo
        self.ctx = ctx
        self.audit = AuditLogger(spark, ctx)

    # -- helpers ----------------------------------------------------------
    def _source_table(self, source_object_id: str) -> str:
        return self.repo.source_object(source_object_id).quality_table_fqn

    def _delivery_filter(self) -> str:
        return f"_delivery_id = '{self.ctx.delivery_id}'"

    def _purge_batch(self, entity: DvEntity) -> None:
        self.spark.sql(
            f"DELETE FROM {entity.physical_table_fqn} WHERE _batch_id = '{self.ctx.batch_id}'"
        )

    # -- HUB --------------------------------------------------------------
    def _load_hub(self, entity: DvEntity) -> int:
        mappings = self.repo.dv_mappings(entity.dv_entity_id)
        bks = [m for m in mappings if m.column_role == "BUSINESS_KEY"]
        source_object_id = bks[0].source_object_id
        source = self._source_table(source_object_id)
        collision = source_object_id.split(".", 1)[0]

        bk_exprs = [m.source_expression for m in bks]
        bk_targets = [safe_identifier(m.target_column) for m in bks]
        hk_expr = hash_key(bk_exprs, collision_code=collision)

        select_cols = ", ".join(
            f"{expr} AS {target}" for expr, target in zip(bk_exprs, bk_targets)
        )
        sql = f"""
        MERGE INTO {entity.physical_table_fqn} t
        USING (
          SELECT DISTINCT {hk_expr} AS {entity.hash_key_column},
                 {select_cols},
                 '{collision}' AS bk_collision_code,
                 {self.ctx.load_date_literal} AS load_date,
                 {entity.record_source_expr} AS record_source,
                 '{self.ctx.batch_id}' AS _batch_id
          FROM {source}
          WHERE {self._delivery_filter()}
        ) s ON t.{entity.hash_key_column} = s.{entity.hash_key_column}
        WHEN NOT MATCHED THEN INSERT *
        """
        return self._execute(sql)

    # -- LINK -------------------------------------------------------------
    def _load_link(self, entity: DvEntity) -> int:
        mappings = self.repo.dv_mappings(entity.dv_entity_id)
        source_object_id = mappings[0].source_object_id
        source = self._source_table(source_object_id)
        collision = source_object_id.split(".", 1)[0]

        parents = self.repo.dv_entities()
        parent_by_id = {e.dv_entity_id: e for e in parents}
        parent_hash_keys = {parent_by_id[p].hash_key_column for p in entity.parent_entity_ids}

        unit_exprs, select_parts = [], []
        for m in mappings:
            if m.column_role in {"BUSINESS_KEY", "DRIVING_KEY"}:
                if m.target_column not in parent_hash_keys:
                    raise ValueError(
                        f"{entity.dv_entity_id}: {m.target_column} hoort bij geen enkele parent hub"
                    )
                hk = hash_key([m.source_expression], collision_code=collision)
                unit_exprs.append(m.source_expression)
                select_parts.append(f"{hk} AS {safe_identifier(m.target_column)}")
            elif m.column_role == "DEGENERATE":
                unit_exprs.append(m.source_expression)
                select_parts.append(
                    f"cast({m.source_expression} AS {m.target_data_type}) AS {safe_identifier(m.target_column)}"
                )

        lnk_hk = hash_key(unit_exprs, collision_code=collision)
        sql = f"""
        MERGE INTO {entity.physical_table_fqn} t
        USING (
          SELECT DISTINCT {lnk_hk} AS {entity.hash_key_column},
                 {', '.join(select_parts)},
                 {self.ctx.load_date_literal} AS load_date,
                 {entity.record_source_expr} AS record_source,
                 '{self.ctx.batch_id}' AS _batch_id
          FROM {source}
          WHERE {self._delivery_filter()}
        ) s ON t.{entity.hash_key_column} = s.{entity.hash_key_column}
        WHEN NOT MATCHED THEN INSERT *
        """
        return self._execute(sql)

    # -- SATELLITE --------------------------------------------------------
    def _load_satellite(self, entity: DvEntity) -> int:
        mappings = self.repo.dv_mappings(entity.dv_entity_id)
        source_object_id = mappings[0].source_object_id
        source = self._source_table(source_object_id)
        collision = source_object_id.split(".", 1)[0]

        parent = next(e for e in self.repo.dv_entities() if e.dv_entity_id == entity.parent_entity_ids[0])
        parent_mappings = self.repo.dv_mappings(parent.dv_entity_id)
        parent_units = [
            m.source_expression for m in parent_mappings
            if m.column_role in {"BUSINESS_KEY", "DEGENERATE"}
        ]
        hk_expr = hash_key(parent_units, collision_code=collision)

        descriptive = [m for m in mappings if m.column_role == "DESCRIPTIVE"]
        hd_exprs = [m.source_expression for m in descriptive if m.is_in_hashdiff]
        hd_expr = hashdiff(hd_exprs)
        attr_select = ", ".join(
            f"cast({m.source_expression} AS {m.target_data_type}) AS {safe_identifier(m.target_column)}"
            for m in descriptive
        )
        attr_names = ", ".join(safe_identifier(m.target_column) for m in descriptive)

        # Alleen wegschrijven als de hashdiff verschilt van de laatst bekende versie.
        sql = f"""
        INSERT INTO {entity.physical_table_fqn}
          ({entity.hash_key_column}, load_date, hashdiff, record_source, _batch_id, {attr_names})
        WITH staged AS (
          SELECT {hk_expr} AS {entity.hash_key_column},
                 {hd_expr} AS hashdiff,
                 {attr_select}
          FROM {source}
          WHERE {self._delivery_filter()}
        ),
        deduped AS (
          SELECT * FROM staged
          QUALIFY row_number() OVER (
            PARTITION BY {entity.hash_key_column} ORDER BY hashdiff) = 1
        ),
        latest AS (
          SELECT {entity.hash_key_column}, hashdiff FROM {entity.physical_table_fqn}
          QUALIFY row_number() OVER (
            PARTITION BY {entity.hash_key_column} ORDER BY load_date DESC) = 1
        )
        SELECT d.{entity.hash_key_column},
               {self.ctx.load_date_literal},
               d.hashdiff,
               {entity.record_source_expr},
               '{self.ctx.batch_id}',
               {', '.join('d.' + safe_identifier(m.target_column) for m in descriptive)}
        FROM deduped d
        LEFT JOIN latest l ON l.{entity.hash_key_column} = d.{entity.hash_key_column}
        WHERE l.hashdiff IS NULL OR l.hashdiff <> d.hashdiff
        """
        return self._execute(sql)

    # -- uitvoering -------------------------------------------------------
    def _execute(self, sql: str) -> int:
        result = self.spark.sql(sql)
        try:
            row = result.collect()[0].asDict()
            return int(row.get("num_inserted_rows", row.get("num_affected_rows", 0)) or 0)
        except (IndexError, AttributeError):
            return 0

    def load(self, dv_entity_id: str) -> int:
        entity = next(e for e in self.repo.dv_entities() if e.dv_entity_id == dv_entity_id)
        layer = "BUSINESS_VAULT" if entity.dv_zone == "BUSINESS_VAULT" else "RAW_VAULT"
        with self.audit.run(layer, dv_entity_id) as stats:
            self._purge_batch(entity)
            if entity.dv_entity_type == "HUB":
                inserted = self._load_hub(entity)
            elif entity.dv_entity_type == "LINK":
                inserted = self._load_link(entity)
            elif entity.dv_entity_type in {"SATELLITE", "LINK_SATELLITE"}:
                inserted = self._load_satellite(entity)
            else:
                raise NotImplementedError(f"Type {entity.dv_entity_type} nog niet ondersteund")
            stats["rows_inserted"] = inserted
            return inserted

    def load_zone(self, zone: str) -> None:
        """Laadt alle entiteiten van een zone in de volgorde uit de metadata."""
        for entity in self.repo.dv_entities():
            if entity.dv_zone == zone and entity.dv_entity_type != "PIT":
                self.load(entity.dv_entity_id)
