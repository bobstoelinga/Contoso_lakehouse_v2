"""Gold Historisch en Gold Actueel.

Gold Historisch  : SCD2 MERGE vanuit de (business) vault.
Gold Actueel     : publish-by-pointer. De nieuwe versie wordt in het inactieve
                   slot gebouwd; pas als de volledige publication group is
                   gebouwd, worden alle views in één stap omgezet. Faalt er iets,
                   dan blijft de vorige versie actief.
"""

from __future__ import annotations

import uuid
from collections import defaultdict

from pyspark.sql import SparkSession

from contoso_lakehouse.audit import AuditLogger
from contoso_lakehouse.context import RunContext
from contoso_lakehouse.metadata import GoldEntity, MetadataRepository, safe_identifier

_SLOTS = ("v1", "v2")


class GoldLoader:
    def __init__(self, spark: SparkSession, repo: MetadataRepository, ctx: RunContext) -> None:
        self.spark = spark
        self.repo = repo
        self.ctx = ctx
        self.audit = AuditLogger(spark, ctx)

    # -- Gold Historisch ---------------------------------------------------
    def load_historical(self, entity: GoldEntity) -> int:
        keys = [safe_identifier(k) for k in entity.business_key_columns]
        on_clause = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        with self.audit.run("GOLD_HIST", entity.gold_entity_id) as stats:
            sql = f"""
            MERGE INTO {entity.target_table_fqn} t
            USING (
              SELECT *, '{self.ctx.batch_id}' AS _batch_id, current_timestamp() AS _loaded_at
              FROM ({entity.select_sql})
            ) s ON {on_clause}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
            rows = self._execute(sql)
            stats["rows_inserted"] = rows
            return rows

    # -- Gold Actueel ------------------------------------------------------
    def _publication_slots(self, entity: GoldEntity) -> tuple[str, str]:
        staging_table = entity.staging_table or f"{entity.target_table}_v2"
        staging_slot = staging_table.rsplit(".", maxsplit=1)[-1]
        if staging_slot.endswith("_v1"):
            base = staging_slot.removesuffix("_v1")
        elif staging_slot.endswith("_v2"):
            base = staging_slot.removesuffix("_v2")
        else:
            raise ValueError(
                f"Staging table voor {entity.gold_entity_id} moet eindigen op _v1 of _v2."
            )
        return base + "_v1", base + "_v2"

    def _staging_slot_fqn(self, entity: GoldEntity, slot: str) -> str:
        staging_table = entity.staging_table
        if staging_table:
            return staging_table.rsplit(".", maxsplit=1)[0] + f".{safe_identifier(slot)}"
        return f"{entity.target_catalog}.current_internal.{safe_identifier(slot)}"

    def _active_slot(self, entity: GoldEntity) -> str:
        row = self.spark.sql(
            f"""
            SELECT physical_slot FROM {self.ctx.settings.meta_catalog}.audit.v_active_gold_publication
            WHERE gold_entity_id = '{entity.gold_entity_id}'
            """
        ).collect()
        return row[0].physical_slot if row else self._publication_slots(entity)[0]

    def _target_slot(self, entity: GoldEntity) -> str:
        active = self._active_slot(entity)
        slot_v1, slot_v2 = self._publication_slots(entity)
        return slot_v2 if active == slot_v1 else slot_v1

    def build_current(self, entity: GoldEntity) -> tuple[str, int]:
        """Bouwt de nieuwe versie in het inactieve slot. Publiceert nog niet."""
        slot = self._target_slot(entity)
        slot_fqn = self._staging_slot_fqn(entity, slot)
        publication_id = str(uuid.uuid4())

        with self.audit.run("GOLD_CURR", entity.gold_entity_id) as stats:
            self.spark.sql(
                f"""
                CREATE OR REPLACE TABLE {slot_fqn} AS
                SELECT *,
                       '{self.ctx.delivery_id}'   AS _as_of_delivery_id,
                       current_timestamp()        AS _as_of_timestamp,
                       '{self.ctx.batch_id}'      AS _batch_id
                FROM ({entity.select_sql})
                """
            )
            count = self.spark.table(slot_fqn).count()
            stats["rows_inserted"] = count
            self.spark.sql(
                f"""
                INSERT INTO {self.ctx.settings.meta_catalog}.audit.audit_gold_publication VALUES (
                  '{publication_id}', '{entity.gold_entity_id}', '{self.ctx.batch_id}',
                  '{self.ctx.delivery_id}', '{slot}', 'BUILDING', {count},
                  current_timestamp(), NULL, NULL)
                """
            )
        return publication_id, count

    def publish_group(self, publication_ids: dict[str, str], entities: list[GoldEntity]) -> None:
        """Publiceert een publication group via één atomische releasepointer.

        Publieke views lezen hun slot dynamisch uit deze pointer. Daardoor is
        één Delta MERGE voldoende om dimensies en feiten samen te publiceren.
        """
        self._require_building_publications(publication_ids)
        audit = f"{self.ctx.settings.meta_catalog}.audit.audit_gold_publication"
        ids = ", ".join(f"'{p}'" for p in publication_ids.values())
        entity_ids = ", ".join(f"'{e.gold_entity_id}'" for e in entities)
        publication_group_id = entities[0].publication_group_id or entities[0].gold_entity_id
        self.spark.sql(
            f"""
            MERGE INTO {self.ctx.settings.meta_catalog}.audit.audit_gold_publication_group t
            USING (SELECT '{publication_group_id}' AS publication_group_id,
                          '{self.ctx.batch_id}' AS batch_id,
                          '{self.ctx.delivery_id}' AS delivery_id) s
              ON t.publication_group_id = s.publication_group_id
            WHEN MATCHED THEN UPDATE SET
              batch_id = s.batch_id,
              delivery_id = s.delivery_id,
              release_status = 'ACTIVE',
              published_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (
              publication_group_id, batch_id, delivery_id, release_status, published_at)
            VALUES (s.publication_group_id, s.batch_id, s.delivery_id, 'ACTIVE', current_timestamp())
            """
        )
        self.spark.sql(
            f"""
            UPDATE {audit} SET publication_status = 'SUPERSEDED', superseded_at = current_timestamp()
            WHERE publication_status = 'ACTIVE' AND gold_entity_id IN ({entity_ids})
            """
        )
        self.spark.sql(
            f"""
            UPDATE {audit} SET publication_status = 'ACTIVE', published_at = current_timestamp()
            WHERE publication_id IN ({ids}) AND publication_status = 'BUILDING'
            """
        )

    def _require_building_publications(self, publication_ids: dict[str, str]) -> None:
        ids = ", ".join(f"'{publication_id}'" for publication_id in publication_ids.values())
        audit = f"{self.ctx.settings.meta_catalog}.audit.audit_gold_publication"
        row = self.spark.sql(
            f"""
            SELECT count(*) AS building_count
            FROM {audit}
            WHERE publication_id IN ({ids}) AND publication_status = 'BUILDING'
            """
        ).collect()[0]
        if row.building_count != len(publication_ids):
            raise RuntimeError(
                "Publicatiegroep bevat geen complete set BUILDING-publicaties; pointers blijven ongewijzigd."
            )

    def run_current_layer(self) -> None:
        """Bouwt en publiceert alle Gold Actueel entiteiten per publication group."""
        groups: dict[str, list[GoldEntity]] = defaultdict(list)
        for entity in self.repo.gold_entities():
            if entity.gold_layer == "CURRENT":
                groups[entity.publication_group_id or entity.gold_entity_id].append(entity)

        for group, entities in groups.items():
            publication_ids: dict[str, str] = {}
            try:
                for entity in sorted(entities, key=lambda e: e.load_order):
                    pub_id, _ = self.build_current(entity)
                    publication_ids[entity.gold_entity_id] = pub_id
            except Exception:
                self._mark_failed(publication_ids)
                raise  # vorige versie blijft actief
            self.publish_group(publication_ids, entities)

    def _mark_failed(self, publication_ids: dict[str, str]) -> None:
        if not publication_ids:
            return
        ids = ", ".join(f"'{p}'" for p in publication_ids.values())
        self.spark.sql(
            f"""
            UPDATE {self.ctx.settings.meta_catalog}.audit.audit_gold_publication
            SET publication_status = 'FAILED'
            WHERE publication_id IN ({ids}) AND publication_status = 'BUILDING'
            """
        )

    def run_historical_layer(self) -> None:
        for entity in self.repo.gold_entities():
            if entity.gold_layer == "HISTORICAL":
                self.load_historical(entity)

    def _execute(self, sql: str) -> int:
        result = self.spark.sql(sql)
        try:
            row = result.collect()[0].asDict()
            return int(
                (row.get("num_inserted_rows") or 0) + (row.get("num_updated_rows") or 0)
            )
        except (IndexError, AttributeError):
            return 0
