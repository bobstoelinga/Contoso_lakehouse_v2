"""Orchestratie: gates en afhankelijkheidsresolutie.

Afhankelijkheden staan uitsluitend in ``meta_dependency``. Deze module leidt
daar een uitvoervolgorde uit af en bewaakt de gates. Er is geen enkele
hardcoded volgorde in de notebooks of de Workflow-definitie.
"""

from __future__ import annotations

from collections import defaultdict, deque

from pyspark.sql import SparkSession

from contoso_lakehouse.context import RunContext
from contoso_lakehouse.metadata import MetadataRepository


class DependencyCycleError(RuntimeError):
    """De afhankelijkheidsgraaf bevat een cyclus."""


class GateNotOpenError(RuntimeError):
    """Een blokkerende voorwaarde is nog niet vervuld."""


def parallel_execution_waves(
    dependencies: dict[str, set[str]], max_parallelism: int, priorities: dict[str, int] | None = None,
) -> list[list[str]]:
    """Deelt een DAG op in begrensde, veilig parallel uit te voeren waves."""
    if max_parallelism < 1:
        raise ValueError("max_parallelism moet minimaal 1 zijn.")
    priorities = priorities or {}
    remaining = {node: set(parents) for node, parents in dependencies.items()}
    waves: list[list[str]] = []
    completed: set[str] = set()
    while remaining:
        ready = sorted(
            (node for node, parents in remaining.items() if parents <= completed),
            key=lambda node: (priorities.get(node, 100), node),
        )
        if not ready:
            raise DependencyCycleError(
                f"Cyclus in parallelle uitvoerplanning: {sorted(remaining)}"
            )
        for start in range(0, len(ready), max_parallelism):
            wave = ready[start:start + max_parallelism]
            waves.append(wave)
            completed.update(wave)
            for node in wave:
                del remaining[node]
    return waves


class Orchestrator:
    def __init__(self, spark: SparkSession, repo: MetadataRepository, ctx: RunContext) -> None:
        self.spark = spark
        self.repo = repo
        self.ctx = ctx
        self.audit_schema = f"{ctx.settings.meta_catalog}.audit"

    # -- gates ------------------------------------------------------------
    def next_delivery(self, source_system_id: str) -> str | None:
        """De eerstvolgende complete, nog niet verwerkte levering.

        Retourneert None als er niets te doen is of als de oudste openstaande
        levering nog niet compleet is: chronologie gaat boven doorstroming.
        """
        rows = self.spark.sql(
            f"""
            SELECT delivery_id, is_ready FROM {self.audit_schema}.v_next_processable_delivery
            WHERE source_system_id = '{source_system_id}'
            """
        ).collect()
        if not rows:
            return None
        return rows[0].delivery_id if rows[0].is_ready else None

    def require_delivery_complete(self, delivery_id: str) -> None:
        row = self.spark.sql(
            f"""
            SELECT is_ready, success_count, expected_object_count, failed_count
            FROM {self.audit_schema}.v_delivery_readiness
            WHERE delivery_id = '{delivery_id}'
            """
        ).collect()
        if not row:
            raise GateNotOpenError(f"Levering {delivery_id} is niet geregistreerd.")
        r = row[0]
        if not r.is_ready:
            raise GateNotOpenError(
                f"Levering {delivery_id} is niet compleet: "
                f"{r.success_count}/{r.expected_object_count} geladen, {r.failed_count} gefaald."
            )

    def require_upstream_success(self, entity_id: str, layer: str) -> None:
        """Controleert alle blokkerende afhankelijkheden uit de metadata."""
        for dep in self.repo.dependencies_for(entity_id, layer):
            if dep.dependency_type == "DELIVERY_COMPLETE":
                self.require_delivery_complete(self.ctx.delivery_id)
                continue
            ok = self.spark.sql(
                f"""
                SELECT count(*) AS n FROM {self.audit_schema}.v_load_run_status
                WHERE batch_id = '{self.ctx.batch_id}'
                  AND entity_id = '{dep.depends_on_entity_id}'
                  AND layer     = '{dep.depends_on_layer}'
                  AND run_status = 'SUCCESS'
                """
            ).collect()[0].n
            if not ok:
                raise GateNotOpenError(
                    f"{layer}.{entity_id} wacht op {dep.depends_on_layer}.{dep.depends_on_entity_id}"
                )

    # -- volgorde ---------------------------------------------------------
    def execution_order(self, layer: str) -> list[str]:
        """Topologische sortering van alle entiteiten binnen één laag."""
        edges: dict[str, set[str]] = defaultdict(set)
        nodes: set[str] = set()
        for dep in self.repo.dependencies():
            if dep.entity_layer != layer:
                continue
            nodes.add(dep.entity_id)
            if dep.depends_on_layer == layer:
                edges[dep.entity_id].add(dep.depends_on_entity_id)
                nodes.add(dep.depends_on_entity_id)

        indegree = {n: len(edges[n]) for n in nodes}
        queue = deque(sorted(n for n, d in indegree.items() if d == 0))
        order: list[str] = []
        while queue:
            node = queue.popleft()
            order.append(node)
            for other in sorted(nodes):
                if node in edges[other]:
                    edges[other].discard(node)
                    indegree[other] -= 1
                    if indegree[other] == 0:
                        queue.append(other)
        if len(order) != len(nodes):
            raise DependencyCycleError(
                f"Cyclus in de afhankelijkheden van laag {layer}: {sorted(nodes - set(order))}"
            )
        return order

    def execution_waves(
        self, layer: str, entity_ids: set[str], max_parallelism: int,
    ) -> list[list[str]]:
        """Plant alleen intra-layer afhankelijkheden; externe gates blijven runtimechecks."""
        dependencies = {entity_id: set() for entity_id in entity_ids}
        priorities: dict[str, int] = {}
        for dep in self.repo.dependencies():
            if dep.entity_layer != layer or dep.entity_id not in entity_ids:
                continue
            priorities[dep.entity_id] = min(
                priorities.get(dep.entity_id, 100), getattr(dep, "priority", 100)
            )
            if dep.depends_on_layer == layer and dep.depends_on_entity_id in entity_ids:
                dependencies[dep.entity_id].add(dep.depends_on_entity_id)
        return parallel_execution_waves(dependencies, max_parallelism, priorities)

    def validate_graph(self) -> None:
        """Controleert de volledige graaf op cycli en wees-verwijzingen."""
        known = (
            {o.source_object_id for o in self.repo.source_objects()}
            | {e.dv_entity_id for e in self.repo.dv_entities()}
            | {e.gold_entity_id for e in self.repo.gold_entities()}
            | {o.source_system_id for o in self.repo.source_objects()}
        )
        orphans = {
            d.depends_on_entity_id for d in self.repo.dependencies()
            if d.depends_on_entity_id not in known
        } | {
            d.entity_id for d in self.repo.dependencies() if d.entity_id not in known
        }
        if orphans:
            raise ValueError(f"meta_dependency verwijst naar onbekende entiteiten: {sorted(orphans)}")
        for layer in ("BRONZE", "QUALITY", "RAW_VAULT", "BUSINESS_VAULT", "GOLD_HIST", "GOLD_CURR"):
            self.execution_order(layer)
