"""Runtime context: environment resolutie en batch-brede constanten.

Alle lagen delen dezelfde ``batch_id`` en ``load_date``. De ``load_date`` wordt
eenmalig per end-to-end run bepaald zodat Data Vault historisatie consistent is.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass(frozen=True)
class Settings:
    """Environment-specifieke resolutie van fysieke namen.

    Logische namen staan in de metadata; hier worden ze naar catalogs vertaald.
    Zo blijft promotie van dev naar prd een configuratiewijziging.
    """

    env: str
    catalog_prefix: str = "contoso"
    raw_catalog_template: str = "raw_{env}"

    @property
    def raw_catalog(self) -> str:
        return self.raw_catalog_template.format(env=self.env)

    @property
    def meta_catalog(self) -> str:
        return f"{self.catalog_prefix}_meta_{self.env}"

    @property
    def bronze_catalog(self) -> str:
        return f"{self.catalog_prefix}_bronze_{self.env}"

    @property
    def quality_catalog(self) -> str:
        return f"{self.catalog_prefix}_quality_{self.env}"

    @property
    def reject_catalog(self) -> str:
        return f"{self.catalog_prefix}_reject_{self.env}"

    @property
    def vault_catalog(self) -> str:
        return f"{self.catalog_prefix}_vault_{self.env}"

    @property
    def gold_catalog(self) -> str:
        return f"{self.catalog_prefix}_gold_{self.env}"

    def resolve(self, value: str) -> str:
        """Vervangt placeholders in metadata-waarden door fysieke namen."""
        if value is None:
            return value
        return (
            value.replace("${env}", self.env)
            .replace("{raw_catalog}", self.raw_catalog)
            .replace("{meta_catalog}", self.meta_catalog)
            .replace("{bronze_catalog}", self.bronze_catalog)
            .replace("{quality_catalog}", self.quality_catalog)
            .replace("{reject_catalog}", self.reject_catalog)
            .replace("{vault_catalog}", self.vault_catalog)
            .replace("{gold_catalog}", self.gold_catalog)
        )


@dataclass(frozen=True)
class RunContext:
    """Onveranderlijke context van één end-to-end run."""

    settings: Settings
    batch_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    load_date: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    delivery_id: str | None = None
    job_run_id: str | None = None

    @property
    def load_date_literal(self) -> str:
        return f"timestamp'{self.load_date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"

    @classmethod
    def create(
        cls,
        settings: Settings,
        batch_id: str | None = None,
        delivery_id: str | None = None,
        job_run_id: str | None = None,
    ) -> "RunContext":
        """Bouwt een context; lege strings uit widgets tellen als 'niet opgegeven'."""
        return cls(
            settings=settings,
            batch_id=batch_id or str(uuid.uuid4()),
            delivery_id=delivery_id or None,
            job_run_id=job_run_id or None,
        )

    def with_delivery(self, delivery_id: str) -> "RunContext":
        return RunContext(
            settings=self.settings,
            batch_id=self.batch_id,
            load_date=self.load_date,
            delivery_id=delivery_id,
            job_run_id=self.job_run_id,
        )
