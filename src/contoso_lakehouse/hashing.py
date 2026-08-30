"""Data Vault 2.0 hashing.

De conventie is bewust centraal en onveranderlijk vastgelegd: als deze wijzigt,
zijn alle bestaande hash keys ongeldig en is een volledige rebuild nodig.

Conventie (versie 1):
  * algoritme      : SHA-256 (geen MD5 -- collisierisico bij miljarden rijen)
  * normalisatie   : upper(trim(cast(x as string)))
  * NULL / lege str: '^^'
  * separator      : '||'
  * volgorde       : exact de volgorde waarin de kolommen worden meegegeven
"""

from __future__ import annotations

from typing import Sequence

HASH_ALGORITHM = "sha2"
HASH_BITS = 256
NULL_TOKEN = "^^"
SEPARATOR = "||"
HASH_CONVENTION_VERSION = 1


def _normalise(expr: str) -> str:
    return f"coalesce(nullif(upper(trim(cast({expr} as string))), ''), '{NULL_TOKEN}')"


def hash_key(columns: Sequence[str], collision_code: str | None = None) -> str:
    """Bouwt de SQL-expressie voor een hub- of link hash key."""
    if not columns:
        raise ValueError("hash_key vereist minstens één kolom")
    parts = list(columns)
    if collision_code is not None:
        parts = [f"'{collision_code}'", *parts]
    concat = f", '{SEPARATOR}', ".join(_normalise(c) for c in parts)
    return f"{HASH_ALGORITHM}(concat({concat}), {HASH_BITS})"


def hashdiff(columns: Sequence[str]) -> str:
    """Bouwt de SQL-expressie voor een satellite hashdiff.

    De kolomvolgorde volgt uit ``meta_dv_mapping.ordinal_position``; wijzigen van
    die volgorde verandert de hashdiff en veroorzaakt onterechte nieuwe versies.
    """
    if not columns:
        raise ValueError("hashdiff vereist minstens één kolom")
    concat = f", '{SEPARATOR}', ".join(_normalise(c) for c in columns)
    return f"{HASH_ALGORITHM}(concat({concat}), {HASH_BITS})"
