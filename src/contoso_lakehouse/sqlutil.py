"""SQL-hulpfuncties zonder Spark-afhankelijkheid."""

from __future__ import annotations

_IDENT_OK = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.")


def safe_identifier(value: str) -> str:
    """Valideert een identifier die in gegenereerde SQL wordt opgenomen.

    Metadata is een privileged input: alleen via Git/DAB te wijzigen. Deze check
    is de tweede verdedigingslinie tegen SQL-injectie via de metadatatabellen.
    """
    if not value or not set(value) <= _IDENT_OK:
        raise ValueError(f"Ongeldige identifier in metadata: {value!r}")
    return value


def sql_string(value: str | None) -> str:
    """Escapet een stringliteral voor gebruik in gegenereerde SQL."""
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"
