"""Herbruikbare publieke HTTP-connectoren voor immutable landingextracten."""

from __future__ import annotations

import json
import csv
import time
from io import StringIO
from collections.abc import Callable, Iterator, Mapping
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _fetch(
    url: str,
    accept: str,
    timeout_seconds: int,
    max_retries: int,
    retry_delay_seconds: float,
    total_timeout_seconds: float | None,
) -> bytes:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds moet groter zijn dan nul.")
    if max_retries < 0:
        raise ValueError("max_retries mag niet negatief zijn.")
    if retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds mag niet negatief zijn.")
    if total_timeout_seconds is not None and total_timeout_seconds <= 0:
        raise ValueError("total_timeout_seconds moet groter zijn dan nul.")

    request = Request(url, headers={"Accept": accept})
    deadline = (
        time.monotonic() + total_timeout_seconds
        if total_timeout_seconds is not None
        else None
    )
    for attempt in range(max_retries + 1):
        remaining_seconds = (
            deadline - time.monotonic() if deadline is not None else timeout_seconds
        )
        if remaining_seconds <= 0:
            raise TimeoutError("Totale HTTP-extracttimeout is bereikt.")
        try:
            # De contextmanager sluit de socket altijd, ook als read() faalt.
            with urlopen(request, timeout=min(timeout_seconds, remaining_seconds)) as response:  # noqa: S310 - endpoint komt uit Git-metadata
                return response.read()
        except HTTPError as exc:
            retryable = exc.code in {408, 429} or 500 <= exc.code < 600
            if not retryable or attempt == max_retries:
                raise
        except (TimeoutError, URLError):
            if attempt == max_retries:
                raise
        retry_delay = retry_delay_seconds * (2**attempt)
        if deadline is not None:
            retry_delay = min(retry_delay, max(0.0, deadline - time.monotonic()))
        time.sleep(retry_delay)

    raise RuntimeError("HTTP-verzoek eindigde zonder respons of fout.")


def fetch_json(
    url: str,
    timeout_seconds: int = 60,
    max_retries: int = 2,
    retry_delay_seconds: float = 1.0,
    total_timeout_seconds: float | None = None,
) -> Mapping[str, Any] | list[dict[str, Any]]:
    """Haalt een publieke JSON-respons op zonder credentials in code op te nemen."""
    payload = json.loads(
        _fetch(
            url,
            "application/json",
            timeout_seconds,
            max_retries,
            retry_delay_seconds,
            total_timeout_seconds,
        )
    )
    if not isinstance(payload, Mapping) and not (
        isinstance(payload, list) and all(isinstance(row, dict) for row in payload)
    ):
        raise ValueError("API-respons moet een JSON-object of recordlijst zijn.")
    return payload


def fetch_text(
    url: str,
    timeout_seconds: int = 60,
    max_retries: int = 2,
    retry_delay_seconds: float = 1.0,
    total_timeout_seconds: float | None = None,
) -> str:
    """Haalt een publieke tekst- of CSV-respons op."""
    return _fetch(
        url,
        "text/csv, text/plain",
        timeout_seconds,
        max_retries,
        retry_delay_seconds,
        total_timeout_seconds,
    ).decode("utf-8-sig")


def csv_rows(content: str) -> list[dict[str, str]]:
    """Parseert een CSV-respons met een verplichte headerregel."""
    rows = list(csv.DictReader(StringIO(content)))
    if not rows:
        return []
    if not rows[0]:
        raise ValueError("CSV-respons bevat geen headerregel.")
    return [dict(row) for row in rows]


def without_null_only_fields(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Verwijdert velden die in een volledige API-pagina alleen null bevatten.

    Spark Connect kan voor zulke velden geen type afleiden. Een latere pagina met
    een werkelijke waarde wordt via Bronze schema evolution toegevoegd.
    """
    populated_fields = {
        field
        for record in records
        for field, value in record.items()
        if value is not None
    }
    return [
        {field: value for field, value in record.items() if field in populated_fields}
        for record in records
    ]


def iter_json_pages(
    endpoint: str,
    fetch: Callable[[str], Mapping[str, Any] | list[dict[str, Any]]] = fetch_json,
    records_key: str = "value",
    next_link_key: str = "@odata.nextLink",
) -> Iterator[list[dict[str, Any]]]:
    """Levert elke JSON-pagina precies eenmaal en weigert circulaire pagina-links."""
    url = endpoint
    seen: set[str] = set()
    while url:
        if url in seen:
            raise RuntimeError(f"API-paginering bevat een cyclus: {url}")
        seen.add(url)
        payload = fetch(url)
        records = payload if isinstance(payload, list) and not records_key else payload.get(records_key)
        if not isinstance(records, list) or not all(isinstance(row, dict) for row in records):
            raise ValueError(f"API-respons bevat geen recordlijst in '{records_key}'.")
        yield records
        next_url = payload.get(next_link_key) if isinstance(payload, Mapping) else None
        if next_url is not None and not isinstance(next_url, str):
            raise ValueError(f"API-respons bevat een ongeldige '{next_link_key}'.")
        url = next_url