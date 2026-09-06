"""Tests voor publieke HTTP-connectorhulpen zonder netwerktoegang."""

from __future__ import annotations

from io import BytesIO
from urllib.error import URLError

import pytest

from contoso_lakehouse import connector
from contoso_lakehouse.connector import csv_rows, fetch_json, iter_json_pages, without_null_only_fields


class _Response(BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def test_iter_json_pages_follows_odata_next_links_once():
    pages = {"a": {"value": [{"id": 1}], "@odata.nextLink": "b"}, "b": {"value": [{"id": 2}]}}
    assert list(iter_json_pages("a", pages.__getitem__)) == [[{"id": 1}], [{"id": 2}]]


def test_iter_json_pages_rejects_cyclic_links():
    with pytest.raises(RuntimeError, match="cyclus"):
        list(iter_json_pages("a", lambda _: {"value": [], "@odata.nextLink": "a"}))


def test_csv_rows_parses_a_header_and_records():
    assert csv_rows("CURRENCY,TIME_PERIOD,OBS_VALUE\nUSD,2026-09-01,1.08\n") == [
        {"CURRENCY": "USD", "TIME_PERIOD": "2026-09-01", "OBS_VALUE": "1.08"}
    ]


def test_iter_json_pages_accepts_a_top_level_record_list():
    assert list(iter_json_pages("holidays", lambda _: [{"date": "2026-01-01"}], records_key="")) == [
        [{"date": "2026-01-01"}]
    ]


def test_without_null_only_fields_preserves_populated_values():
    records = [
        {"id": 1, "present": None, "always_null": None},
        {"id": 2, "present": "value", "always_null": None},
    ]

    assert without_null_only_fields(records) == [
        {"id": 1, "present": None},
        {"id": 2, "present": "value"},
    ]


def test_fetch_json_retries_a_temporary_connection_failure(monkeypatch):
    calls = 0

    def open_after_one_failure(request, timeout):
        nonlocal calls
        calls += 1
        assert timeout == 5
        if calls == 1:
            raise URLError("temporary outage")
        return _Response(b'{"value": [{"id": 1}]}')

    monkeypatch.setattr(connector, "urlopen", open_after_one_failure)
    monkeypatch.setattr(connector.time, "sleep", lambda _: None)

    assert fetch_json("https://example.test/data", timeout_seconds=5, max_retries=1) == {
        "value": [{"id": 1}]
    }
    assert calls == 2


def test_fetch_json_stops_when_the_total_timeout_expires(monkeypatch):
    calls = 0
    ticks = iter([0.0, 0.0, 2.0, 2.0])

    def always_unavailable(request, timeout):
        nonlocal calls
        calls += 1
        raise URLError("temporary outage")

    monkeypatch.setattr(connector, "urlopen", always_unavailable)
    monkeypatch.setattr(connector.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(connector.time, "sleep", lambda _: None)

    with pytest.raises(TimeoutError, match="Totale HTTP-extracttimeout"):
        fetch_json(
            "https://example.test/data",
            timeout_seconds=5,
            max_retries=3,
            total_timeout_seconds=1,
        )

    assert calls == 1