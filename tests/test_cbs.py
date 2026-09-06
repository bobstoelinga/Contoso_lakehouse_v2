"""Tests voor de CBS StatLine connectorhelpers zonder netwerktoegang."""

from __future__ import annotations

import pytest

from contoso_lakehouse.cbs import iter_odata_pages, statline_endpoint, with_odata_page_size


def test_statline_endpoint_accepts_a_cbs_dataset_code():
    assert statline_endpoint("03759ned") == (
        "https://opendata.cbs.nl/ODataApi/OData/03759ned/TypedDataSet"
    )


@pytest.mark.parametrize("dataset_id", ["03759", "CBS/03759ned", "03759ned?x=1"])
def test_statline_endpoint_rejects_unsafe_or_incomplete_dataset_codes(dataset_id):
    with pytest.raises(ValueError, match="dataset_id"):
        statline_endpoint(dataset_id)


def test_odata_endpoint_adds_a_bounded_page_size():
    assert with_odata_page_size("https://example.test/data?format=json") == (
        "https://example.test/data?format=json&%24top=5000"
    )


def test_odata_pagination_returns_each_page_once():
    pages = {
        "first": {"value": [{"RegioS": "NL01"}], "@odata.nextLink": "second"},
        "second": {"value": [{"RegioS": "NL02"}]},
    }

    assert list(iter_odata_pages("first", pages.__getitem__)) == [
        [{"RegioS": "NL01"}],
        [{"RegioS": "NL02"}],
    ]


def test_odata_pagination_rejects_a_cyclic_next_link():
    pages = {"first": {"value": [], "@odata.nextLink": "first"}}

    with pytest.raises(RuntimeError, match="cyclus"):
        list(iter_odata_pages("first", pages.__getitem__))