"""CBS StatLine OData helpers voor een landing-first ingestiepatroon."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator, Mapping
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_DATASET_ID = re.compile(r"^[0-9]{5}[a-z]{3}$", re.IGNORECASE)
_API_ROOT = "https://opendata.cbs.nl/ODataApi/OData"


def statline_endpoint(dataset_id: str) -> str:
    """Geeft het TypedDataSet-endpoint voor een gevalideerde StatLine-tabel."""
    if not _DATASET_ID.fullmatch(dataset_id):
        raise ValueError("CBS dataset_id moet bestaan uit vijf cijfers en drie letters.")
    return f"{_API_ROOT}/{dataset_id}/TypedDataSet"


def with_odata_page_size(endpoint: str, page_size: int = 5000) -> str:
    """Begrenst een OData-response onder de Databricks response-limiet."""
    if page_size < 1:
        raise ValueError("page_size moet minimaal 1 zijn.")
    parts = urlsplit(endpoint)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["$top"] = str(page_size)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def iter_odata_pages(
    endpoint: str,
    fetch_page: Callable[[str], Mapping[str, Any]],
) -> Iterator[list[dict[str, Any]]]:
    """Levert alle CBS OData-pagina's en bewaakt circulaire next-links."""
    url = endpoint
    seen_urls: set[str] = set()
    while url:
        if url in seen_urls:
            raise RuntimeError(f"CBS OData-paginering bevat een cyclus: {url}")
        seen_urls.add(url)
        payload = fetch_page(url)
        records = payload.get("value")
        if not isinstance(records, list) or not all(isinstance(record, dict) for record in records):
            raise ValueError("CBS OData-respons bevat geen lijst met records in 'value'.")
        yield records
        next_url = payload.get("@odata.nextLink")
        if next_url is not None and not isinstance(next_url, str):
            raise ValueError("CBS OData-respons bevat een ongeldige '@odata.nextLink'.")
        url = next_url