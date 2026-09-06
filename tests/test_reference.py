"""Tests voor de generieke reference-data SQL-hulp."""

from __future__ import annotations

import pytest

from contoso_lakehouse.reference import reference_hash_expression


def test_reference_hash_expression_is_null_safe_and_stable():
    assert reference_hash_expression(["name", "province_code"]) == (
        "sha2(concat_ws('||', coalesce(cast(name AS string), '^^'), "
        "coalesce(cast(province_code AS string), '^^')), 256)"
    )


def test_reference_hash_expression_requires_change_tracking_columns():
    with pytest.raises(ValueError, match="change-trackingkolom"):
        reference_hash_expression([])