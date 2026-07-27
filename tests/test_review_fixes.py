"""Regression tests for the 2026-07-27 external review findings not already
covered by test_client.py (seeds, zero-row loads, lazy cursor)."""

from __future__ import annotations

from decimal import Decimal

import agate
import pyarrow as pa
import pytest

from dbt.adapters.hotdata.impl import HotdataAdapter
from dbt.adapters.hotdata.seeds import agate_to_arrow
from tests.test_impl import Harness, _relation


def _number_table(values, name="v") -> agate.Table:
    return agate.Table([[v] for v in values], column_names=[name], column_types=[agate.Number()])


# --- seeds: wide decimals (quantize under default context prec=28 crashed) ---


def test_seed_decimal_with_38_digits_round_trips_exactly():
    value = Decimal("123456789012345678901234567890123456.78")  # precision 38, scale 2
    arrow = agate_to_arrow(_number_table([value, None]))
    assert arrow.schema.field("v").type == pa.decimal128(38, 2)
    assert arrow.column("v").to_pylist() == [value, None]


def test_seed_decimal_past_38_digits_still_falls_back_to_float():
    value = Decimal("1234567890123456789012345678901234567890.1")  # precision 41
    arrow = agate_to_arrow(_number_table([value]))
    assert arrow.schema.field("v").type == pa.float64()


def test_seed_decimal_29_digit_mixed_scale():
    values = [Decimal("12345678901234567890123456789.5"), Decimal("0.125")]
    arrow = agate_to_arrow(_number_table(values))
    assert arrow.schema.field("v").type.scale == 3
    # Compare numerically: quantizing the expected value here would itself
    # need a widened decimal context — the very bug under test.
    assert arrow.column("v").to_pylist() == values


# --- seeds: override validation (ValueError escaped for precision > 38) -----


def test_decimal_override_past_38_warns_and_keeps_inferred_type():
    arrow = agate_to_arrow(_number_table([Decimal("1.5")]), {"v": "decimal(99,2)"})
    assert arrow.schema.field("v").type == pa.decimal128(2, 1)  # inferred, not decimal(99,2)


def test_decimal_override_zero_precision_warns_and_keeps_inferred_type():
    arrow = agate_to_arrow(_number_table([Decimal(3)]), {"v": "decimal(0,0)"})
    assert arrow.schema.field("v").type == pa.int64()


# --- zero-row incremental loads skip the upload/load ------------------------


@pytest.fixture
def harness(fake_client):
    return Harness(fake_client)


def test_zero_row_append_is_a_no_op_load(harness, fake_client):
    fake_client.default_result = pa.table({"id": pa.array([], pa.int64())})
    result = HotdataAdapter.create_table_from_query(harness, _relation(), "select ...", "append")
    assert result == {"message": "LOAD APPEND 0 (no new rows)", "rows": 0}
    assert fake_client.loads == []  # no upload, no catalog write lock taken
    assert ("public", "orders") in fake_client.tables  # but the table is declared


def test_zero_row_upsert_is_a_no_op_load(harness, fake_client):
    fake_client.default_result = pa.table({"id": pa.array([], pa.int64())})
    result = HotdataAdapter.create_table_from_query(
        harness, _relation(), "select ...", "upsert", unique_key="id"
    )
    assert result["rows"] == 0
    assert fake_client.loads == []


def test_zero_row_replace_still_loads(harness, fake_client):
    # An empty replace is how a full refresh truncates: it must NOT be skipped.
    fake_client.default_result = pa.table({"id": pa.array([], pa.int64())})
    HotdataAdapter.create_table_from_query(harness, _relation(), "select ...", "replace")
    assert [load["mode"] for load in fake_client.loads] == ["replace"]


# --- cursor shim: rows materialize lazily ------------------------------------


def test_cursor_shim_does_not_materialize_until_fetched():
    from dbt.adapters.hotdata.connections import _ArrowCursorShim

    cursor = _ArrowCursorShim(pa.table({"a": [1, 2, 3]}))
    assert cursor._materialized is None  # construction is free
    assert [d[0] for d in cursor.description] == ["a"]  # schema needs no rows
    assert cursor._materialized is None
    assert cursor.fetchone() == (1,)
    assert cursor._materialized is not None
