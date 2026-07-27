from __future__ import annotations

import datetime
from decimal import Decimal

import agate
import pyarrow as pa
import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.hotdata.seeds import agate_to_arrow


def _table(names, types, rows) -> agate.Table:
    return agate.Table(rows, column_names=names, column_types=types)


def test_integral_numbers_become_int64():
    table = _table(["id"], [agate.Number()], [[Decimal(1)], [Decimal(2)], [None]])
    arrow = agate_to_arrow(table)
    assert arrow.schema.field("id").type == pa.int64()
    assert arrow.column("id").to_pylist() == [1, 2, None]


def test_money_decimals_stay_exact():
    table = _table(["price"], [agate.Number()], [[Decimal("99.00")], [Decimal("49.50")], [None]])
    arrow = agate_to_arrow(table)
    assert pa.types.is_decimal(arrow.schema.field("price").type)
    assert arrow.schema.field("price").type.scale == 2
    assert arrow.column("price").to_pylist() == [Decimal("99.00"), Decimal("49.50"), None]


def test_mixed_scale_decimals_use_widest_scale():
    table = _table(["v"], [agate.Number()], [[Decimal("1.5")], [Decimal("2.125")]])
    arrow = agate_to_arrow(table)
    assert arrow.schema.field("v").type.scale == 3
    assert arrow.column("v").to_pylist() == [Decimal("1.500"), Decimal("2.125")]


def test_boolean_date_datetime_text():
    table = _table(
        ["flag", "day", "at", "note"],
        [agate.Boolean(), agate.Date(), agate.DateTime(), agate.Text()],
        [
            [
                True,
                datetime.date(2026, 7, 27),
                datetime.datetime(2026, 7, 27, 12, 0),  # noqa: DTZ001 — naive on purpose
                "hi",
            ]
        ],
    )
    arrow = agate_to_arrow(table)
    assert arrow.schema.field("flag").type == pa.bool_()
    assert arrow.schema.field("day").type == pa.date32()
    assert arrow.schema.field("at").type == pa.timestamp("us")
    assert arrow.schema.field("note").type == pa.string()


def test_all_null_number_column_falls_back_to_float():
    table = _table(["v"], [agate.Number()], [[None], [None]])
    arrow = agate_to_arrow(table)
    assert arrow.schema.field("v").type == pa.float64()


def test_column_types_override_casts():
    table = _table(["id", "code"], [agate.Number(), agate.Number()], [[Decimal(1), Decimal(7)]])
    arrow = agate_to_arrow(table, {"id": "integer", "code": "varchar"})
    assert arrow.schema.field("id").type == pa.int32()
    assert arrow.schema.field("code").type == pa.string()
    assert arrow.column("code").to_pylist() == ["7"]


def test_column_types_decimal_override():
    table = _table(["amount"], [agate.Number()], [[Decimal("5")]])
    arrow = agate_to_arrow(table, {"amount": "decimal(38,9)"})
    assert arrow.schema.field("amount").type == pa.decimal128(38, 9)


def test_unsupported_override_warns_and_keeps_inferred(caplog):
    table = _table(["v"], [agate.Number()], [[Decimal(1)]])
    arrow = agate_to_arrow(table, {"v": "hyperloglog"})
    assert arrow.schema.field("v").type == pa.int64()


def test_impossible_cast_raises_actionable_error():
    table = _table(["word"], [agate.Text()], [["abc"]])
    with pytest.raises(DbtRuntimeError) as excinfo:
        agate_to_arrow(table, {"word": "integer"})
    assert "word" in str(excinfo.value)


def test_empty_seed_keeps_columns():
    table = _table(["id", "name"], [agate.Number(), agate.Text()], [])
    arrow = agate_to_arrow(table)
    assert arrow.num_rows == 0
    assert arrow.column_names == ["id", "name"]
