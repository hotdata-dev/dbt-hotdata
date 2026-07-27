from __future__ import annotations

import pyarrow as pa

from dbt.adapters.hotdata.column import HotdataColumn, dtype_from_arrow


def test_dtype_mapping():
    cases = {
        pa.bool_(): "boolean",
        pa.int16(): "smallint",
        pa.int32(): "integer",
        pa.int64(): "bigint",
        pa.float32(): "real",
        pa.float64(): "double precision",
        pa.decimal128(38, 9): "numeric(38,9)",
        pa.date32(): "date",
        pa.timestamp("ns"): "timestamp",
        pa.timestamp("us", tz="UTC"): "timestamptz",
        pa.string(): "varchar",
        pa.large_string(): "varchar",
        pa.binary(): "bytea",
    }
    for arrow_type, expected in cases.items():
        assert dtype_from_arrow(arrow_type) == expected, str(arrow_type)


def test_from_arrow_field_carries_decimal_precision():
    column = HotdataColumn.from_arrow_field(pa.field("amount", pa.decimal128(20, 4)))
    assert column.name == "amount"
    assert column.dtype == "numeric(20,4)"
    assert column.numeric_precision == 20
    assert column.numeric_scale == 4


def test_nested_types_fall_back_to_arrow_name():
    assert "list" in dtype_from_arrow(pa.list_(pa.int64()))
