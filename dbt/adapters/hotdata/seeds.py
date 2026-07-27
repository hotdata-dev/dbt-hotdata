"""Seed (CSV) conversion: agate -> Arrow, ready for a parquet managed load.

dbt parses seed CSVs into agate tables. Warehouses ingest seeds with INSERT
statements; Hotdata has no INSERT surface, so seeds take the same path as
models — an Arrow table written to parquet and applied server-side. Numbers
stay exact: agate parses them as Decimal, and integral / fractional columns
land as int64 / decimal128 (never silently as floats unless the values do
not fit a decimal at all).
"""

from __future__ import annotations

import re
from decimal import Decimal, localcontext
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.exceptions import DbtRuntimeError

if TYPE_CHECKING:
    import agate

logger = AdapterLogger("Hotdata")

_MAX_DECIMAL_PRECISION = 38

_DECIMAL_RE = re.compile(r"^(?:decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$")

# `column_types:` overrides, profile-side names -> Arrow types.
_OVERRIDE_TYPES: dict[str, pa.DataType] = {
    "smallint": pa.int16(),
    "int2": pa.int16(),
    "integer": pa.int32(),
    "int": pa.int32(),
    "int4": pa.int32(),
    "bigint": pa.int64(),
    "int8": pa.int64(),
    "real": pa.float32(),
    "float4": pa.float32(),
    "float": pa.float64(),
    "float8": pa.float64(),
    "double": pa.float64(),
    "double precision": pa.float64(),
    "boolean": pa.bool_(),
    "bool": pa.bool_(),
    "date": pa.date32(),
    "timestamp": pa.timestamp("us"),
    "datetime": pa.timestamp("us"),
    "timestamptz": pa.timestamp("us", tz="UTC"),
    "varchar": pa.string(),
    "text": pa.string(),
    "string": pa.string(),
}


def _override_type(type_str: str) -> pa.DataType | None:
    normalized = type_str.strip().lower()
    normalized = re.sub(
        r"^(varchar|char|character varying)\s*\(\s*\d+\s*\)$", "varchar", normalized
    )
    match = _DECIMAL_RE.match(normalized)
    if match:
        precision = int(match.group(1))
        if not 1 <= precision <= _MAX_DECIMAL_PRECISION:
            # pa.decimal128 raises a bare ValueError past 38; treat it like any
            # other unsupported override (warn + inferred type) instead.
            return None
        return pa.decimal128(precision, int(match.group(2)))
    return _OVERRIDE_TYPES.get(normalized)


def _number_array(values: list[Any]) -> pa.Array:
    """Decimal column -> int64 when integral, decimal128 otherwise.

    Falls back to float64 only when the values cannot fit decimal128 at all
    (precision past 38 digits), so money-style columns stay exact.
    """
    decimals = [Decimal(v) if v is not None and not isinstance(v, Decimal) else v for v in values]
    nonnull = [v for v in decimals if v is not None]
    if not nonnull:
        return pa.array(decimals, pa.float64())
    if any(not v.is_finite() for v in nonnull):
        # NaN/Infinity cannot be decimals; floats are the only honest home.
        return pa.array([float(v) if v is not None else None for v in decimals], pa.float64())
    if all(v == v.to_integral_value() for v in nonnull):
        try:
            return pa.array([int(v) if v is not None else None for v in decimals], pa.int64())
        except OverflowError:
            pass  # wider than int64: fall through to decimal
    # exponents are ints here — the finiteness guard above excluded NaN/Inf.
    tuples = [v.as_tuple() for v in nonnull]
    exponents = [int(t.exponent) for t in tuples]
    scale = max(max(-exponent, 0) for exponent in exponents)
    # digits + exponent = count of integer digits.
    int_digits = max(
        len(t.digits) + exponent for t, exponent in zip(tuples, exponents, strict=True)
    )
    precision = max(max(int_digits, 0) + scale, 1)
    if precision > _MAX_DECIMAL_PRECISION:
        return pa.array([float(v) if v is not None else None for v in decimals], pa.float64())
    quantum = Decimal(1).scaleb(-scale)
    # Python's default decimal context caps precision at 28, so quantizing a
    # 29-38 digit value — squarely inside what decimal128 holds — would raise
    # InvalidOperation. Quantize under a context wide enough for anything that
    # passed the precision guard above.
    with localcontext() as ctx:
        ctx.prec = _MAX_DECIMAL_PRECISION
        quantized = [v.quantize(quantum) if v is not None else None for v in decimals]
    return pa.array(quantized, pa.decimal128(precision, scale))


def _default_array(values: list[Any], agate_type: agate.data_types.DataType) -> pa.Array:
    import agate as agate_module

    if isinstance(agate_type, agate_module.data_types.Boolean):
        return pa.array(values, pa.bool_())
    if isinstance(agate_type, agate_module.data_types.Number):
        return _number_array(values)
    if isinstance(agate_type, agate_module.data_types.Date):
        return pa.array(values, pa.date32())
    if isinstance(agate_type, agate_module.data_types.DateTime):
        return pa.array(values, pa.timestamp("us"))
    # Text, TimeDelta, and anything else: strings.
    return pa.array([str(v) if v is not None else None for v in values], pa.string())


def agate_to_arrow(
    agate_table: agate.Table, column_types: dict[str, str] | None = None
) -> pa.Table:
    """Convert a dbt seed's agate table to Arrow, honoring `column_types:`."""
    column_types = column_types or {}
    names: list[str] = list(agate_table.column_names)
    arrays: list[pa.Array] = []
    for index, name in enumerate(names):
        values = [row[index] for row in agate_table.rows]
        agate_type = agate_table.column_types[index]
        override = column_types.get(name)
        target = _override_type(override) if override else None
        if override and target is None:
            logger.warning(
                f"seed column {name!r}: unsupported column_types override {override!r}; "
                "using the inferred type instead"
            )
        array = _default_array(values, agate_type)
        if target is not None and array.type != target:
            try:
                array = array.cast(target)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
                raise DbtRuntimeError(
                    f"seed column {name!r} cannot be cast to {override!r}: {exc}"
                ) from exc
        arrays.append(array)
    return pa.table(arrays, names=names)
