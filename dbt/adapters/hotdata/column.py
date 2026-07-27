from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
from dbt.adapters.base.column import Column


def dtype_from_arrow(arrow_type: pa.DataType) -> str:
    """Render an Arrow type as the Postgres-surface name DataFusion presents.

    Used when describing relations: the engine returns Arrow schemas, and dbt
    (docs, `{{ col.data_type }}`, schema tests) expects SQL type names.
    """
    if pa.types.is_boolean(arrow_type):
        return "boolean"
    if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type):
        return "smallint"
    if pa.types.is_int32(arrow_type):
        return "integer"
    if pa.types.is_int64(arrow_type) or pa.types.is_unsigned_integer(arrow_type):
        return "bigint"
    if pa.types.is_float32(arrow_type):
        return "real"
    if pa.types.is_float64(arrow_type):
        return "double precision"
    if pa.types.is_decimal(arrow_type):
        return f"numeric({arrow_type.precision},{arrow_type.scale})"
    if pa.types.is_date(arrow_type):
        return "date"
    if pa.types.is_timestamp(arrow_type):
        return "timestamptz" if arrow_type.tz else "timestamp"
    if pa.types.is_time(arrow_type):
        return "time"
    if pa.types.is_duration(arrow_type):
        return "interval"
    if (
        pa.types.is_binary(arrow_type)
        or pa.types.is_large_binary(arrow_type)
        or pa.types.is_binary_view(arrow_type)
    ):
        return "bytea"
    if (
        pa.types.is_string(arrow_type)
        or pa.types.is_large_string(arrow_type)
        # DataFusion reads loaded string columns back as Utf8View; the SQL
        # name must still be a castable one, never "string_view".
        or pa.types.is_string_view(arrow_type)
    ):
        return "varchar"
    # Nested/list/struct and anything else: fall back to the Arrow name so the
    # information is preserved rather than mislabeled.
    return str(arrow_type)


@dataclass
class HotdataColumn(Column):
    @classmethod
    def string_type(cls, size: int) -> str:
        # The base class renders "character varying(256)", which DataFusion
        # rejects in casts; strings are unbounded here.
        return "varchar"

    @classmethod
    def from_arrow_field(cls, field: pa.Field) -> HotdataColumn:
        arrow_type = field.type
        numeric_precision = arrow_type.precision if pa.types.is_decimal(arrow_type) else None
        numeric_scale = arrow_type.scale if pa.types.is_decimal(arrow_type) else None
        return cls(
            column=field.name,
            dtype=dtype_from_arrow(arrow_type),
            numeric_precision=numeric_precision,
            numeric_scale=numeric_scale,
        )
