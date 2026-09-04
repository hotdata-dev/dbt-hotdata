from __future__ import annotations

import os

import pyarrow as pa
import pytest

from dbt.adapters.hotdata.credentials import HotdataCredentials


@pytest.fixture(autouse=True)
def _isolate_ambient_env(monkeypatch):
    """Credentials resolve unset fields from HOTDATA_* environment variables,
    so a developer's real environment must never leak into tests."""
    for name in list(os.environ):
        if name.startswith("HOTDATA_"):
            monkeypatch.delenv(name, raising=False)


@pytest.fixture
def credentials() -> HotdataCredentials:
    return HotdataCredentials(
        database="default",
        schema="public",
        workspace_id="ws_test",
        api_key="hk_test",
    )


class FakeDbtClient:
    """In-memory stand-in for HotdataDbtClient (the connection handle).

    Records every call and serves canned Arrow results, so connection-manager
    and adapter logic is exercised without any HTTP.
    """

    def __init__(self, results: dict[str, pa.Table] | None = None) -> None:
        self.results = results or {}
        self.default_result = pa.table({"n": [1]})
        self.tables: dict[tuple[str, str], dict] = {}  # (schema, table) -> {"key": ...}
        self.schemas: set[str] = set()
        self.loads: list[dict] = []
        self.executed: list[str] = []
        self.closed = False

    # --- client surface used by the adapter ---------------------------------

    def execute_sql(self, sql: str) -> pa.Table:
        self.executed.append(sql)
        for fragment, table in self.results.items():
            if fragment in sql:
                return table
        return self.default_result

    def list_tables(self, schema: str):
        from hotdata_framework.databases import ManagedTable

        return [
            ManagedTable(full_name=f"db.{s}.{t}", schema=s, table=t, synced=True, last_sync=None)
            for (s, t) in sorted(self.tables)
            if s == schema
        ]

    def list_schemas(self):
        return sorted({s for (s, _t) in self.tables})

    def has_table(self, table: str, *, schema: str) -> bool:
        return (schema, table) in self.tables

    def ensure_schema(self, schema: str) -> None:
        self.schemas.add(schema)

    def ensure_table(self, table: str, *, schema: str, key=None) -> None:
        self.tables.setdefault((schema, table), {"key": key})

    def load_arrow(self, table: str, *, schema: str, data: pa.Table, mode="replace", key=None):
        self.loads.append(
            {"table": table, "schema": schema, "mode": mode, "key": key, "rows": data.num_rows}
        )
        self.tables.setdefault((schema, table), {"key": key})
        return data.num_rows

    def truncate_table(self, table: str, *, schema: str) -> None:
        self.loads.append({"table": table, "schema": schema, "mode": "truncate"})

    def drop_table(self, table: str, *, schema: str) -> None:
        self.tables.pop((schema, table), None)

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def fake_client() -> FakeDbtClient:
    return FakeDbtClient()
