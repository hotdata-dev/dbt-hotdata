"""Id-first database resolution in HotdataDbtClient (no network: the
resolve/create seams are overridden)."""

from __future__ import annotations

import dataclasses
from typing import Any, ClassVar

import pytest
from hotdata_framework.databases import ManagedDatabase
from hotdata_framework.errors import HotdataTerminalError

from dbt.adapters.hotdata.client import HotdataDbtClient, _quote_ident
from dbt.adapters.hotdata.credentials import HotdataCredentials

DB = ManagedDatabase(id="db_abc123", description="dbt", default_connection_id="conn_1")


def _wrapped(status: int, message: str = "") -> RuntimeError:
    """Framework-style error: RuntimeError raised ``from`` a real ApiException,
    so retry classification (409/5xx transient, 404 terminal) behaves as live."""
    from hotdata.rest import ApiException

    error = RuntimeError(message or f"{status}: error")
    error.__cause__ = ApiException(status=status, reason=message, body=message)
    return error


class StubbedClient(HotdataDbtClient):
    existing_ids: ClassVar[set[str]] = {"db_abc123"}
    created: ClassVar[list[str]] = []

    def _get_database_by_id(self, database_id: str) -> ManagedDatabase:
        if database_id in self.existing_ids:
            return dataclasses.replace(DB, id=database_id)
        raise KeyError(database_id)

    def _create_database(self) -> ManagedDatabase:
        type(self).created.append(self._database_label)
        return DB


@pytest.fixture(autouse=True)
def _reset_created():
    StubbedClient.created = []


def _creds(**overrides) -> HotdataCredentials:
    base: dict[str, Any] = {
        "database": "default",
        "schema": "public",
        "workspace_id": "ws_test",
        "api_key": "hk_test",
    }
    base.update(overrides)
    return HotdataCredentials(**base)


def _client(credentials: HotdataCredentials | None = None, **overrides) -> StubbedClient:
    return StubbedClient(credentials or _creds(**overrides))


def test_pinned_id_binds_without_creating():
    client = _client(database_id="db_abc123")
    db = client.database()
    assert db is not None and db.id == "db_abc123"
    assert StubbedClient.created == []


def test_missing_pinned_id_is_terminal_with_guidance():
    client = _client(database_id="db_gone")
    with pytest.raises(HotdataTerminalError) as excinfo:
        client.database()
    message = str(excinfo.value)
    assert "db_gone" in message
    assert "cannot be recreated" in message


def test_threads_sharing_credentials_create_once():
    # dbt hands the same credentials object to every thread's connection;
    # resolution must happen exactly once across them.
    shared = _creds()
    first_db = _client(shared).database()
    second_db = _client(shared).database()
    assert first_db is not None and first_db.id == "db_abc123"
    assert second_db is not None and second_db.id == "db_abc123"
    assert StubbedClient.created == ["dbt"]


def test_separate_invocations_do_not_share_resolution():
    # A later dbtRunner.invoke() builds fresh credentials: no state may leak
    # across invocations (stale records, other credentials' resolutions).
    _client(_creds()).database()
    _client(_creds()).database()
    assert StubbedClient.created == ["dbt", "dbt"]


def test_create_disabled_errors_even_after_another_invocation_created():
    # The old process-global cache let a create_database_if_missing: false
    # invocation silently reuse a database created by an earlier one.
    _client(_creds()).database()
    with pytest.raises(HotdataTerminalError) as excinfo:
        _client(_creds(create_database_if_missing=False)).database()
    assert "database_id" in str(excinfo.value)


def test_probe_never_creates():
    client = _client()
    assert client.database(create=False) is None
    assert client.list_tables("public") == []
    assert client.list_schemas() == []
    assert client.resolved_database_id() is None
    assert StubbedClient.created == []


def test_create_disabled_is_terminal_with_guidance():
    client = _client(create_database_if_missing=False)
    with pytest.raises(HotdataTerminalError) as excinfo:
        client.database()
    assert "database_id" in str(excinfo.value)


def test_drop_table_on_unresolved_database_is_a_no_op():
    client = _client()
    client.drop_table("orders", schema="public")  # nothing pinned, nothing created
    assert StubbedClient.created == []


# --- idempotency & staleness ------------------------------------------------


class _FakeRuntime:
    def __init__(self) -> None:
        self.add_calls = 0
        self.delete_calls = 0
        self.add_error: Exception | None = None
        self.delete_error: Exception | None = None
        self.list_error: Exception | None = None

    def add_managed_table(self, *args: Any, **kwargs: Any) -> None:
        self.add_calls += 1
        if self.add_error is not None:
            raise self.add_error

    def delete_managed_table(self, *args: Any, **kwargs: Any) -> None:
        self.delete_calls += 1
        if self.delete_error is not None:
            raise self.delete_error

    def list_managed_tables(self, *args: Any, **kwargs: Any) -> list:
        if self.list_error is not None:
            raise self.list_error
        return []


def _client_with_runtime(**overrides) -> tuple[StubbedClient, _FakeRuntime]:
    client = _client(database_id="db_abc123", **overrides)
    runtime = _FakeRuntime()
    client._runtime = runtime  # type: ignore[assignment]
    return client, runtime


def test_ensure_table_treats_already_exists_conflict_as_success():
    client, runtime = _client_with_runtime()
    runtime.add_error = _wrapped(409, "Table 'public.orders' already exists for connection 'c1'")
    client.ensure_table("orders", schema="public")  # must not raise, must not retry
    assert runtime.add_calls == 1


def test_ensure_table_does_not_swallow_other_conflicts():
    client, runtime = _client_with_runtime(max_retries=2, retry_backoff_seconds=0.0)
    runtime.add_error = _wrapped(409, "catalog is locked by another writer")
    with pytest.raises(Exception, match="locked"):
        client.ensure_table("orders", schema="public")
    assert runtime.add_calls == 2  # transient path: retried, then raised


def test_drop_table_swallows_not_found():
    client, runtime = _client_with_runtime()
    runtime.delete_error = _wrapped(404, "table not found")
    client.drop_table("orders", schema="public")  # already gone == success
    assert runtime.delete_calls == 1


def test_drop_table_raises_other_errors():
    client, runtime = _client_with_runtime(max_retries=1)
    runtime.delete_error = _wrapped(500, "boom")
    with pytest.raises(Exception, match="boom"):
        client.drop_table("orders", schema="public")


def test_list_tables_invalidates_resolution_when_database_is_gone():
    credentials = _creds(database_id="db_abc123")
    client = StubbedClient(credentials)
    runtime = _FakeRuntime()
    client._runtime = runtime  # type: ignore[assignment]
    assert client.database() is not None  # resolve + cache
    runtime.list_error = _wrapped(404, "database not found")
    with pytest.raises(HotdataTerminalError, match="dropped while this run"):
        client.list_tables("public")
    # The cached record was invalidated with it.
    assert getattr(credentials, "_hotdata_resolved_db", None) is None


def test_load_arrow_preserves_server_reported_zero(monkeypatch):
    import pyarrow as pa
    from hotdata_framework.databases import LoadManagedTableResult

    client = _client(database_id="db_abc123")
    monkeypatch.setattr(client, "upload_parquet", lambda path: "upld_1")
    monkeypatch.setattr(
        client,
        "load_managed_table",
        lambda *a, **k: LoadManagedTableResult(
            connection_id="c1", schema_name="public", table_name="t", row_count=0, full_name="f"
        ),
    )
    rows = client.load_arrow("t", schema="public", data=pa.table({"a": [1, 2]}), mode="upsert")
    assert rows == 0  # the authoritative server 0, not the input count of 2


def test_quote_ident_escapes_embedded_quotes():
    assert _quote_ident('odd"name') == '"odd""name"'
    assert _quote_ident("plain") == '"plain"'
