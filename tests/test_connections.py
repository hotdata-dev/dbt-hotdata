from __future__ import annotations

import multiprocessing
from types import SimpleNamespace
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.hotdata.connections import HotdataConnectionManager, _ArrowCursorShim


@pytest.fixture
def manager(fake_client):
    mgr = HotdataConnectionManager(MagicMock(), multiprocessing.get_context("spawn"))
    connection = SimpleNamespace(name="test", handle=fake_client, credentials=None)
    mgr.get_thread_connection = lambda: connection  # type: ignore[assignment, method-assign, return-value]
    return mgr


def test_execute_fetch_returns_agate(manager, fake_client):
    fake_client.results["select customer"] = pa.table(
        {"customer": ["Alice", "Bob"], "spend": [99, 49]}
    )
    response, table = manager.execute("select customer, spend from orders", fetch=True)
    assert response.rows_affected == 2
    assert [tuple(row) for row in table.rows] == [("Alice", 99), ("Bob", 49)]
    assert table.column_names == ("customer", "spend")


def test_execute_without_fetch_returns_empty_table(manager, fake_client):
    response, table = manager.execute("select 1")
    assert response._message == "OK"
    assert len(table.rows) == 0
    assert fake_client.executed == ["select 1"]


def test_execute_limit_slices_rows(manager, fake_client):
    fake_client.default_result = pa.table({"n": [1, 2, 3, 4]})
    _, table = manager.execute("select n", fetch=True, limit=2)
    assert [row[0] for row in table.rows] == [1, 2]


def test_bindings_are_rejected(manager):
    with pytest.raises(DbtRuntimeError, match="parameterized"):
        manager.add_query("select * from t where id = ?", bindings=[1])


def test_errors_surface_as_dbt_runtime_errors(manager, fake_client):
    from hotdata_framework.errors import HotdataTerminalError

    def boom(sql):
        raise HotdataTerminalError("400: Bad Request — table 'x' not found")

    fake_client.execute_sql = boom
    with pytest.raises(DbtRuntimeError, match="not found"):
        manager.execute("select * from x")


def test_transactions_are_no_ops(manager):
    manager.begin()
    manager.commit()
    manager.clear_transaction()


def test_cursor_shim_fetch_surface():
    cursor = _ArrowCursorShim(pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}))
    assert [d[0] for d in cursor.description] == ["a", "b"]
    assert cursor.fetchone() == (1, "x")
    assert cursor.fetchmany(1) == [(2, "y")]
    assert cursor.fetchall() == [(3, "z")]
    assert cursor.fetchone() is None
