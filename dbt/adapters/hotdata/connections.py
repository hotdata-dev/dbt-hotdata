"""dbt connection manager for Hotdata.

There is no database driver here: a "connection" is an HTTPS client
(:class:`HotdataDbtClient`), SQL executes server-side on Apache DataFusion
(Postgres dialect) scoped to the run's instant database, and results come
back as Arrow. Consequences for the dbt contract:

* ``begin``/``commit`` are no-ops — the engine has no transactions.
* There is no bind protocol — SQL goes over as a literal string.
* ``cancel`` is a no-op — an in-flight HTTP query cannot be interrupted.

Transient failures (409 from a concurrent writer's catalog lock, 429, 5xx)
are retried inside the client for up to ``max_retries`` x backoff before
they surface here; whatever does surface is terminal for the node.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.exceptions import DbtRuntimeError
from hotdata_framework.errors import HotdataError

from dbt.adapters.hotdata.client import HotdataDbtClient

if TYPE_CHECKING:
    from collections.abc import Iterator

    import agate
    import pyarrow as pa

logger = AdapterLogger("Hotdata")


class _ArrowCursorShim:
    """Just enough DB-API cursor for base-class helpers that expect one.

    Row materialization is lazy: most statements run with ``fetch=False`` and
    never touch rows, so the Arrow table must not be copied into Python
    objects at construction time.
    """

    def __init__(self, table: pa.Table) -> None:
        self.table = table
        self._materialized: list[dict[str, Any]] | None = None
        self._pos = 0

    @property
    def _rows(self) -> list[dict[str, Any]]:
        if self._materialized is None:
            self._materialized = self.table.to_pylist()
        return self._materialized

    @property
    def description(self) -> list[tuple[Any, ...]]:
        return [(name, None, None, None, None, None, None) for name in self.table.column_names]

    def fetchall(self) -> list[tuple[Any, ...]]:
        rows = [tuple(row.values()) for row in self._rows[self._pos :]]
        self._pos = len(self._rows)
        return rows

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        end = self._pos + size
        rows = [tuple(row.values()) for row in self._rows[self._pos : end]]
        self._pos = min(end, len(self._rows))
        return rows

    def fetchone(self) -> tuple[Any, ...] | None:
        if self._pos >= len(self._rows):
            return None
        row = tuple(self._rows[self._pos].values())
        self._pos += 1
        return row


class HotdataConnectionManager(SQLConnectionManager):
    TYPE = "hotdata"

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            return connection
        credentials = connection.credentials
        try:
            credentials.validate_connection_setup()
            connection.handle = HotdataDbtClient(credentials)
            connection.state = ConnectionState.OPEN
        except Exception as exc:
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise FailedToConnectError(str(exc)) from exc
        return connection

    def cancel(self, connection: Connection) -> None:
        # An HTTPS query in flight has nothing to interrupt client-side.
        pass

    @classmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        rows = getattr(getattr(cursor, "table", None), "num_rows", None)
        return AdapterResponse(_message="OK", rows_affected=rows)

    @contextmanager
    def exception_handler(self, sql: str) -> Iterator[None]:
        try:
            yield
        except DbtRuntimeError:
            raise
        except HotdataError as exc:
            logger.debug(f"hotdata error while running:\n{sql}")
            raise DbtRuntimeError(str(exc)) from exc
        except Exception as exc:
            logger.debug(f"error while running:\n{sql}")
            raise DbtRuntimeError(str(exc)) from exc

    # --- execution ----------------------------------------------------------

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Any | None = None,
        abridge_sql_log: bool = False,
        retryable_exceptions: tuple[type[Exception], ...] = (),
        retry_limit: int = 1,
    ) -> tuple[Connection, Any]:
        if bindings:
            raise DbtRuntimeError(
                "the hotdata adapter does not support parameterized queries "
                "(the query API takes a plain SQL string with no bind protocol)"
            )
        connection = self.get_thread_connection()
        client: HotdataDbtClient = connection.handle
        fire_sql = sql if not abridge_sql_log else f"{sql[:512]}..."
        logger.debug(f'Using hotdata connection "{connection.name}"')
        logger.debug(f"On {connection.name}: {fire_sql}")
        started = time.perf_counter()
        with self.exception_handler(sql):
            table = client.execute_sql(sql)
        elapsed = time.perf_counter() - started
        logger.debug(f"SQL status: OK ({table.num_rows} rows) in {elapsed:.2f} seconds")
        return connection, _ArrowCursorShim(table)

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: int | None = None,
    ) -> tuple[AdapterResponse, agate.Table]:
        from dbt_common.clients.agate_helper import empty_table, table_from_data_flat

        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            arrow: pa.Table = cursor.table
            if limit is not None:
                arrow = arrow.slice(0, limit)
            table = table_from_data_flat(arrow.to_pylist(), arrow.column_names)
        else:
            table = empty_table()
        return response, table

    # --- transactions (none) --------------------------------------------------

    def begin(self) -> None:
        pass

    def commit(self) -> None:
        pass

    def clear_transaction(self) -> None:
        pass
