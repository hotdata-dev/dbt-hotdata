"""Retry-wrapped Hotdata client used by the dbt adapter.

Builds on :class:`hotdata_framework.managed_client.ManagedDatabaseClient` (the
shared client behind hotdata-dlt-destination and hotdata-airflow) and adds the
adapter's addressing rules:

* **Id-first.** A managed database is identified by its id, never by name —
  Hotdata names are not unique. A pinned ``database_id`` is fetched once via
  ``GET /databases/{id}``; there is deliberately no by-name lookup.
* **Create on first run.** With no ``database_id`` and
  ``create_database_if_missing``, the database is created (labelled
  ``database_name``) and its new id is logged so it can be pinned.
* **One resolution per invocation.** The resolved record is cached on the
  credentials object all of a run's connections share (dbt hands
  ``profile.credentials`` — one instance — to every thread), so threads share
  a single bind/create without any state outliving the invocation. A second
  ``dbtRunner.invoke()`` in the same process gets fresh credentials and
  resolves fresh — nothing can serve a stale or differently-configured record.

Every SQL statement is executed server-side (Apache DataFusion, Postgres
dialect) scoped to the resolved database, and results come back as Arrow.
There is no DDL surface: tables are created by declaring them and loading
parquet (``replace`` / ``append`` / ``upsert`` / ``delete`` modes).
"""

from __future__ import annotations

import os
import tempfile
import threading
import time
from typing import TYPE_CHECKING, ClassVar

import pyarrow as pa
import pyarrow.parquet as pq
from dbt.adapters.events.logging import AdapterLogger
from hotdata_framework.databases import (
    ManagedDatabase,
    ManagedTable,
    managed_database_from_detail,
)
from hotdata_framework.errors import HotdataError, HotdataTerminalError
from hotdata_framework.managed_client import ManagedDatabaseClient

from hotdata.api.databases_api import DatabasesApi

if TYPE_CHECKING:
    from hotdata_framework.client import ManagedLoadMode

    from dbt.adapters.hotdata.credentials import HotdataCredentials

logger = AdapterLogger("Hotdata")

# Attribute the resolved ManagedDatabase is cached under on the shared
# credentials object (the run cache; see the module docstring).
_RESOLVED_ATTR = "_hotdata_resolved_db"


def _chain_has_status(exc: Exception, status: int) -> bool:
    """True when ``exc`` or anything in its ``__cause__`` chain carries ``status``.

    The framework wraps ``ApiException`` in typed errors raised ``from`` it, so
    the HTTP status usually sits one level down the cause chain.
    """
    current: BaseException | None = exc
    for _ in range(6):
        if current is None:
            break
        if getattr(current, "status", None) == status:
            return True
        current = current.__cause__
    return False


def _is_not_found(exc: Exception) -> bool:
    return _chain_has_status(exc, 404)


def _chain_text(exc: Exception, limit: int = 6) -> str:
    """Lowercased messages of ``exc`` and its ``__cause__`` chain, joined."""
    parts: list[str] = []
    current: BaseException | None = exc
    for _ in range(limit):
        if current is None:
            break
        parts.append(str(current))
        current = current.__cause__
    return " ".join(parts).lower()


def _is_already_exists_conflict(exc: Exception) -> bool:
    """A 409 whose message says the resource already exists.

    409 is also how the engine reports catalog-lock contention (transient);
    only the already-exists form is a success signal for declarations.
    """
    return _chain_has_status(exc, 409) and "already exists" in _chain_text(exc)


def _quote_ident(name: str) -> str:
    """Postgres-style identifier quoting, escaping embedded quotes."""
    return '"' + name.replace('"', '""') + '"'


class HotdataDbtClient(ManagedDatabaseClient):
    """One instance per dbt connection (thread); resolution is shared."""

    # Serializes resolution across threads so exactly one of them binds or
    # creates; the resolved record itself lives on the credentials object.
    _resolve_lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(self, credentials: HotdataCredentials) -> None:
        super().__init__(
            api_key=credentials.resolve_api_key(),
            workspace_id=credentials.workspace_id or "",
            api_base_url=credentials.api_base_url,
            max_retries=credentials.max_retries,
            retry_backoff_seconds=credentials.retry_backoff_seconds,
        )
        self._credentials = credentials
        self._database_id = credentials.database_id
        self._database_label = credentials.database_name
        self._create_if_missing = credentials.create_database_if_missing

    # --- resolution (id-first, never by name) -----------------------------

    def _get_database_by_id(self, database_id: str) -> ManagedDatabase:
        """``GET /databases/{id}``; raises ``KeyError`` when the id is gone."""
        try:
            detail = self._request_with_retry(
                lambda: DatabasesApi(self._runtime.api).get_database(database_id)
            )
        except HotdataTerminalError as exc:
            if _is_not_found(exc):
                raise KeyError(database_id) from exc
            raise
        return managed_database_from_detail(detail)

    def _create_database(self) -> ManagedDatabase:
        db = self._request_with_retry(
            lambda: self._runtime.create_managed_database(description=self._database_label)
        )
        # Loud on purpose: without pinning this id, the next run creates
        # another database (names are labels, not identifiers).
        logger.warning(
            f"hotdata: created managed database {db.id} (name={self._database_label!r}). "
            f"Pin it for future runs by setting database_id: {db.id} in profiles.yml."
        )
        return db

    def database(self, *, create: bool = True) -> ManagedDatabase | None:
        """Resolve the run's managed database (id-first), creating if allowed.

        ``create=False`` is the probe form used by metadata calls before any
        model has run: it never creates and returns ``None`` when nothing is
        pinned, so an empty workspace lists as empty instead of allocating a
        database as a side effect of `dbt docs generate` or a dry parse.
        """
        with self._resolve_lock:
            cached = getattr(self._credentials, _RESOLVED_ATTR, None)
            if cached is not None:
                return cached
            if self._database_id:
                try:
                    db = self._get_database_by_id(self._database_id)
                except KeyError:
                    # Ids are server-assigned: a pinned id can never be
                    # recreated, so this is terminal, not a silent recreate.
                    raise HotdataTerminalError(
                        f"configured database_id {self._database_id!r} was not found "
                        "(it may have been dropped). A managed database cannot be "
                        "recreated with the same id — unset database_id to create a "
                        "new one, or pin an existing id."
                    ) from None
            elif not create:
                return None
            elif self._create_if_missing:
                db = self._create_database()
            else:
                raise HotdataTerminalError(
                    "no managed database is configured: set database_id: in "
                    "profiles.yml, or set create_database_if_missing: true to "
                    "create one on first run."
                )
            setattr(self._credentials, _RESOLVED_ATTR, db)
            return db

    def _invalidate_resolution(self) -> None:
        """Drop the run cache after the database itself went missing remotely."""
        with self._resolve_lock:
            if getattr(self._credentials, _RESOLVED_ATTR, None) is not None:
                setattr(self._credentials, _RESOLVED_ATTR, None)

    def _database_gone(self, db: ManagedDatabase) -> HotdataTerminalError:
        self._invalidate_resolution()
        return HotdataTerminalError(
            f"managed database {db.id} was not found — it appears to have been "
            "dropped while this run was using it."
        )

    def _require_database(self) -> ManagedDatabase:
        db = self.database(create=True)
        assert db is not None  # create=True never returns None
        return db

    def resolved_database_id(self) -> str | None:
        db = self.database(create=False)
        return db.id if db else None

    # --- SQL (server-side, Arrow back) -------------------------------------

    def execute_sql(self, sql: str) -> pa.Table:
        """Run SQL scoped to the run's database; return the full result as Arrow.

        Submits the query, polls the run/result until ready, and fetches the
        stored result as Arrow — the inline response rows are only a preview.
        """

        def operation() -> pa.Table:
            db = self._require_database()
            result_id = self._query_database_scoped(sql, database_id=db.id)
            if result_id is None:
                return pa.table({})
            return self._fetch_result_arrow(result_id, database_id=db.id)

        return self._request_with_retry(operation)

    # --- managed tables -----------------------------------------------------

    def list_tables(self, schema: str) -> list[ManagedTable]:
        db = self.database(create=False)
        if db is None:
            return []
        try:
            return self._request_with_retry(
                lambda: self._runtime.list_managed_tables(db.id, schema=schema)
            )
        except HotdataError as exc:
            # A 404 here can only mean the database itself: nothing narrower
            # is addressed. Drop the run cache so the failure is explained
            # instead of cascading into cryptic downstream errors.
            if _is_not_found(exc):
                raise self._database_gone(db) from exc
            raise

    def list_schemas(self) -> list[str]:
        db = self.database(create=False)
        if db is None:
            return []
        try:
            tables = self._request_with_retry(lambda: self._runtime.list_managed_tables(db.id))
        except HotdataError as exc:
            if _is_not_found(exc):
                raise self._database_gone(db) from exc
            raise
        return sorted({t.schema for t in tables})

    def has_table(self, table: str, *, schema: str) -> bool:
        return any(t.table == table for t in self.list_tables(schema))

    def ensure_schema(self, schema: str) -> None:
        """Declare ``schema`` on the database; already-declared is success.

        Tables can only be declared inside a declared schema — schemas do NOT
        come into being with their first table. dbt calls ``create_schema``
        for every schema its nodes need before running; this backs it.
        """
        from hotdata.models.add_managed_schema_request import AddManagedSchemaRequest

        db = self._require_database()

        def declare() -> None:
            try:
                DatabasesApi(self._runtime.api).add_database_schema(
                    db.id, AddManagedSchemaRequest(name=schema)
                )
            except Exception as error:
                if _is_already_exists_conflict(error):
                    return  # already declared — that's the goal state
                raise

        self._request_with_retry(declare)

    def ensure_table(self, table: str, *, schema: str, key: list[str] | None = None) -> None:
        """Declare ``table`` on the database; already-declared is success.

        Declares unconditionally and treats the server's 409 CONFLICT
        ("already exists") as success instead of pre-checking existence: the
        check-then-create pattern races concurrent invocations, and — worse —
        the framework classifies 409 as transient (for load catalog-lock
        contention), so an unswallowed conflict would be retried for the whole
        ~42s budget before failing. A table declared concurrently without our
        ``key`` still upserts fine: the merge key is also passed per-load.
        """
        db = self._require_database()

        def declare() -> None:
            try:
                self._runtime.add_managed_table(db.id, table, schema=schema, key=key)
            except Exception as error:
                if _is_already_exists_conflict(error):
                    return  # already declared — that's the goal state
                if _is_not_found(error) and "schema" in _chain_text(error):
                    # "Schema '<x>' is not declared": dbt normally creates
                    # schemas up front, but custom materializations may not —
                    # declare it and retry the table once.
                    self.ensure_schema(schema)
                    self._runtime.add_managed_table(db.id, table, schema=schema, key=key)
                    return
                raise  # anything else (incl. lock-contention 409s) retries normally

        self._request_with_retry(declare)

    def load_arrow(
        self,
        table: str,
        *,
        schema: str,
        data: pa.Table,
        mode: ManagedLoadMode = "replace",
        key: list[str] | None = None,
    ) -> int:
        """Write ``data`` as parquet, upload it, and apply it to ``table``.

        Returns the loaded row count. ``key`` is the per-load merge key for
        ``upsert``/``delete`` modes; ignored for ``replace``/``append``.
        """
        db = self._require_database()
        with tempfile.TemporaryDirectory(prefix="dbt_hotdata_") as tmp_dir:
            path = os.path.join(tmp_dir, "data.parquet")
            pq.write_table(data, path)
            upload_id = self.upload_parquet(path)
        attempts = max(self._max_retries, 1)
        result = None
        for attempt in range(1, attempts + 1):
            try:
                result = self.load_managed_table(
                    db.id, table, schema=schema, upload_id=upload_id, mode=mode, key=key
                )
                break
            except HotdataError as exc:
                # Deletes propagate lazily: dropping a table and redeclaring
                # its name can land the declare on the stale entry (409
                # already-exists, swallowed as success) while the load runs
                # after the delete applied — 404. Re-declaring inside the
                # retry converges from either side of that window. The miss
                # happens before anything is applied, so retrying is safe for
                # every mode — including append, which the framework itself
                # never retries.
                if _is_not_found(exc) and attempt < attempts:
                    time.sleep(min(self._retry_backoff_seconds * attempt, 5.0))
                    self.ensure_table(table, schema=schema, key=key)
                    continue
                raise
        assert result is not None  # loop either breaks with a result or raises
        # An authoritative 0 from the server (e.g. a no-op upsert) must not be
        # overwritten by the input count — `or` would treat it as missing.
        return data.num_rows if result.row_count is None else result.row_count

    def truncate_table(self, table: str, *, schema: str) -> None:
        """Empty a table while keeping its schema: replace-load zero rows.

        A ``LIMIT 0`` probe captures the table's current Arrow schema, and a
        ``replace`` load of that empty result clears the contents in place.
        """
        qualified = f'"default".{_quote_ident(schema)}.{_quote_ident(table)}'
        empty = self.execute_sql(f"select * from {qualified} limit 0")
        self.load_arrow(table, schema=schema, data=empty, mode="replace")

    def drop_table(self, table: str, *, schema: str) -> None:
        """Delete ``table``; already-absent is success (no check-then-drop race)."""
        db = self.database(create=False)
        if db is None:
            return
        try:
            self._request_with_retry(
                lambda: self._runtime.delete_managed_table(db.id, table, schema=schema)
            )
        except HotdataError as exc:
            if not _is_not_found(exc):
                raise


__all__ = ["HotdataDbtClient"]
