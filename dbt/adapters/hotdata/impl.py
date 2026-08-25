"""Hotdata dbt adapter.

Hotdata has no DDL surface — tables are created by declaring them on the
instant database and loading parquet, and queries run server-side (Apache
DataFusion, Postgres dialect) returning Arrow. The adapter therefore keeps
all metadata and materialization work in Python:

* Relation listing, columns, and the docs catalog come from the
  managed-table API plus a ``SELECT * ... LIMIT 0`` Arrow-schema probe —
  never from ``information_schema`` SQL.
* Materializations call :meth:`create_table_from_query` /
  :meth:`load_seed`, which run the model's SQL server-side, pull the result
  as Arrow, and apply it with a native load mode (``replace`` / ``append`` /
  ``upsert``). Nothing is ever renamed or swapped — there is no rename.
* Views and snapshots do not exist server-side; those materializations fail
  up front with a clear message (see the macros) rather than mid-run.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dbt.adapters.base import available
from dbt.adapters.base.impl import ConstraintSupport
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.sql import SQLAdapter
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.hotdata.column import HotdataColumn
from dbt.adapters.hotdata.connections import HotdataConnectionManager
from dbt.adapters.hotdata.relation import HotdataRelation
from dbt.adapters.hotdata.seeds import agate_to_arrow

if TYPE_CHECKING:
    import agate
    from dbt.adapters.base.relation import InformationSchema

    from dbt.adapters.hotdata.client import HotdataDbtClient

_CATALOG_COLUMNS = (
    "table_database",
    "table_schema",
    "table_name",
    "table_type",
    "table_comment",
    "column_name",
    "column_index",
    "column_type",
    "column_comment",
    "table_owner",
)


class HotdataAdapter(SQLAdapter):
    ConnectionManager = HotdataConnectionManager
    Relation = HotdataRelation
    Column = HotdataColumn

    # No DDL means no server-side constraint enforcement of any kind; dbt
    # warns instead of silently pretending a contract is enforced.
    CONSTRAINT_SUPPORT = {  # noqa: RUF012 — mirrors the base class attribute
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
    }

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    def _client(self) -> HotdataDbtClient:
        return self.connections.get_thread_connection().handle

    @staticmethod
    def _schema_of(relation: BaseRelation) -> str:
        if relation.schema is None:
            raise DbtRuntimeError(f"relation {relation} has no schema")
        return relation.schema

    # --- schemas ------------------------------------------------------------

    def list_schemas(self, database: str) -> list[str]:
        return self._client().list_schemas()

    def check_schema_exists(self, database: str, schema: str) -> bool:
        return schema in self.list_schemas(database)

    def create_schema(self, relation: BaseRelation) -> None:
        # Schemas must be declared before tables can be declared inside them.
        self._client().ensure_schema(self._schema_of(relation))
        self.cache.add_schema(relation.database, relation.schema)

    def drop_schema(self, relation: BaseRelation) -> None:
        client = self._client()
        for managed_table in client.list_tables(self._schema_of(relation)):
            client.drop_table(managed_table.table, schema=self._schema_of(relation))
        self.cache.drop_schema(relation.database, relation.schema)

    # --- relations ------------------------------------------------------------

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> list[BaseRelation]:
        client = self._client()
        return [
            self.Relation.create(
                database=schema_relation.database,
                schema=schema_relation.schema,
                identifier=managed_table.table,
                type=RelationType.Table,
            )
            for managed_table in client.list_tables(self._schema_of(schema_relation))
        ]

    def get_columns_in_relation(self, relation: BaseRelation) -> list[HotdataColumn]:
        # The engine's own types, from the Arrow schema of an empty probe —
        # there is no information_schema to query.
        arrow = self._client().execute_sql(f"select * from {relation} limit 0")
        return [HotdataColumn.from_arrow_field(field) for field in arrow.schema]

    def drop_relation(self, relation: BaseRelation) -> None:
        # No existence pre-check: the client treats an already-absent table as
        # success, which is race-free and one API call cheaper.
        client = self._client()
        if relation.identifier:
            client.drop_table(relation.identifier, schema=self._schema_of(relation))
        self.cache_dropped(relation)

    def truncate_relation(self, relation: BaseRelation) -> None:
        if relation.identifier is None:
            raise DbtRuntimeError("cannot truncate a relation with no identifier")
        self._client().truncate_table(relation.identifier, schema=self._schema_of(relation))

    def rename_relation(self, from_relation: BaseRelation, to_relation: BaseRelation) -> None:
        raise DbtRuntimeError(
            "Hotdata managed tables cannot be renamed. The hotdata materializations "
            "never rename; if you hit this from a custom materialization, restructure "
            "it to build the target directly (see create_table_from_query)."
        )

    def expand_column_types(self, goal: BaseRelation, current: BaseRelation) -> None:
        # Loads widen column types additively server-side; nothing to alter.
        pass

    def expand_target_column_types(
        self, from_relation: BaseRelation, to_relation: BaseRelation
    ) -> None:
        pass

    def valid_incremental_strategies(self) -> list[str]:
        return ["append", "merge"]

    def submit_python_job(self, parsed_model: dict, compiled_code: str) -> Any:
        raise DbtRuntimeError("Python models are not supported on Hotdata")

    # --- materialization workhorses (called from macros) ----------------------

    @available
    def create_table_from_query(
        self,
        relation: BaseRelation,
        sql: str,
        mode: str = "replace",
        unique_key: str | list[str] | None = None,
    ) -> dict[str, Any]:
        """Build ``relation`` from ``sql`` — the Chain pattern, engine-side.

        Runs the model's SELECT on the engine, pulls the full result as
        Arrow, and applies it to the managed table with a native load mode:
        ``replace`` (table builds, full refreshes), ``append`` (incremental),
        or ``upsert`` (incremental with a ``unique_key``, matched per-load).
        The table is declared first when missing, so first runs and
        subsequent runs take the same path.
        """
        if relation.identifier is None:
            raise DbtRuntimeError("cannot materialize a relation with no identifier")
        if mode not in ("replace", "append", "upsert"):
            raise DbtRuntimeError(f"unsupported load mode {mode!r}")
        key: list[str] | None = None
        if unique_key is not None:
            key = [unique_key] if isinstance(unique_key, str) else list(unique_key)
        if mode == "upsert" and not key:
            raise DbtRuntimeError("upsert requires a unique_key")

        client = self._client()
        arrow = client.execute_sql(sql)
        if arrow.num_columns == 0:
            raise DbtRuntimeError(
                f"model {relation} produced no result set — the compiled SQL must be a "
                "SELECT (Hotdata materializations run the model's query server-side and "
                "load the result)"
            )
        client.ensure_table(relation.identifier, schema=self._schema_of(relation), key=key)
        if arrow.num_rows == 0 and mode != "replace":
            # Nothing to add or match: skip the upload and load entirely. Each
            # load takes the database's catalog-level write lock, so a no-new-rows
            # incremental run must not contend with concurrent writers. (An empty
            # `replace` still loads — that is how a full refresh truncates.)
            self.cache_added(relation)
            return {"message": f"LOAD {mode.upper()} 0 (no new rows)", "rows": 0}
        rows = client.load_arrow(
            relation.identifier,
            schema=self._schema_of(relation),
            data=arrow,
            mode=mode,  # type: ignore[arg-type]
            key=key if mode == "upsert" else None,
        )
        self.cache_added(relation)
        return {"message": f"LOAD {mode.upper()} {rows}", "rows": rows}

    @available
    def load_seed(
        self,
        relation: BaseRelation,
        agate_table: agate.Table,
        column_types: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Load a dbt seed: agate -> Arrow -> parquet -> replace load."""
        if relation.identifier is None:
            raise DbtRuntimeError("cannot seed a relation with no identifier")
        arrow = agate_to_arrow(agate_table, column_types)
        client = self._client()
        client.ensure_table(relation.identifier, schema=self._schema_of(relation))
        rows = client.load_arrow(
            relation.identifier, schema=self._schema_of(relation), data=arrow, mode="replace"
        )
        self.cache_added(relation)
        return {"message": f"LOAD SEED {rows}", "rows": rows}

    # --- docs catalog -----------------------------------------------------------

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: set[str],
        used_schemas: frozenset[tuple[str, str]],
    ) -> agate.Table:
        from dbt_common.clients.agate_helper import table_from_data_flat

        client = self._client()
        rows: list[dict[str, Any]] = []
        for schema in schemas:
            for managed_table in client.list_tables(schema):
                relation = self.Relation.create(
                    database=information_schema.database,
                    schema=schema,
                    identifier=managed_table.table,
                    type=RelationType.Table,
                )
                try:
                    columns = self.get_columns_in_relation(relation)
                except DbtRuntimeError:
                    # Declared-but-never-loaded tables have no queryable
                    # schema yet; list them without columns.
                    columns = []
                base = {
                    "table_database": information_schema.database,
                    "table_schema": schema,
                    "table_name": managed_table.table,
                    "table_type": "BASE TABLE",
                    "table_comment": None,
                    "table_owner": None,
                }
                if not columns:
                    rows.append(
                        {
                            **base,
                            "column_name": None,
                            "column_index": None,
                            "column_type": None,
                            "column_comment": None,
                        }
                    )
                for index, column in enumerate(columns):
                    rows.append(
                        {
                            **base,
                            "column_name": column.name,
                            "column_index": index + 1,
                            "column_type": column.dtype,
                            "column_comment": None,
                        }
                    )
        table = table_from_data_flat(rows, _CATALOG_COLUMNS)
        return self._catalog_filter_table(table, used_schemas)
