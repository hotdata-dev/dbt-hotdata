# Architecture

How `dbt-hotdata` maps dbt's warehouse-shaped expectations onto Hotdata's
API-shaped reality.

## The constraint that shapes everything

Hotdata is a managed engine (Apache DataFusion, Postgres-dialect SQL over
HTTPS) with **no DDL surface**:

- Queries: `POST /query` scoped to a managed database (`X-Database-Id`),
  polled to completion, result fetched as Arrow. SELECT only — no
  `CREATE TABLE AS`, no `INSERT`, no `ALTER`, no transactions, no bind
  parameters, no rename.
- Writes: declare a table on the database (`add_managed_table`), upload
  parquet, apply it with a load mode — `replace`, `append`, or `upsert` /
  `delete` matched by a key.

A dbt adapter normally emits DDL from macros. This one keeps materialization
and metadata work in Python and uses macros only as thin dispatch shims.

## Layers

```
dbt-core materializations (ours, in dbt/include/hotdata)
        │  {% do adapter.create_table_from_query(...) %}
HotdataAdapter (impl.py)            ── metadata + materialization workhorses
HotdataConnectionManager            ── execute(): SQL → Arrow → agate
HotdataDbtClient (client.py)        ── id-first resolution, load pipeline
hotdata_framework.ManagedDatabaseClient   ── shared retry-wrapped client
hotdata SDK (generated)             ── HTTP
```

`HotdataDbtClient` extends the same `ManagedDatabaseClient` that
hotdata-dlt-destination and hotdata-airflow build on, so retry
classification (transient 409/429/5xx vs terminal), Arrow result fetching,
and upload orchestration are shared, not duplicated.

## Model builds: the Chain pattern

`create_table_from_query(relation, sql, mode, unique_key)`:

1. Run the compiled SELECT server-side (poll run → poll result).
2. Fetch the **stored result** as Arrow — the inline response rows are only
   a preview; the stored result is the full set.
3. Write parquet to a temp file, upload it (chunked, memory-bounded, inside
   the SDK), and apply with the native mode.

Modes map from dbt semantics: `table` → `replace`; incremental `append` →
`append`; incremental `merge` + `unique_key` → `upsert` with the key passed
**per-load**, so it works regardless of what key the table was declared
with. The table is declared (`add_managed_table`) when missing, carrying the
key, so first runs and later runs take one code path.

There is deliberately no temp-table-and-rename dance: the engine has no
rename, and `replace` swaps contents in a single load anyway.

The result does round-trip through the client (engine → Arrow → parquet →
engine). That is the price of no server-side CTAS today; if the API ever
grows "create table from result_id", `create_table_from_query` is the one
place to change.

## Database addressing (id-first)

Same rules as hotdata-dlt-destination v0.11:

- A database is identified by **id** — names are labels, not identifiers,
  and there is no by-name lookup anywhere in this codebase.
- Pinned `database_id` → one `GET /databases/{id}` per process; a missing
  pinned id is a terminal error (ids can't be recreated), never a silent
  recreate.
- No pin + `create_database_if_missing` → create once, log the id loudly.
- Resolution is cached class-wide under a lock: dbt opens one connection per
  thread, and without the shared cache each thread would race to create its
  own database.
- Metadata probes (`list_relations`, docs) resolve with `create=False` so a
  parse or docs run never allocates a database as a side effect.

## Metadata without information_schema

- `list_relations_without_caching` / `list_schemas` → managed-table API.
- `get_columns_in_relation` → `SELECT * FROM rel LIMIT 0`, read the Arrow
  schema, render Postgres-surface type names (`bigint`, `numeric(38,9)`, …).
- `_get_one_catalog` (docs generate) → the two combined, built into the
  agate shape dbt expects, in Python.

## The execute path

`ConnectionManager.execute()` is overridden whole: comment-annotated SQL →
`client.execute_sql` → Arrow → agate (`table_from_data_flat`). `begin` /
`commit` are no-ops, bindings are rejected with a clear message, `cancel` is
a no-op (HTTPS in flight). This is the path tests, hooks, `dbt show`, and
source freshness ride.

## What fails, and where

Failing up front with the fix beats failing mid-run with an engine error:

- `view` materialization → compiler error naming `+materialized: table`
  (dbt's default is `view`, so new users hit this first).
- `snapshot` → compiler error suggesting append-only incrementals.
- `rename_relation`, `alter_column_type`, temp tables, Python models,
  parameterized queries → explicit errors explaining the constraint.
- Credentials are validated at connection open, naming each missing field.
- `database:` in the profile other than `default` is rejected at parse time
  — the managed database is selected by `database_id`, never in SQL.

## Testing

Offline by design. Adapter logic runs against an in-memory fake client
(`tests/conftest.py`); id-first resolution stubs only the two network seams
(`_get_database_by_id` / `_create_database`); and `tests/test_project_parse.py`
runs a real `dbt parse` on a tiny project, which loads the plugin and every
macro through dbt-core itself.
