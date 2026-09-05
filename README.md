# dbt-hotdata

[![Python versions](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://pypi.org/project/dbt-hotdata/)
[![dbt](https://img.shields.io/badge/dbt-1.10%2B-orange.svg)](https://www.getdbt.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Transform data in [Hotdata](https://hotdata.dev) instant databases with [dbt](https://www.getdbt.com).

Hotdata is a managed analytics engine (Apache DataFusion, Postgres-dialect SQL) with **no DDL surface**: tables are created by loading data, not by `CREATE TABLE`. This adapter embraces that. Every model runs as the Chain pattern, entirely against the API:

1. the model's compiled `SELECT` executes **server-side**,
2. the result streams back as Arrow,
3. a native load (`replace` / `append` / `upsert`) applies it to the managed table.

No local database engine, no driver, no version matching — pure Python over HTTPS. The same project runs unchanged on a laptop, in CI, or in a serverless function.

## Contents

- [Requirements](#requirements)
- [Install](#install)
- [Quickstart](#quickstart)
- [How materializations work](#how-materializations-work)
- [Feature support](#feature-support)
- [Configuration](#configuration)
- [How it relates to hotdata-dlt-destination](#how-it-relates-to-hotdata-dlt-destination)
- [Development](#development)
- [License](#license)

## Requirements

- Python **3.11+**, dbt-core **1.10+**
- A [Hotdata](https://hotdata.dev) workspace, an API key, and its workspace ID — from your Hotdata dashboard or the [Hotdata CLI](https://github.com/hotdata-dev/sdk-python).

## Install

```bash
pip install dbt-hotdata
# or
uv add dbt-hotdata
```

## Quickstart

**profiles.yml**

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: hotdata
      workspace_id: your_workspace_id
      # database_id: db_abc123   # pin after the first run — see below
      schema: public
      threads: 1
```

Set your API key in the environment (it's a secret; the workspace ID is routing, not a credential):

```bash
export HOTDATA_API_KEY=your_api_key
```

**dbt_project.yml** — Hotdata has no views, and dbt's default materialization is `view`, so set the project default to `table`:

```yaml
models:
  my_project:
    +materialized: table
```

Then:

```bash
dbt run
```

On first run, an instant database labelled `dbt` is created automatically and its **id** is printed:

```
hotdata: created instant database db_abc123 (name='dbt'). Pin it for future runs by
setting database_id: db_abc123 in profiles.yml.
```

Instant databases are addressed by id — Hotdata database names are not unique, so a name can't identify one. Pin `database_id` in the profile to keep building into the same database; without it, each run creates a fresh one (useful for CI or per-branch runs — databases can be set to expire).

### An incremental model

```sql
-- models/events_rollup.sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id'
) }}

select event_id, user_id, count(*) as touches, max(occurred_at) as last_seen
from {{ source('app', 'events') }}
{% if is_incremental() %}
where occurred_at > (select max(last_seen) from {{ this }})
{% endif %}
group by event_id, user_id
```

`merge` runs as a **native server-side upsert** matched on `unique_key` — updates matches, inserts the rest, no full-table read. `append` (the default strategy) adds the new rows. First runs and `--full-refresh` load with `replace`.

## How materializations work

| Materialization | What happens |
|---|---|
| `table` | Model SQL runs server-side → result loads with native `replace`. No temp table, no rename swap (there is no rename). |
| `incremental` | Same, with `append` (default) or `upsert` (`incremental_strategy: merge` + `unique_key`, composite keys supported). |
| `seed` | The CSV becomes Arrow (numbers stay exact — integers and decimals, never silently floats), then a `replace` load. `column_types:` are applied as Arrow casts. |
| `ephemeral` | Standard dbt — inlined into consumers, nothing built. |
| `view` | ❌ Fails up front: Hotdata has no views. The error tells you to set `+materialized: table`. |
| `snapshot` | ❌ Fails up front: merges update rows in place, so past versions aren't kept. Keep history with an `append` incremental model. |

Tests, `dbt show`, analyses, and source freshness all run as plain SELECTs on the server. `dbt docs generate` builds the catalog from the managed-table API plus Arrow schema probes.

Schema evolution is additive and automatic: a model that starts producing a new column just includes it in the next load — existing data is never touched, and types can widen but never silently shrink. (`on_schema_change` is therefore ignored.)

## Feature support

| Feature | Support | Notes |
|---|:-:|---|
| `table`, `incremental`, `seed`, `ephemeral` | ✅ | See above |
| Incremental strategies | ⚠️ | `append`, `merge` (native upsert by `unique_key`). No `delete+insert`, no `microbatch` |
| `view`, `snapshot` | ❌ | Clear error up front |
| Tests (generic + singular) | ✅ | Run server-side; `store_failures` supported |
| `dbt docs generate` | ✅ | Catalog from the managed-table API |
| Source freshness | ✅ | `loaded_at_field` queries run server-side |
| Hooks (`pre-hook`/`post-hook`, `on-run-*`) | ⚠️ | Run server-side — SELECT-shaped SQL only (no DDL exists) |
| Python models | ❌ | |
| Model contracts / constraints | ❌ | No DDL; dbt warns they are unenforced |
| Grants | ❌ | Ignored with a warning — access is governed by workspace API keys |
| Transactions | ❌ | `begin`/`commit` are no-ops (DataFusion has no transactions) |
| Query cancellation | ❌ | An in-flight HTTPS query can't be interrupted client-side |

## Configuration

| Profile field | Env variable | Default | Description |
|---|---|---|---|
| `api_key` | `HOTDATA_API_KEY` | required | API key (a secret — prefer the env var or `"{{ env_var('HOTDATA_API_KEY') }}"`) |
| `workspace_id` | `HOTDATA_WORKSPACE` | required | Workspace ID (routing, not a secret) |
| `database_id` | `HOTDATA_DATABASE` | — | Id of the instant database to build into. **This is how a database is targeted** — names aren't unique. Printed on first-run create; pin it to reuse |
| `database_name` | — | `dbt` | Display label used **only when creating** a new database (never to look one up) |
| `schema` | — | `public` | Schema inside the instant database |
| `create_database_if_missing` | — | `true` | Create a database on first run when no `database_id` is pinned |
| `api_base_url` | `HOTDATA_API_URL` | `https://api.hotdata.dev` | API endpoint |
| `max_retries` | — | `8` | Retry budget for transient errors (409/429/5xx). Loads take a catalog-level lock per database; ~42s of linear backoff outlasts a concurrent writer |
| `retry_backoff_seconds` | — | `1.5` | Initial retry wait (grows linearly) |
| `threads` | — | `1` | Loads into one database serialize server-side (contention is retried); more threads still help when models spend most of their time in query execution |

`database:` stays unset — inside an instant database the SQL catalog is always literally `default` (relations render as `"default"."schema"."table"`), and the adapter rejects any other value up front.

Fields left unset in the profile resolve from the platform's own `HOTDATA_*` environment variables (explicit profile values always win). These are the Hotdata CLI conventions — under any orchestrator that sets them, the adapter needs no profile fields at all beyond `type: hotdata`. A `database_id` adopted from the environment is logged, since it retargets the whole build.

## How it relates to hotdata-dlt-destination

[hotdata-dlt-destination](https://github.com/hotdata-dev/hotdata-dlt-destination) loads external data **into** Hotdata (the EL); this adapter transforms it **inside** Hotdata (the T). They share the same conventions — `HOTDATA_API_KEY` from the environment, `workspace_id` as a plain parameter, id-first `database_id` addressing, the same retry classification — and the same underlying SDK (`hotdata` + `hotdata-framework`). Point dbt at the `database_id` your dlt pipeline prints, add sources for the loaded tables, and build models on top.

### Running after a dlt load

[hotdata-dlt-destination](https://github.com/hotdata-dev/hotdata-dlt-destination) ships a dbt bridge: after a pipeline run, one helper call executes a dbt package against the exact instant database the load just wrote — no profiles.yml to author, credentials and routing reused from the pipeline. See “Transform with dbt” in that repo's README. This adapter itself knows nothing about dlt; the bridge drives it through the `HOTDATA_*` environment contract above.

## Development

The project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
git clone https://github.com/hotdata-dev/dbt-hotdata.git
cd dbt-hotdata

uv sync                 # install deps (including dev group)

uv run pytest           # run the test suite (offline — no credentials needed)
uv run ruff check       # lint
uv run ruff format      # format
uv run mypy             # type-check
```

The test suite runs entirely offline: adapter logic is exercised against an in-memory fake client, and a real `dbt parse` verifies plugin registration and every macro.

## License

[MIT](LICENSE) © Hotdata Inc.

## Resources

- [Hotdata Python SDK](https://github.com/hotdata-dev/sdk-python) · [hotdata-framework](https://github.com/hotdata-dev/sdk-python-framework)
- [hotdata-dlt-destination](https://github.com/hotdata-dev/hotdata-dlt-destination)
- [dbt adapter documentation](https://docs.getdbt.com/docs/connect-adapters)
- [Changelog](CHANGELOG.md) · [Architecture](docs/architecture.md)
