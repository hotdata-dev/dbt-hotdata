# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.2] - 2026-09-09

### Changed

- Raised the Hotdata SDK ceilings to `hotdata<0.10` and
  `hotdata-framework<0.15`, allowing the newest coherent pair (hotdata 0.9.x +
  framework 0.14.x). Verified against a live workspace: full dbt run and test
  suite pass with hotdata 0.9.1 + hotdata-framework 0.14.0.

## [0.2.1] - 2026-09-09

### Changed

- Version bump only, to produce the first PyPI release via the tag-triggered
  release workflow.

## [0.2.0] - 2026-09-08

### Added

- Ambient-environment fallbacks for profile fields from the platform's own
  `HOTDATA_*` variables (explicit profile values always win): the API key
  from `HOTDATA_API_KEY`, `workspace_id` from `HOTDATA_WORKSPACE`,
  `database_id` from `HOTDATA_DATABASE`, and `api_base_url` from
  `HOTDATA_API_URL`. Resolution reuses the `hotdata_framework.env` helpers,
  so URLs are normalized the same way as every other SDK consumer. A dbt
  project runs with no profile fields beyond `type: hotdata` when the
  environment provides the rest, and orchestrator bridges (e.g.
  hotdata-dlt-destination's dbt bridge) drive the adapter through the same
  contract. A `database_id` adopted from the environment is logged, since it
  retargets the whole build.

### Fixed

- `convert_timezone` returned the UTC instant unshifted; it now converts via
  `to_local_time()` (DST-aware, non-UTC sources verified) and defaults a
  falsy `source_tz` to UTC.

### Changed

- Docs and comments now use the product branding — HotSQL for the SQL
  surface, Hotdata for engine behavior — and the README documents the
  cross-database macros and the SQL dialect story.
- Dev lockfile resolves dbt-core 1.12.3 (sqlparse 0.6.0), clearing the
  open Dependabot alerts; the supported floor stays dbt-core 1.10.

## [0.1.0] - 2026-07-27

### Added

- Initial dbt adapter for Hotdata managed databases (`type: hotdata`). Models
  run server-side and the results load back with native modes — no local
  engine, no DDL, pure HTTPS.
- Materializations: `table`, `incremental` (`append`, or `merge` as a native
  upsert by `unique_key`), `seed` (numbers stay exact), `ephemeral`. Views and
  snapshots fail up front with actionable errors.
- dbt unit tests, data tests, `dbt show`, source freshness, and
  `dbt docs generate`.
- Id-first database addressing: pin `database_id` in the profile, or let the
  first run create a database and print its id.
- Cross-database macros for HotSQL: `dateadd`, `datediff`,
  `convert_timezone`.
- Transient API errors (409/429/5xx) retry for ~42s via the shared
  `hotdata-framework` client; terminal errors fail the node immediately.
- CI: lint, format, type-check, offline tests (Python 3.11/3.12), and a
  wheel-contents check.
