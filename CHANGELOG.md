# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- Cross-database macros for DataFusion's SQL surface: `dateadd`, `datediff`,
  `convert_timezone`.
- Transient API errors (409/429/5xx) retry for ~42s via the shared
  `hotdata-framework` client; terminal errors fail the node immediately.
- CI: lint, format, type-check, offline tests (Python 3.11/3.12), and a
  wheel-contents check.
