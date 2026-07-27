"""End-to-end plugin registration: `dbt parse` on a real (tiny) project.

Parsing loads the adapter plugin and every macro in dbt/include/hotdata, so
this catches jinja syntax errors and dispatch problems without a connection.
"""

from __future__ import annotations

import pytest

pytest.importorskip("dbt.cli.main")

from dbt.cli.main import dbtRunner

PROJECT_YML = """
name: parse_check
version: "1.0"
profile: parse_check
models:
  parse_check:
    +materialized: table
"""

PROFILES_YML = """
parse_check:
  target: dev
  outputs:
    dev:
      type: hotdata
      workspace_id: ws_test
      api_key: hk_test
      schema: public
      threads: 1
"""


def test_dbt_parse_loads_adapter_and_macros(tmp_path, monkeypatch):
    project = tmp_path / "project"
    (project / "models").mkdir(parents=True)
    (project / "dbt_project.yml").write_text(PROJECT_YML)
    (project / "models" / "orders.sql").write_text("select 1 as id, 99.0 as total")
    (project / "models" / "orders_incr.sql").write_text(
        "{{ config(materialized='incremental', unique_key='id', "
        "incremental_strategy='merge') }} select 1 as id"
    )
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(PROFILES_YML)

    result = dbtRunner().invoke(
        [
            "parse",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(profiles),
        ]
    )
    assert result.success, result.exception
