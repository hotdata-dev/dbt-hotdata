"""Offline end-to-end: real `dbt seed` + `dbt run` through dbt-core, with the
HTTP client swapped for the in-memory fake at the connection-manager seam.

This executes the custom materializations (table, incremental, seed) through
dbt's own jinja machinery — parse alone never renders a materialization body.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

pytest.importorskip("dbt.cli.main")

from dbt.cli.main import dbtRunner

import dbt.adapters.hotdata.connections as connections_module
from tests.conftest import FakeDbtClient

PROJECT_YML = """
name: run_check
version: "1.0"
profile: run_check
models:
  run_check:
    +materialized: table
"""

PROFILES_YML = """
run_check:
  target: dev
  outputs:
    dev:
      type: hotdata
      workspace_id: ws_test
      api_key: hk_test
      schema: public
      threads: 1
"""

SEED_CSV = "id,name,price\n1,widget,9.99\n2,gadget,19.50\n"

INCREMENTAL_MODEL = (
    "{{ config(materialized='incremental', incremental_strategy='merge', "
    "unique_key='id') }}\n"
    "select id, price from {{ ref('products') }}\n"
    "{% if is_incremental() %} where id > 0 {% endif %}"
)


@pytest.fixture
def project(tmp_path):
    project = tmp_path / "project"
    (project / "models").mkdir(parents=True)
    (project / "seeds").mkdir()
    (project / "dbt_project.yml").write_text(PROJECT_YML)
    (project / "seeds" / "products.csv").write_text(SEED_CSV)
    (project / "models" / "pricing.sql").write_text(
        "select id, price * 2 as double_price from {{ ref('products') }}"
    )
    (project / "models" / "pricing_incr.sql").write_text(INCREMENTAL_MODEL)
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(PROFILES_YML)
    return project, profiles


def _invoke(project, profiles, *args):
    return dbtRunner().invoke(
        [*args, "--project-dir", str(project), "--profiles-dir", str(profiles)]
    )


def test_seed_and_run_offline(project, monkeypatch):
    project_dir, profiles_dir = project
    fake = FakeDbtClient()
    fake.default_result = pa.table({"id": [1, 2], "price": [9.99, 19.5]})
    monkeypatch.setattr(connections_module, "HotdataDbtClient", lambda credentials: fake)

    result = _invoke(project_dir, profiles_dir, "seed")
    assert result.success, result.exception
    seed_loads = [load for load in fake.loads if load["table"] == "products"]
    assert seed_loads == [
        {"table": "products", "schema": "public", "mode": "replace", "key": None, "rows": 2}
    ]

    result = _invoke(project_dir, profiles_dir, "run")
    assert result.success, result.exception
    # Refs must render fully qualified: target.database comes from
    # _connection_keys(), and dropping it compiles refs as ""."public"."x".
    compiled = (
        project_dir / "target" / "compiled" / "run_check" / "models" / "pricing.sql"
    ).read_text()
    assert '"default"."public"."products"' in compiled, compiled
    modes = {load["table"]: load["mode"] for load in fake.loads if load["table"] != "products"}
    # First build: both models load with replace (incremental has no target yet).
    assert modes == {"pricing": "replace", "pricing_incr": "replace"}

    # Second run: the incremental model's target now exists -> native upsert.
    fake.loads.clear()
    result = _invoke(project_dir, profiles_dir, "run")
    assert result.success, result.exception
    by_table = {load["table"]: load for load in fake.loads}
    assert by_table["pricing"]["mode"] == "replace"
    assert by_table["pricing_incr"]["mode"] == "upsert"
    assert by_table["pricing_incr"]["key"] == ["id"]


def test_view_materialization_fails_with_guidance(project, monkeypatch):
    project_dir, profiles_dir = project
    fake = FakeDbtClient()
    monkeypatch.setattr(connections_module, "HotdataDbtClient", lambda credentials: fake)
    (project_dir / "models" / "a_view.sql").write_text(
        "{{ config(materialized='view') }} select 1 as id"
    )
    result = _invoke(project_dir, profiles_dir, "run", "--select", "a_view")
    assert not result.success or any(
        getattr(node_result, "status", "") == "error" for node_result in result.result
    )
