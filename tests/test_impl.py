"""Adapter materialization workhorses, run against the in-memory fake client.

The methods are exercised unbound with a minimal harness standing in for the
adapter instance, so no dbt RuntimeConfig is needed.
"""

from __future__ import annotations

import agate
import pyarrow as pa
import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.hotdata.column import HotdataColumn
from dbt.adapters.hotdata.impl import HotdataAdapter
from dbt.adapters.hotdata.relation import HotdataRelation


class Harness:
    Relation = HotdataRelation
    Column = HotdataColumn
    _schema_of = staticmethod(HotdataAdapter._schema_of)

    def __init__(self, client):
        self.client = client
        self.added = []
        self.dropped = []

    def _client(self):
        return self.client

    def cache_added(self, relation):
        self.added.append(relation)

    def cache_dropped(self, relation):
        self.dropped.append(relation)


def _relation(identifier="orders", schema="public"):
    from dbt.adapters.contracts.relation import RelationType

    return HotdataRelation.create(
        database="default", schema=schema, identifier=identifier, type=RelationType.Table
    )


@pytest.fixture
def harness(fake_client):
    return Harness(fake_client)


def test_create_table_replace(harness, fake_client):
    fake_client.default_result = pa.table({"id": [1, 2], "total": [99.0, 49.5]})
    result = HotdataAdapter.create_table_from_query(harness, _relation(), "select ...")
    assert result == {"message": "LOAD REPLACE 2", "rows": 2}
    assert fake_client.loads == [
        {"table": "orders", "schema": "public", "mode": "replace", "key": None, "rows": 2}
    ]
    assert harness.added == [_relation()]


def test_incremental_upsert_passes_key(harness, fake_client):
    fake_client.default_result = pa.table({"id": [1], "total": [10.0]})
    HotdataAdapter.create_table_from_query(
        harness, _relation(), "select ...", mode="upsert", unique_key="id"
    )
    load = fake_client.loads[0]
    assert load["mode"] == "upsert"
    assert load["key"] == ["id"]
    # The table is declared with the key too, so upsert works on first creation.
    assert fake_client.tables[("public", "orders")]["key"] == ["id"]


def test_composite_unique_key(harness, fake_client):
    HotdataAdapter.create_table_from_query(
        harness, _relation(), "select ...", mode="upsert", unique_key=["id", "region"]
    )
    assert fake_client.loads[0]["key"] == ["id", "region"]


def test_append_does_not_send_key(harness, fake_client):
    HotdataAdapter.create_table_from_query(
        harness, _relation(), "select ...", mode="append", unique_key="id"
    )
    assert fake_client.loads[0]["mode"] == "append"
    assert fake_client.loads[0]["key"] is None


def test_upsert_without_key_fails_up_front(harness):
    with pytest.raises(DbtRuntimeError, match="unique_key"):
        HotdataAdapter.create_table_from_query(harness, _relation(), "select ...", mode="upsert")


def test_unknown_mode_fails_up_front(harness):
    with pytest.raises(DbtRuntimeError, match="unsupported load mode"):
        HotdataAdapter.create_table_from_query(harness, _relation(), "select ...", mode="scd2")


def test_empty_result_is_a_clear_error(harness, fake_client):
    fake_client.default_result = pa.table({})
    with pytest.raises(DbtRuntimeError, match="produced no result"):
        HotdataAdapter.create_table_from_query(harness, _relation(), "call something()")


def test_load_seed(harness, fake_client):
    from decimal import Decimal

    table = agate.Table(
        [[Decimal(1), "Alice"], [Decimal(2), "Bob"]],
        column_names=["id", "name"],
        column_types=[agate.Number(), agate.Text()],
    )
    result = HotdataAdapter.load_seed(harness, _relation("customers"), table)
    assert result == {"message": "LOAD SEED 2", "rows": 2}
    assert fake_client.loads[0]["mode"] == "replace"


def test_get_columns_in_relation_probes_with_limit_zero(harness, fake_client):
    fake_client.results['from "default"."public"."orders" limit 0'] = pa.table(
        {"id": pa.array([], pa.int64()), "total": pa.array([], pa.decimal128(38, 9))}
    )
    columns = HotdataAdapter.get_columns_in_relation(harness, _relation())
    assert 'select * from "default"."public"."orders" limit 0' in fake_client.executed
    assert [(c.name, c.dtype) for c in columns] == [("id", "bigint"), ("total", "numeric(38,9)")]


def test_list_relations(harness, fake_client):
    fake_client.ensure_table("orders", schema="public")
    fake_client.ensure_table("customers", schema="public")
    fake_client.ensure_table("elsewhere", schema="other")
    schema_relation = HotdataRelation.create(database="default", schema="public")
    relations = HotdataAdapter.list_relations_without_caching(harness, schema_relation)
    assert sorted(r.identifier or "" for r in relations) == ["customers", "orders"]
    assert all(r.type == "table" for r in relations)


def test_drop_relation_is_idempotent(harness, fake_client):
    fake_client.ensure_table("orders", schema="public")
    HotdataAdapter.drop_relation(harness, _relation())
    assert ("public", "orders") not in fake_client.tables
    # Dropping again must not error (the table is already gone).
    HotdataAdapter.drop_relation(harness, _relation())
    assert harness.dropped == [_relation(), _relation()]


def test_create_schema_declares_it(harness, fake_client):
    from unittest.mock import MagicMock

    harness.cache = MagicMock()
    HotdataAdapter.create_schema(harness, HotdataRelation.create(database="default", schema="raw"))
    # Schemas must be declared before tables can be declared inside them —
    # a no-op here breaks any project with a non-default schema (e.g. seeds).
    assert "raw" in fake_client.schemas
    harness.cache.add_schema.assert_called_once()


def test_rename_is_a_clear_error(harness):
    with pytest.raises(DbtRuntimeError, match="renamed"):
        HotdataAdapter.rename_relation(harness, _relation("a"), _relation("b"))
