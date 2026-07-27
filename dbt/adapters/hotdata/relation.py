from __future__ import annotations

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation


@dataclass(frozen=True, eq=False, repr=False)
class HotdataRelation(BaseRelation):
    """Relations render Postgres-style: ``"default"."<schema>"."<table>"``.

    The database part is always the literal ``default`` catalog — the managed
    database itself is selected out-of-band (by ``database_id``), never in SQL.
    Nothing is renameable: the managed-table API has no rename, and the
    materializations are written so they never need one.
    """

    renameable_relations = frozenset()
    replaceable_relations = frozenset()
