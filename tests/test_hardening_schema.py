# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Hardening of generated DDL: catalog names are quoted, defaults stay literals."""

from __future__ import annotations

import pytest

from auniqorm.columns import Column, PrimaryKey
from auniqorm.models import AUniqORMBase
from auniqorm.schema import DDLGenerator, SchemaDiff, quote_identifier

HOSTILE = 'x"; DROP TABLE secrets;--'
QUOTED = quote_identifier(HOSTILE, "sqlite")


async def _tables(db) -> set[str]:
    rows = await db.fetch_all("SELECT name FROM sqlite_master WHERE type = 'table'")
    return {r["name"] for r in rows}


class TestSchemaDiffQuotesCatalogNames:
    async def test_drop_table_is_quoted(self, sqlite_db):
        await sqlite_db.execute("CREATE TABLE secrets (id INTEGER PRIMARY KEY)")
        await sqlite_db.execute(f"CREATE TABLE {QUOTED} (a)")

        diff = await SchemaDiff(sqlite_db).generate()

        drops = {op.table: op.sql for op in diff.table_ops if op.action == "drop"}
        assert drops[HOSTILE] == f"DROP TABLE {QUOTED}"

    async def test_applying_drops_only_the_named_table(self, sqlite_db):
        await sqlite_db.execute("CREATE TABLE secrets (id INTEGER PRIMARY KEY)")
        await sqlite_db.execute(f"CREATE TABLE {QUOTED} (a)")

        await SchemaDiff(sqlite_db).apply(safe=False)

        assert HOSTILE not in await _tables(sqlite_db)
        # No model is registered, so every non-model table is dropped; the point is
        # that the hostile name did not smuggle a second statement.
        assert "secrets" not in await _tables(sqlite_db)

    async def test_drop_column_is_quoted(self, sqlite_db):
        class Item(AUniqORMBase):
            id: int = PrimaryKey()

        hostile_col = 'c"; DROP TABLE items;--'
        await Item.create_table()
        await sqlite_db.execute(
            f"ALTER TABLE items ADD COLUMN {quote_identifier(hostile_col, 'sqlite')}"
        )

        diff = await SchemaDiff(sqlite_db).generate()

        drops = [c for c in diff.column_ops if c.action == "drop"]
        assert [c.sql for c in drops] == [
            f"ALTER TABLE items DROP COLUMN {quote_identifier(hostile_col, 'sqlite')}"
        ]

        await SchemaDiff(sqlite_db).apply(safe=False)
        assert "items" in await _tables(sqlite_db)


def test_quote_identifier_is_importable_from_both_modules():
    from auniqorm import pandas_io, schema

    assert pandas_io.quote_identifier is schema.quote_identifier


class TestCreateIndexValidatesArguments:
    @pytest.mark.parametrize(
        ("columns", "name"),
        [
            (["id); DROP TABLE secrets;--"], "idx"),
            (["id"], "idx ON t (id); DROP TABLE secrets;--"),
            (["a b"], None),
            ([""], None),
        ],
    )
    def test_rejects_unsafe_names(self, columns, name):
        class Thing(AUniqORMBase):
            id: int = PrimaryKey()

        with pytest.raises(ValueError, match="Invalid"):
            DDLGenerator("sqlite").create_index(Thing, columns, name=name)


class TestStringDefaultsStayLiterals:
    def test_string_with_parentheses_is_not_sql(self):
        class Note(AUniqORMBase):
            id: int = PrimaryKey()
            label: str = "'x'); DROP TABLE secrets;--()"

        meta = Note.__struct_defaults__[1]
        assert isinstance(meta, Column)
        assert meta.server_default is None
        assert meta.default == "'x'); DROP TABLE secrets;--()"
        assert "DEFAULT" not in DDLGenerator("sqlite").create_table(Note)

    def test_explicit_server_default_still_works(self):
        class Stamp(AUniqORMBase):
            id: int = PrimaryKey()
            ts: str = Column(server_default="now()")

        assert "DEFAULT now()" in DDLGenerator("postgresql").create_table(Stamp)
