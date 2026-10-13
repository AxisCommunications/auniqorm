# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for automatic column migration (ALTER TABLE ADD COLUMN)."""

import logging

import pytest

from auniqorm.columns import Column, PrimaryKey
from auniqorm.exceptions import SchemaMigrationError
from auniqorm.models import AUniqORMBase
from auniqorm.schema import SchemaDiff


class TestSyncColumnsSuccess:
    """Adding nullable or defaulted columns to an existing table."""

    async def test_adds_missing_nullable_column(self, sqlite_db):
        # Create table with only id + name
        await sqlite_db.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"
        )

        class Item(AUniqORMBase):
            id: int = PrimaryKey()
            name: str
            description: str | None

        await Item.create_table()

        # Verify the new column exists
        rows = await sqlite_db.fetch_all("PRAGMA table_info(items)")
        col_names = {r["name"] for r in rows}
        assert "description" in col_names

    async def test_adds_column_with_server_default(self, sqlite_db):
        await sqlite_db.execute(
            "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT)"
        )
        # Insert a row so the table is non-empty
        await sqlite_db.execute("INSERT INTO events (title) VALUES (?)", ["launch"])

        class Event(AUniqORMBase):
            id: int = PrimaryKey()
            title: str
            active: int = Column(server_default="1")

        await Event.create_table()

        rows = await sqlite_db.fetch_all("PRAGMA table_info(events)")
        col_names = {r["name"] for r in rows}
        assert "active" in col_names

        # Verify the default was applied to the existing row
        row = await sqlite_db.fetch_one(
            "SELECT active FROM events WHERE title = ?", ["launch"]
        )
        assert row["active"] == 1

    async def test_no_op_when_columns_match(self, sqlite_db):
        """No ALTER issued when the table already matches the model."""

        class Widget(AUniqORMBase):
            id: int = PrimaryKey()
            label: str

        await Widget.create_table()

        # Second call should succeed without errors (idempotent)
        await Widget.create_table()

        rows = await sqlite_db.fetch_all("PRAGMA table_info(widgets)")
        col_names = [r["name"] for r in rows]
        assert col_names == ["id", "label"]

    async def test_adds_multiple_missing_columns(self, sqlite_db):
        await sqlite_db.execute(
            "CREATE TABLE multi (id INTEGER PRIMARY KEY AUTOINCREMENT)"
        )

        class Multi(AUniqORMBase, table="multi"):
            id: int = PrimaryKey()
            a: str | None
            b: int | None
            c: float | None

        await Multi.create_table()

        rows = await sqlite_db.fetch_all("PRAGMA table_info(multi)")
        col_names = {r["name"] for r in rows}
        assert {"id", "a", "b", "c"} == col_names

    async def test_logs_info_on_successful_migration(self, sqlite_db, caplog):
        await sqlite_db.execute(
            "CREATE TABLE logged (id INTEGER PRIMARY KEY AUTOINCREMENT)"
        )

        class Logged(AUniqORMBase, table="logged"):
            id: int = PrimaryKey()
            extra: str | None

        with caplog.at_level(logging.INFO, logger="auniqorm"):
            await Logged.create_table()

        assert "Migrated table 'logged'" in caplog.text
        assert "extra" in caplog.text


class TestSyncColumnsRollback:
    """Failed migrations roll back and re-raise."""

    async def test_not_null_no_default_raises_on_nonempty_table(
        self, sqlite_db, caplog
    ):
        await sqlite_db.execute(
            "CREATE TABLE strict (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"
        )
        await sqlite_db.execute("INSERT INTO strict (name) VALUES (?)", ["existing"])

        class Strict(AUniqORMBase, table="strict"):
            id: int = PrimaryKey()
            name: str
            required: str = Column(nullable=False)

        with (
            caplog.at_level(logging.WARNING, logger="auniqorm"),
            pytest.raises(SchemaMigrationError, match="strict") as exc_info,
        ):
            await Strict.create_table()

        err = exc_info.value
        assert err.table == "strict"
        assert "required" in err.columns
        assert err.cause is not None
        assert "rolled back" in caplog.text

        # Verify the column was NOT added (rollback worked)
        rows = await sqlite_db.fetch_all("PRAGMA table_info(strict)")
        col_names = {r["name"] for r in rows}
        assert "required" not in col_names

    async def test_rollback_leaves_existing_data_intact(self, sqlite_db):
        await sqlite_db.execute(
            "CREATE TABLE safe (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)"
        )
        await sqlite_db.execute("INSERT INTO safe (val) VALUES (?)", ["keep"])

        class Safe(AUniqORMBase, table="safe"):
            id: int = PrimaryKey()
            val: str
            bad: str = Column(nullable=False)

        with pytest.raises(SchemaMigrationError):
            await Safe.create_table()

        row = await sqlite_db.fetch_one("SELECT val FROM safe WHERE id = 1")
        assert row is not None
        assert row["val"] == "keep"


# ── SchemaDiff ───────────────────────────────────────────────────────────────


class TestSchemaDiffDetectsNewTable:
    async def test_detects_table_to_create(self, sqlite_db):
        class NewTbl(AUniqORMBase, table="new_tbl"):
            id: int = PrimaryKey()
            name: str

        diff = await SchemaDiff(sqlite_db).generate()
        assert diff.has_changes
        tables_to_create = [t.table for t in diff.table_ops if t.action == "create"]
        assert "new_tbl" in tables_to_create

    async def test_no_changes_when_synced(self, sqlite_db):
        class Synced(AUniqORMBase, table="synced"):
            id: int = PrimaryKey()
            val: int

        await Synced.create_table()

        diff = await SchemaDiff(sqlite_db).generate()
        # Should have no ops for this table
        synced_ops = [c for c in diff.column_ops if c.table == "synced"] + [
            t for t in diff.table_ops if t.table == "synced"
        ]
        assert synced_ops == []


class TestSchemaDiffDetectsColumnChanges:
    async def test_detects_column_to_add(self, sqlite_db):
        await sqlite_db.execute(
            "CREATE TABLE partial (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"
        )

        class Partial(AUniqORMBase, table="partial"):
            id: int = PrimaryKey()
            name: str
            extra: str | None

        diff = await SchemaDiff(sqlite_db).generate()
        adds = [
            c for c in diff.column_ops if c.action == "add" and c.table == "partial"
        ]
        assert len(adds) == 1
        assert adds[0].column == "extra"

    async def test_detects_column_to_drop(self, sqlite_db):
        await sqlite_db.execute(
            "CREATE TABLE wide (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "name TEXT, obsolete TEXT)"
        )

        class Wide(AUniqORMBase, table="wide"):
            id: int = PrimaryKey()
            name: str

        diff = await SchemaDiff(sqlite_db).generate()
        drops = [c for c in diff.column_ops if c.action == "drop" and c.table == "wide"]
        assert len(drops) == 1
        assert drops[0].column == "obsolete"


class TestSchemaDiffDetectsOrphanTable:
    async def test_detects_table_to_drop(self, sqlite_db):
        await sqlite_db.execute("CREATE TABLE orphan (id INTEGER PRIMARY KEY)")

        diff = await SchemaDiff(sqlite_db).generate()
        drops = [
            t for t in diff.table_ops if t.action == "drop" and t.table == "orphan"
        ]
        assert len(drops) == 1


class TestSchemaDiffSqlStatements:
    async def test_sql_statements_order(self, sqlite_db):
        """Creates come before drops in statement list."""
        await sqlite_db.execute("CREATE TABLE old_tbl (id INTEGER PRIMARY KEY)")

        class Brand(AUniqORMBase, table="brand_new"):
            id: int = PrimaryKey()

        diff = await SchemaDiff(sqlite_db).generate()
        stmts = diff.sql_statements()
        assert any("brand_new" in s for s in stmts)
        assert any("old_tbl" in s for s in stmts)

        # Creates should appear before drops
        create_idx = next(i for i, s in enumerate(stmts) if "brand_new" in s)
        drop_idx = next(i for i, s in enumerate(stmts) if "old_tbl" in s)
        assert create_idx < drop_idx


class TestSchemaDiffSummary:
    async def test_summary_no_changes(self, sqlite_db):
        class Clean(AUniqORMBase, table="clean"):
            id: int = PrimaryKey()

        await Clean.create_table()
        diff = await SchemaDiff(sqlite_db).generate()
        # Filter to only ops for "clean" table
        clean_table_ops = [t for t in diff.table_ops if t.table == "clean"]
        clean_col_ops = [c for c in diff.column_ops if c.table == "clean"]
        assert clean_table_ops == []
        assert clean_col_ops == []

    async def test_summary_has_content(self, sqlite_db):
        class Todo(AUniqORMBase):
            id: int = PrimaryKey()

        diff = await SchemaDiff(sqlite_db).generate()
        summary = diff.summary()
        assert "CREATE" in summary


class TestSchemaDiffApply:
    async def test_apply_creates_table(self, sqlite_db):
        class Applied(AUniqORMBase, table="applied"):
            id: int = PrimaryKey()
            name: str

        diff = await SchemaDiff(sqlite_db).apply()
        assert diff.has_changes

        # Table should now exist
        row = await sqlite_db.fetch_one(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='applied'"
        )
        assert row is not None

    async def test_apply_adds_column(self, sqlite_db):
        await sqlite_db.execute(
            "CREATE TABLE grow (id INTEGER PRIMARY KEY AUTOINCREMENT, a TEXT)"
        )

        class Grow(AUniqORMBase, table="grow"):
            id: int = PrimaryKey()
            a: str
            b: str | None

        diff = await SchemaDiff(sqlite_db).apply()
        adds = [c for c in diff.column_ops if c.action == "add" and c.table == "grow"]
        assert len(adds) == 1

        rows = await sqlite_db.fetch_all("PRAGMA table_info(grow)")
        col_names = {r["name"] for r in rows}
        assert "b" in col_names

    async def test_apply_safe_skips_drops(self, sqlite_db):
        await sqlite_db.execute(
            "CREATE TABLE keep_me (id INTEGER PRIMARY KEY, extra TEXT)"
        )

        class KeepMe(AUniqORMBase, table="keep_me"):
            id: int = PrimaryKey()

        diff = await SchemaDiff(sqlite_db).apply(safe=True)
        drops = [c for c in diff.column_ops if c.action == "drop"]
        assert len(drops) == 1  # detected but not applied

        # Column should still exist
        rows = await sqlite_db.fetch_all("PRAGMA table_info(keep_me)")
        col_names = {r["name"] for r in rows}
        assert "extra" in col_names
