# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for opt-in raw-SQL placeholder normalization.

The normalizer lets callers write portable raw SQL with a single canonical
placeholder style — qmark (``?``) or named (``:name``) — and translates it to
the connected dialect's native style ($N for PostgreSQL, %s for MySQL, ? for
SQLite) just before dispatch.  It is opt-in per call via ``paramstyle=`` so the
ORM's own (already-native) SQL is never touched.
"""

from __future__ import annotations

import pytest

from auniqorm import AUniqORMBase, Column, PrimaryKey, QueryError
from auniqorm._paramstyle import normalize_many, normalize_query

# ── qmark translation ─────────────────────────────────────────────────────────


class TestQmarkTranslation:
    @pytest.mark.parametrize(
        ("dialect", "expected_sql"),
        [
            ("postgresql", "SELECT * FROM t WHERE a = $1 AND b = $2"),
            ("mysql", "SELECT * FROM t WHERE a = %s AND b = %s"),
            ("sqlite", "SELECT * FROM t WHERE a = ? AND b = ?"),
        ],
    )
    def test_qmark_to_native(self, dialect, expected_sql):
        sql, values = normalize_query(
            "SELECT * FROM t WHERE a = ? AND b = ?",
            [1, 2],
            dialect,
            "qmark",
        )
        assert sql == expected_sql
        assert values == [1, 2]

    def test_no_params_returns_none(self):
        sql, values = normalize_query("SELECT 1", [], "postgresql", "qmark")
        assert sql == "SELECT 1"
        assert values is None

    def test_count_mismatch_raises(self):
        with pytest.raises(QueryError):
            normalize_query("SELECT ?", [1, 2], "sqlite", "qmark")

    def test_missing_params_raises(self):
        with pytest.raises(QueryError):
            normalize_query("SELECT ?", None, "postgresql", "qmark")


# ── named translation ──────────────────────────────────────────────────────────


class TestNamedTranslation:
    @pytest.mark.parametrize(
        ("dialect", "expected_sql"),
        [
            ("postgresql", "SELECT * FROM t WHERE a = $1 AND b = $2"),
            ("mysql", "SELECT * FROM t WHERE a = %s AND b = %s"),
            ("sqlite", "SELECT * FROM t WHERE a = ? AND b = ?"),
        ],
    )
    def test_named_to_native(self, dialect, expected_sql):
        sql, values = normalize_query(
            "SELECT * FROM t WHERE a = :alpha AND b = :beta",
            {"alpha": 1, "beta": 2},
            dialect,
            "named",
        )
        assert sql == expected_sql
        assert values == [1, 2]

    def test_repeated_name_emits_each_occurrence(self):
        sql, values = normalize_query(
            "SELECT * FROM t WHERE a = :x OR b = :x",
            {"x": 7},
            "postgresql",
            "named",
        )
        assert sql == "SELECT * FROM t WHERE a = $1 OR b = $2"
        assert values == [7, 7]

    def test_double_colon_cast_not_a_param(self):
        sql, values = normalize_query(
            "SELECT a::text FROM t WHERE id = :id",
            {"id": 3},
            "postgresql",
            "named",
        )
        assert sql == "SELECT a::text FROM t WHERE id = $1"
        assert values == [3]

    def test_missing_key_raises(self):
        with pytest.raises(QueryError):
            normalize_query("SELECT :a, :b", {"a": 1}, "sqlite", "named")

    def test_named_requires_mapping(self):
        with pytest.raises(QueryError):
            normalize_query("SELECT :a", [1], "sqlite", "named")


# ── auto style detection ───────────────────────────────────────────────────────


class TestAutoStyle:
    def test_auto_picks_named_for_mapping(self):
        sql, values = normalize_query(
            "SELECT * FROM t WHERE id = :id",
            {"id": 5},
            "postgresql",
            "auto",
        )
        assert sql == "SELECT * FROM t WHERE id = $1"
        assert values == [5]

    def test_auto_picks_qmark_for_sequence(self):
        sql, values = normalize_query(
            "SELECT * FROM t WHERE id = ?",
            [5],
            "postgresql",
            "auto",
        )
        assert sql == "SELECT * FROM t WHERE id = $1"
        assert values == [5]


# ── literal / comment / identifier safety ──────────────────────────────────────


class TestLiteralSafety:
    def test_qmark_inside_string_literal_untouched(self):
        sql, values = normalize_query(
            "SELECT * FROM t WHERE label = 'why?' AND id = ?",
            [1],
            "postgresql",
            "qmark",
        )
        assert sql == "SELECT * FROM t WHERE label = 'why?' AND id = $1"
        assert values == [1]

    def test_named_inside_string_literal_untouched(self):
        sql, values = normalize_query(
            "SELECT * FROM t WHERE label = ':notparam' AND id = :id",
            {"id": 1},
            "postgresql",
            "named",
        )
        assert sql == "SELECT * FROM t WHERE label = ':notparam' AND id = $1"
        assert values == [1]

    def test_escaped_quote_in_string(self):
        sql, values = normalize_query(
            "SELECT * FROM t WHERE label = 'it''s ?' AND id = ?",
            [1],
            "sqlite",
            "qmark",
        )
        assert sql == "SELECT * FROM t WHERE label = 'it''s ?' AND id = ?"
        assert values == [1]

    def test_double_quoted_identifier_untouched(self):
        sql, _ = normalize_query(
            'SELECT "we?rd" FROM t WHERE id = ?',
            [1],
            "postgresql",
            "qmark",
        )
        assert sql == 'SELECT "we?rd" FROM t WHERE id = $1'

    def test_line_comment_untouched(self):
        sql, values = normalize_query(
            "SELECT ? -- trailing ? comment\n, ?",
            [1, 2],
            "postgresql",
            "qmark",
        )
        assert sql == "SELECT $1 -- trailing ? comment\n, $2"
        assert values == [1, 2]

    def test_block_comment_untouched(self):
        sql, values = normalize_query(
            "SELECT ? /* ? :x */, ?",
            [1, 2],
            "postgresql",
            "qmark",
        )
        assert sql == "SELECT $1 /* ? :x */, $2"
        assert values == [1, 2]

    def test_dollar_quoted_body_untouched(self):
        sql, values = normalize_query(
            "SELECT $tag$ ? :x $tag$, ?",
            [9],
            "postgresql",
            "qmark",
        )
        assert sql == "SELECT $tag$ ? :x $tag$, $1"
        assert values == [9]


# ── MySQL percent escaping ──────────────────────────────────────────────────────


class TestMySQLPercentEscaping:
    def test_literal_percent_escaped_for_mysql(self):
        sql, values = normalize_query(
            "SELECT * FROM t WHERE name LIKE '%foo%' AND id = ?",
            [1],
            "mysql",
            "qmark",
        )
        assert sql == "SELECT * FROM t WHERE name LIKE '%%foo%%' AND id = %s"
        assert values == [1]

    def test_percent_not_escaped_for_postgres(self):
        sql, _ = normalize_query(
            "SELECT * FROM t WHERE name LIKE '%foo%' AND id = ?",
            [1],
            "postgresql",
            "qmark",
        )
        assert sql == "SELECT * FROM t WHERE name LIKE '%foo%' AND id = $1"

    def test_percent_not_escaped_for_sqlite(self):
        sql, _ = normalize_query(
            "SELECT * FROM t WHERE name LIKE '%foo%' AND id = ?",
            [1],
            "sqlite",
            "qmark",
        )
        assert sql == "SELECT * FROM t WHERE name LIKE '%foo%' AND id = ?"


# ── batch normalization ─────────────────────────────────────────────────────────


class TestNormalizeMany:
    def test_qmark_rows(self):
        sql, rows = normalize_many(
            "INSERT INTO t (a, b) VALUES (?, ?)",
            [[1, 2], [3, 4]],
            "postgresql",
            "qmark",
        )
        assert sql == "INSERT INTO t (a, b) VALUES ($1, $2)"
        assert rows == [[1, 2], [3, 4]]

    def test_named_rows(self):
        sql, rows = normalize_many(
            "INSERT INTO t (a, b) VALUES (:a, :b)",
            [{"a": 1, "b": 2}, {"a": 3, "b": 4}],
            "mysql",
            "named",
        )
        assert sql == "INSERT INTO t (a, b) VALUES (%s, %s)"
        assert rows == [[1, 2], [3, 4]]

    def test_empty_rows_passthrough(self):
        sql, rows = normalize_many("INSERT INTO t VALUES (?)", [], "sqlite", "qmark")
        assert sql == "INSERT INTO t VALUES (?)"
        assert rows == []


# ── invalid paramstyle ─────────────────────────────────────────────────────────


def test_invalid_style_raises():
    with pytest.raises(QueryError):
        normalize_query("SELECT ?", [1], "sqlite", "bogus")


# ── Database integration (per-call opt-in) ──────────────────────────────────────


class TestDatabaseParamstyle:
    async def test_raw_qmark_across_dialects(self, db):
        class Widget(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)
            score: int = Column(default=0)

        await Widget.create_table()
        await Widget(name="alpha", score=10).insert()
        await Widget(name="beta", score=20).insert()

        rows = await db.fetch_all(
            "SELECT name FROM widgets WHERE score = ?",
            [20],
            paramstyle="qmark",
        )
        assert [r["name"] for r in rows] == ["beta"]

    async def test_raw_named_across_dialects(self, db):
        class Gadget(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)
            score: int = Column(default=0)

        await Gadget.create_table()
        await Gadget(name="alpha", score=10).insert()
        await Gadget(name="beta", score=20).insert()

        row = await db.fetch_one(
            "SELECT name FROM gadgets WHERE score = :score",
            {"score": 10},
            paramstyle="named",
        )
        assert row is not None
        assert row["name"] == "alpha"

    async def test_auto_detects_named_from_dict(self, db):
        class Doohickey(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)

        await Doohickey.create_table()
        await Doohickey(name="alpha").insert()

        val = await db.fetch_val(
            "SELECT COUNT(*) FROM doohickeys WHERE name = :n",
            {"n": "alpha"},
            paramstyle="auto",
        )
        assert val == 1

    async def test_default_no_paramstyle_unchanged(self, sqlite_db):
        """Without paramstyle, raw SQL passes through verbatim (native ?)."""

        class Thing(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)

        await Thing.create_table()
        await Thing(name="alpha").insert()

        rows = await sqlite_db.fetch_all(
            "SELECT name FROM things WHERE name = ?", ["alpha"]
        )
        assert [r["name"] for r in rows] == ["alpha"]
