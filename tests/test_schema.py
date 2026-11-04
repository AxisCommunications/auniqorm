# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for auniqorm.schema DDL generation."""

import uuid
from datetime import date, datetime, time
from decimal import Decimal

import pytest

from auniqorm.columns import Column, PrimaryKey
from auniqorm.schema import DDLGenerator, python_to_db_type, unwrap_optional


class TestUnwrapOptional:
    @pytest.mark.parametrize(
        "tp,expected_type,expected_optional",
        [
            (int, int, False),
            (int | None, int, True),
            (str | None, str, True),
            # NoneType is not a Union, so unwrap_optional treats it as non-optional
            (type(None), type(None), False),
        ],
    )
    def test_unwrap(self, tp, expected_type, expected_optional):
        out_type, out_optional = unwrap_optional(tp)
        assert out_type is expected_type
        assert out_optional is expected_optional


class TestPythonToDbType:
    @pytest.mark.parametrize(
        "dialect,py_type,expected",
        [
            ("postgresql", int, "INTEGER"),
            ("postgresql", float, "DOUBLE PRECISION"),
            ("postgresql", str, "TEXT"),
            ("postgresql", bool, "BOOLEAN"),
            ("postgresql", bytes, "BYTEA"),
            ("postgresql", datetime, "TIMESTAMPTZ"),
            ("postgresql", date, "DATE"),
            ("postgresql", time, "TIME"),
            ("postgresql", Decimal, "NUMERIC"),
            ("postgresql", uuid.UUID, "UUID"),
            ("postgresql", dict, "JSONB"),
            ("postgresql", list, "JSONB"),
            ("sqlite", int, "INTEGER"),
            ("sqlite", float, "REAL"),
            ("sqlite", str, "TEXT"),
            ("sqlite", bool, "INTEGER"),
            ("sqlite", bytes, "BLOB"),
        ],
    )
    def test_dialect_types(self, dialect, py_type, expected):
        assert python_to_db_type(py_type, None, dialect) == expected

    def test_explicit_db_type_overrides(self):
        col = Column(db_type="BIGINT")
        assert python_to_db_type(int, col, "postgresql") == "BIGINT"

    def test_varchar_with_max_length(self):
        col = Column(max_length=100)
        assert python_to_db_type(str, col, "postgresql") == "VARCHAR(100)"

    def test_optional_unwraps(self):
        result = python_to_db_type(int | None, None, "postgresql")
        assert result == "INTEGER"

    def test_unknown_type_fallback(self):
        class Custom:
            pass

        assert python_to_db_type(Custom, None, "postgresql") == "TEXT"


class TestDDLGenerator:
    def _make_model(self, table_name, annotations, defaults):
        """Create a minimal mock model for DDL testing."""

        class FakeModel:
            pass

        FakeModel.__table_name__ = table_name
        FakeModel.__annotations__ = annotations
        FakeModel.__struct_defaults__ = defaults
        FakeModel.__struct_fields__ = tuple(annotations.keys())
        return FakeModel

    def test_create_table_simple(self):
        ddl = DDLGenerator("postgresql")
        model = self._make_model(
            "users",
            {"id": int, "name": str},
            [PrimaryKey(), Column(nullable=False)],
        )
        sql = ddl.create_table(model)
        assert "CREATE TABLE IF NOT EXISTS users" in sql
        assert "id SERIAL PRIMARY KEY" in sql
        assert "name TEXT NOT NULL" in sql

    def test_create_table_sqlite(self):
        ddl = DDLGenerator("sqlite")
        model = self._make_model(
            "items",
            {"id": int, "label": str},
            [PrimaryKey(), Column()],
        )
        sql = ddl.create_table(model)
        assert "INTEGER PRIMARY KEY AUTOINCREMENT" in sql

    def test_foreign_key_constraint(self):
        ddl = DDLGenerator("postgresql")
        model = self._make_model(
            "posts",
            {"id": int, "user_id": int},
            [PrimaryKey(), Column(foreign_key="users.id", on_delete="CASCADE")],
        )
        sql = ddl.create_table(model)
        assert "FOREIGN KEY (user_id) REFERENCES users(id)" in sql
        assert "ON DELETE CASCADE" in sql

    def test_unique_constraint(self):
        ddl = DDLGenerator("postgresql")
        model = self._make_model(
            "users",
            {"id": int, "email": str},
            [PrimaryKey(), Column(unique=True, nullable=False)],
        )
        sql = ddl.create_table(model)
        assert "UNIQUE (email)" in sql

    def test_server_default(self):
        ddl = DDLGenerator("postgresql")
        model = self._make_model(
            "events",
            {"id": int, "created": datetime},
            [PrimaryKey(), Column(server_default="now()")],
        )
        sql = ddl.create_table(model)
        assert "DEFAULT now()" in sql

    def test_nullable_optional_field(self):
        ddl = DDLGenerator("postgresql")
        model = self._make_model(
            "things",
            {"id": int, "note": str | None},
            [PrimaryKey(), Column()],
        )
        sql = ddl.create_table(model)
        # Optional field should NOT have NOT NULL
        assert "note TEXT" in sql
        assert "note TEXT NOT NULL" not in sql

    @pytest.mark.parametrize(
        ("dialect", "placeholder"), [("postgresql", "$1"), ("mysql", "%s")]
    )
    def test_introspect_columns_sql_information_schema(self, dialect, placeholder):
        sql, params = DDLGenerator(dialect).introspect_columns_sql("users")
        assert "information_schema.columns" in sql
        assert placeholder in sql
        assert params == ["users"]

    def test_introspect_columns_sql_sqlite(self):
        ddl = DDLGenerator("sqlite")
        sql, params = ddl.introspect_columns_sql("users")
        assert "PRAGMA table_info(users)" == sql
        assert params == []

    def test_parse_column_names_postgresql(self):
        ddl = DDLGenerator("postgresql")
        rows = [{"column_name": "id"}, {"column_name": "name"}]
        assert ddl.parse_column_names(rows) == {"id", "name"}

    def test_parse_column_names_sqlite(self):
        ddl = DDLGenerator("sqlite")
        rows = [
            {"cid": 0, "name": "id", "type": "INTEGER"},
            {"cid": 1, "name": "name", "type": "TEXT"},
        ]
        assert ddl.parse_column_names(rows) == {"id", "name"}

    def test_alter_add_column(self):
        ddl = DDLGenerator("postgresql")
        sql = ddl.alter_add_column("users", "email", str, Column(nullable=False))
        assert sql == "ALTER TABLE users ADD COLUMN email TEXT NOT NULL"

    def test_alter_add_column_nullable(self):
        ddl = DDLGenerator("postgresql")
        sql = ddl.alter_add_column("users", "bio", str | None, Column())
        assert sql == "ALTER TABLE users ADD COLUMN bio TEXT"
        assert "NOT NULL" not in sql

    def test_alter_add_column_with_default(self):
        ddl = DDLGenerator("postgresql")
        sql = ddl.alter_add_column(
            "users", "active", bool, Column(server_default="true")
        )
        assert "DEFAULT true" in sql

    def test_create_index_basic(self):
        ddl = DDLGenerator("postgresql")
        model = self._make_model("users", {"id": int}, [PrimaryKey()])
        sql = ddl.create_index(model, ["email"])
        assert sql == ("CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)")

    def test_create_index_unique(self):
        ddl = DDLGenerator("postgresql")
        model = self._make_model("users", {"id": int}, [PrimaryKey()])
        sql = ddl.create_index(model, ["email"], unique=True)
        assert "CREATE UNIQUE INDEX" in sql

    def test_create_index_custom_name(self):
        ddl = DDLGenerator("postgresql")
        model = self._make_model("users", {"id": int}, [PrimaryKey()])
        sql = ddl.create_index(model, ["a", "b"], name="my_idx")
        assert "my_idx" in sql
        assert "ON users (a, b)" in sql

    def test_create_table_mysql_pk(self):
        ddl = DDLGenerator("mysql")
        model = self._make_model("items", {"id": int}, [PrimaryKey()])
        sql = ddl.create_table(model)
        assert "PRIMARY KEY AUTO_INCREMENT" in sql

    def test_drop_table(self):
        ddl = DDLGenerator("postgresql")
        model = self._make_model("users", {"id": int}, [PrimaryKey()])
        sql = ddl.drop_table(model, if_exists=True)
        assert sql == "DROP TABLE IF EXISTS users"
        sql2 = ddl.drop_table(model, if_exists=False)
        assert sql2 == "DROP TABLE users"


class TestUnwrapOptionalEdgeCases:
    def test_none_type_direct(self):
        """type(None) itself is not a union — returned as-is."""
        tp, optional = unwrap_optional(type(None))
        assert tp is type(None)
        assert optional is False

    def test_generic_list(self):
        """list[int] should not be unwrapped as optional."""
        tp, optional = unwrap_optional(list[int])
        assert optional is False

    def test_generic_dict(self):
        """dict[str, int] should not be unwrapped as optional."""
        tp, optional = unwrap_optional(dict[str, int])
        assert optional is False


class TestPythonToDbTypeGeneric:
    def test_list_generic_postgresql(self):
        result = python_to_db_type(list[int], None, "postgresql")
        assert result == "JSONB"

    def test_dict_generic_postgresql(self):
        result = python_to_db_type(dict[str, int], None, "postgresql")
        assert result == "JSONB"

    def test_list_generic_sqlite(self):
        result = python_to_db_type(list[str], None, "sqlite")
        assert result in ("JSON", "TEXT")

    def test_dict_generic_sqlite(self):
        result = python_to_db_type(dict[str, str], None, "sqlite")
        assert result in ("JSON", "TEXT")
