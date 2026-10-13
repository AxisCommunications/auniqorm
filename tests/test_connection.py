# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for auniqorm.connection.Database and SQLite CRUD integration."""

import pytest

from auniqorm.columns import Column, ForeignKey, PrimaryKey
from auniqorm.connection import Database
from auniqorm.exceptions import ConnectionError
from auniqorm.models import AUniqORMBase


class TestDatabaseConnection:
    async def test_connect_disconnect(self):
        db = Database("sqlite://:memory:")
        assert not db.is_connected
        await db.connect()
        assert db.is_connected
        await db.disconnect()
        assert not db.is_connected

    async def test_protocol_detection(self):
        assert Database._parse_protocol("postgresql://localhost/db") == "postgresql"
        assert Database._parse_protocol("postgres://localhost/db") == "postgresql"
        assert Database._parse_protocol("pg://localhost/db") == "postgresql"
        assert Database._parse_protocol("sqlite:///path") == "sqlite"
        assert Database._parse_protocol("mysql://localhost/db") == "mysql"
        assert Database._parse_protocol("mariadb://localhost/db") == "mysql"
        assert Database._parse_protocol("cockroachdb://localhost/db") == "postgresql"
        assert Database._parse_protocol("cockroach://localhost/db") == "postgresql"
        assert Database._parse_protocol("crdb://localhost/db") == "postgresql"

    async def test_context_manager(self):
        async with Database("sqlite://:memory:") as db:
            assert db.is_connected
        assert not db.is_connected

    async def test_execute_not_connected_raises(self):
        db = Database("sqlite://:memory:")
        with pytest.raises(ConnectionError):
            await db.execute("SELECT 1")


class TestDatabaseQueries:
    async def test_execute_raw_sql(self, sqlite_db):
        await sqlite_db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
        await sqlite_db.execute("INSERT INTO test (val) VALUES (?)", ["hello"])
        row = await sqlite_db.fetch_one("SELECT * FROM test WHERE val = ?", ["hello"])
        assert row is not None
        assert row["val"] == "hello"

    async def test_fetch_all(self, sqlite_db):
        await sqlite_db.execute("CREATE TABLE nums (n INTEGER)")
        for i in range(5):
            await sqlite_db.execute("INSERT INTO nums (n) VALUES (?)", [i])
        rows = await sqlite_db.fetch_all("SELECT * FROM nums ORDER BY n")
        assert len(rows) == 5
        assert [r["n"] for r in rows] == [0, 1, 2, 3, 4]

    async def test_fetch_val(self, sqlite_db):
        await sqlite_db.execute("CREATE TABLE kv (k TEXT, v INTEGER)")
        await sqlite_db.execute("INSERT INTO kv VALUES (?, ?)", ["x", 42])
        val = await sqlite_db.fetch_val("SELECT v FROM kv WHERE k = ?", ["x"])
        assert val == 42

    async def test_fetch_one_no_result(self, sqlite_db):
        await sqlite_db.execute("CREATE TABLE empty (id INTEGER)")
        row = await sqlite_db.fetch_one("SELECT * FROM empty")
        assert row is None

    async def test_execute_many(self, sqlite_db):
        await sqlite_db.execute("CREATE TABLE batch (id INTEGER, name TEXT)")
        await sqlite_db.execute_many(
            "INSERT INTO batch VALUES (?, ?)",
            [[1, "a"], [2, "b"], [3, "c"]],
        )
        rows = await sqlite_db.fetch_all("SELECT * FROM batch ORDER BY id")
        assert len(rows) == 3


class TestModelCRUD:
    """Integration tests for AUniqORMBase CRUD operations via SQLite."""

    async def test_create_table_and_insert(self, sqlite_db):
        class User(AUniqORMBase):
            id: int = PrimaryKey()
            email: str = Column(nullable=False, unique=True)
            name: str

        await User.create_table()
        user = User(email="alice@example.com", name="Alice")
        await user.insert()

        row = await sqlite_db.fetch_one(
            "SELECT * FROM users WHERE email = ?", ["alice@example.com"]
        )
        assert row is not None
        assert row["name"] == "Alice"

    async def test_get(self, sqlite_db):
        class Product(AUniqORMBase):
            id: int = PrimaryKey()
            sku: str = Column(nullable=False)

        await Product.create_table()
        Product.__database__ = sqlite_db
        p = Product(sku="WDG-001")
        await p.insert()

        found = await Product.get(sku="WDG-001")
        assert found is not None
        assert found.sku == "WDG-001"

    async def test_get_not_found(self, sqlite_db):
        class Ghost(AUniqORMBase):
            id: int = PrimaryKey()
            name: str

        await Ghost.create_table()
        Ghost.__database__ = sqlite_db
        result = await Ghost.get(name="nope")
        assert result is None

    async def test_filter(self, sqlite_db):
        class Tag(AUniqORMBase):
            id: int = PrimaryKey()
            label: str = Column(nullable=False)

        await Tag.create_table()
        Tag.__database__ = sqlite_db
        await Tag(label="red").insert()
        await Tag(label="blue").insert()
        await Tag(label="red").insert()

        reds = await Tag.filter(label="red")
        assert len(reds) == 2

    async def test_all(self, sqlite_db):
        class Color(AUniqORMBase):
            id: int = PrimaryKey()
            name: str

        await Color.create_table()
        Color.__database__ = sqlite_db
        await Color(name="r").insert()
        await Color(name="g").insert()
        all_colors = await Color.all()
        assert len(all_colors) == 2

    async def test_drop_table(self, sqlite_db):
        class Ephemeral(AUniqORMBase):
            id: int = PrimaryKey()

        await Ephemeral.create_table()
        await sqlite_db.execute("INSERT INTO ephemerals (id) VALUES (?)", [1])
        await Ephemeral.drop_table()
        # Table should no longer exist
        row = await sqlite_db.fetch_one(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='ephemerals'"
        )
        assert row is None

    async def test_foreign_key(self, sqlite_db):
        class Author(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)

        class Book(AUniqORMBase):
            id: int = PrimaryKey()
            title: str = Column(nullable=False)
            author_id: int = ForeignKey("authors.id")

        await Author.create_table()

        # DDL generator uses "REFERENCES table.column" format which
        # is valid for PostgreSQL but SQLite needs "REFERENCES table(column)".
        # Verify the DDL is generated, even if SQLite can't execute it.
        from auniqorm.schema import DDLGenerator

        ddl = DDLGenerator("sqlite")
        sql = ddl.create_table(Book)
        assert "FOREIGN KEY (author_id)" in sql
        assert "REFERENCES authors(id)" in sql
