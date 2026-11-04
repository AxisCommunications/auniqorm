# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Mock-based tests for PostgreSQL and MySQL drivers.

These exercise the driver logic without requiring real database servers by
mocking the underlying asyncpg / aiomysql connection pools.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from auniqorm.exceptions import QueryError

# Calls every driver guards with "Not connected" before touching the pool.
NOT_CONNECTED_CALLS = [
    pytest.param("execute", ("SELECT 1",), id="execute"),
    pytest.param("execute_many", ("SELECT 1", []), id="execute_many"),
    pytest.param("fetch_one", ("SELECT 1",), id="fetch_one"),
    pytest.param("fetch_all", ("SELECT 1",), id="fetch_all"),
    pytest.param("fetch_val", ("SELECT 1",), id="fetch_val"),
    pytest.param("begin", (), id="begin"),
]
STREAM_CALLS = [
    pytest.param("iterate", id="iterate"),
    pytest.param("iterate_batches", id="iterate_batches"),
]


async def _call(driver: Any, method: str, *args: Any) -> None:
    """Await a driver method, draining it if it is an async generator."""
    result = getattr(driver, method)(*args)
    if hasattr(result, "__aiter__"):
        async for _ in result:
            pass
    else:
        await result


# ---------------------------------------------------------------------------
# Fixtures: mock asyncpg pool / connection
# ---------------------------------------------------------------------------


@pytest.fixture
def asyncpg_pool() -> AsyncMock:
    """Fresh mock asyncpg.Pool for each test."""
    pool = AsyncMock()
    pool.close = AsyncMock()
    pool.execute = AsyncMock(return_value="INSERT 0 1")
    pool.executemany = AsyncMock()
    pool.fetchrow = AsyncMock(return_value={"id": 1, "name": "alice"})
    pool.fetch = AsyncMock(return_value=[{"id": 1}, {"id": 2}])
    pool.fetchval = AsyncMock(return_value=42)
    pool.acquire = AsyncMock()
    pool.release = AsyncMock()
    yield pool


@pytest.fixture
def asyncpg_conn() -> AsyncMock:
    """Fresh mock asyncpg connection (for transactions)."""
    conn = AsyncMock()
    conn.execute = AsyncMock()
    conn.fetchrow = AsyncMock(return_value={"id": 1})
    conn.fetch = AsyncMock(return_value=[{"id": 1}])
    conn.fetchval = AsyncMock(return_value=99)
    yield conn


# ---------------------------------------------------------------------------
# Fixtures: mock aiomysql cursor / connection / pool
# ---------------------------------------------------------------------------


@pytest.fixture
def mysql_cursor(request: pytest.FixtureRequest) -> AsyncMock:
    """Fresh mock aiomysql cursor.  Pass overrides via indirect parametrize."""
    overrides: dict[str, Any] = getattr(request, "param", {}) or {}
    cursor = AsyncMock()
    cursor.execute = AsyncMock()
    cursor.executemany = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=overrides.get("fetchone", {"id": 1}))
    cursor.fetchall = AsyncMock(
        return_value=overrides.get("fetchall", [{"id": 1}]),
    )
    cursor.lastrowid = overrides.get("lastrowid", 1)
    yield cursor


@pytest.fixture
def mysql_conn(mysql_cursor: AsyncMock) -> AsyncMock:
    """Fresh mock aiomysql connection wired to *mysql_cursor*."""
    conn = AsyncMock()
    conn.commit = AsyncMock()
    conn.begin = AsyncMock()
    conn.rollback = AsyncMock()

    @asynccontextmanager
    async def _cursor(cursor_cls=None):
        yield mysql_cursor

    conn.cursor = _cursor
    yield conn


@pytest.fixture
def mysql_pool(mysql_conn: AsyncMock) -> MagicMock:
    """Fresh mock aiomysql pool wired to *mysql_conn*."""
    pool = MagicMock()
    pool.close = MagicMock()
    pool.wait_closed = AsyncMock()
    pool.release = MagicMock()

    @asynccontextmanager
    async def _acquire():
        yield mysql_conn

    pool.acquire = _acquire
    yield pool


# ---------------------------------------------------------------------------
# PostgreSQL driver
# ---------------------------------------------------------------------------


class TestPostgresDriver:
    @pytest.fixture
    def asyncpg_conn_pinned(self):
        """Mock connection returned by pool.acquire() context manager."""
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="INSERT 0 1")
        conn.executemany = AsyncMock()
        conn.fetchrow = AsyncMock(return_value={"id": 1, "name": "alice"})
        conn.fetch = AsyncMock(return_value=[{"id": 1}, {"id": 2}])
        conn.fetchval = AsyncMock(return_value=42)
        yield conn

    @pytest.fixture
    def driver(self, asyncpg_pool, asyncpg_conn_pinned):
        from contextlib import asynccontextmanager

        from auniqorm.drivers import postgresql as pg_mod

        orig = pg_mod.asyncpg
        pg_mod.asyncpg = MagicMock()
        d = pg_mod.PostgresAsyncpgDriver("postgresql://user:pass@localhost/db")
        pg_mod.asyncpg = orig
        d._pool = asyncpg_pool

        # Mock pool.acquire() as async context manager yielding the connection
        @asynccontextmanager
        async def _acquire():
            yield asyncpg_conn_pinned

        asyncpg_pool.acquire = _acquire
        # Keep a reference for assertions in tests
        d._mock_conn = asyncpg_conn_pinned
        yield d

    @pytest.fixture
    def driver_no_pool(self):
        from auniqorm.drivers import postgresql as pg_mod

        orig = pg_mod.asyncpg
        pg_mod.asyncpg = MagicMock()
        d = pg_mod.PostgresAsyncpgDriver("postgresql://localhost/db")
        pg_mod.asyncpg = orig
        d._pool = None
        yield d

    async def test_is_connected(self, driver):
        assert driver.is_connected is True
        driver._pool = None
        assert driver.is_connected is False

    async def test_connect_already_connected(self, driver):
        pool = driver._pool
        await driver.connect()
        assert driver._pool is pool  # unchanged

    async def test_disconnect(self, driver):
        pool = driver._pool
        await driver.disconnect()
        pool.close.assert_awaited_once()
        assert driver._pool is None

    @pytest.mark.parametrize(("method", "args"), NOT_CONNECTED_CALLS)
    async def test_not_connected(self, driver_no_pool, method, args):
        with pytest.raises(QueryError, match="Not connected"):
            await _call(driver_no_pool, method, *args)

    @pytest.mark.parametrize("method", STREAM_CALLS)
    async def test_stream_not_connected(self, driver_no_pool, method):
        with pytest.raises(QueryError, match="Not connected"):
            await _call(driver_no_pool, method, "SELECT 1")

    @pytest.mark.parametrize(
        ("method", "args", "conn_method", "message"),
        [
            ("execute", ("BAD SQL",), "execute", "Execute failed"),
            ("execute_many", ("BAD", [[1]]), "executemany", "Execute many failed"),
            ("fetch_one", ("BAD",), "fetchrow", "Fetch one failed"),
            ("fetch_all", ("BAD",), "fetch", "Fetch all failed"),
            ("fetch_val", ("BAD",), "fetchval", "Fetch val failed"),
        ],
    )
    async def test_error_wraps(self, driver, method, args, conn_method, message):
        setattr(
            driver._mock_conn, conn_method, AsyncMock(side_effect=RuntimeError("boom"))
        )
        with pytest.raises(QueryError, match=message):
            await _call(driver, method, *args)

    async def test_disconnect_when_not_connected(self, driver_no_pool):
        await driver_no_pool.disconnect()  # no-op, no error

    # -- execute --

    async def test_execute_with_values(self, driver):
        result = await driver.execute("INSERT INTO t VALUES ($1)", [1])
        driver._mock_conn.execute.assert_awaited_once_with(
            "INSERT INTO t VALUES ($1)", 1
        )
        assert result == "INSERT 0 1"

    async def test_execute_without_values(self, driver):
        await driver.execute("CREATE TABLE t (id INT)")
        driver._mock_conn.execute.assert_awaited_once_with("CREATE TABLE t (id INT)")

    # -- execute_many --

    async def test_execute_many(self, driver):
        await driver.execute_many("INSERT INTO t VALUES ($1)", [[1], [2]])
        driver._mock_conn.executemany.assert_awaited_once()

    # -- fetch_one --

    async def test_fetch_one_with_values(self, driver):
        row = await driver.fetch_one("SELECT * FROM t WHERE id=$1", [1])
        driver._mock_conn.fetchrow.assert_awaited_once_with(
            "SELECT * FROM t WHERE id=$1", 1
        )
        assert row == {"id": 1, "name": "alice"}

    async def test_fetch_one_without_values(self, driver):
        row = await driver.fetch_one("SELECT * FROM t LIMIT 1")
        assert row is not None

    async def test_fetch_one_returns_none(self, driver):
        driver._mock_conn.fetchrow = AsyncMock(return_value=None)
        row = await driver.fetch_one("SELECT * FROM t WHERE id=$1", [999])
        assert row is None

    # -- fetch_all --

    async def test_fetch_all_with_values(self, driver):
        rows = await driver.fetch_all("SELECT * FROM t WHERE x=$1", [1])
        assert rows == [{"id": 1}, {"id": 2}]

    async def test_fetch_all_without_values(self, driver):
        rows = await driver.fetch_all("SELECT * FROM t")
        assert len(rows) == 2

    # -- fetch_val --

    async def test_fetch_val_with_values(self, driver):
        val = await driver.fetch_val("SELECT count(*) FROM t WHERE x=$1", [1])
        assert val == 42

    async def test_fetch_val_without_values(self, driver):
        val = await driver.fetch_val("SELECT count(*) FROM t")
        assert val == 42

    # -- transactions --

    async def test_begin(self, driver, asyncpg_conn):
        # An asyncpg-managed transaction, so cursors work inside it.
        transaction = AsyncMock()
        asyncpg_conn.transaction = MagicMock(return_value=transaction)
        driver._pool.acquire = AsyncMock(return_value=asyncpg_conn)
        await driver.begin()
        scope = driver._scope()
        assert scope.conn is asyncpg_conn
        assert scope.state is transaction
        transaction.start.assert_awaited_once()
        asyncpg_conn.execute.assert_not_awaited()

    @pytest.mark.parametrize("method", ["commit", "rollback"])
    async def test_end_transaction(self, driver, asyncpg_conn, method):
        transaction = AsyncMock()
        driver._open_scope((asyncpg_conn, transaction), began=True)
        await getattr(driver, method)()
        getattr(transaction, method).assert_awaited_once()
        driver._pool.release.assert_awaited_once_with(asyncpg_conn)
        assert driver._scope() is None

    async def test_begin_releases_connection_when_start_fails(
        self, driver, asyncpg_conn
    ):
        transaction = AsyncMock()
        transaction.start.side_effect = RuntimeError("boom")
        asyncpg_conn.transaction = MagicMock(return_value=transaction)
        driver._pool.acquire = AsyncMock(return_value=asyncpg_conn)
        with pytest.raises(RuntimeError, match="boom"):
            await driver.begin()
        driver._pool.release.assert_awaited_once_with(asyncpg_conn)
        assert driver._scope() is None and not driver._live_scopes

    @pytest.mark.parametrize("method", ["commit", "rollback"])
    async def test_end_without_transaction_is_noop(self, driver, method):
        await getattr(driver, method)()

    # -- transaction delegates to connection --

    async def test_execute_uses_transaction_conn(self, driver, asyncpg_conn):
        driver._open_scope((asyncpg_conn, None), began=True)
        await driver.execute("SELECT 1", [42])
        asyncpg_conn.execute.assert_awaited_once_with("SELECT 1", 42)

    async def test_fetch_one_uses_transaction_conn(self, driver, asyncpg_conn):
        driver._open_scope((asyncpg_conn, None), began=True)
        row = await driver.fetch_one("SELECT * FROM t WHERE id=$1", [1])
        asyncpg_conn.fetchrow.assert_awaited_once_with("SELECT * FROM t WHERE id=$1", 1)
        assert row == {"id": 1}

    async def test_fetch_all_uses_transaction_conn(self, driver, asyncpg_conn):
        driver._open_scope((asyncpg_conn, None), began=True)
        await driver.fetch_all("SELECT * FROM t")
        asyncpg_conn.fetch.assert_awaited_once_with("SELECT * FROM t")

    async def test_fetch_val_uses_transaction_conn(self, driver, asyncpg_conn):
        driver._open_scope((asyncpg_conn, None), began=True)
        await driver.fetch_val("SELECT 1", [5], column=0)
        asyncpg_conn.fetchval.assert_awaited_once_with("SELECT 1", 5, column=0)

    # -- iterate --

    async def test_iterate_yields_rows(self, driver, asyncpg_conn_pinned):
        """iterate() uses pool connection + transaction + cursor."""
        records = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

        # Patch conn.transaction() as async context manager
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _fake_txn():
            yield

        asyncpg_conn_pinned.transaction = _fake_txn

        # Patch conn.cursor() to return an async-iterable
        async def _fake_cursor_iter():
            for r in records:
                yield r

        asyncpg_conn_pinned.cursor = lambda *a, **kw: _fake_cursor_iter()

        # pool.acquire() returns awaitable conn (not context manager)
        driver._pool.acquire = AsyncMock(return_value=asyncpg_conn_pinned)
        driver._pool.release = AsyncMock()

        rows = []
        async for row in driver.iterate("SELECT * FROM t", [1]):
            rows.append(row)
        assert rows == records

    @pytest.mark.parametrize("method", STREAM_CALLS)
    async def test_stream_error_wraps(self, driver, asyncpg_conn_pinned, method):
        @asynccontextmanager
        async def _fail_txn():
            raise RuntimeError("cursor error")
            yield

        asyncpg_conn_pinned.transaction = _fail_txn
        driver._pool.acquire = AsyncMock(return_value=asyncpg_conn_pinned)
        driver._pool.release = AsyncMock()

        with pytest.raises(QueryError, match="Iterate failed"):
            await _call(driver, method, "SELECT 1")

    async def test_iterate_uses_txn_conn(self, driver, asyncpg_conn):
        """When in a transaction, iterate uses the txn connection."""
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _fake_txn():
            yield

        asyncpg_conn.transaction = _fake_txn

        records = [{"id": 1}]

        async def _fake_cursor_iter():
            for r in records:
                yield r

        asyncpg_conn.cursor = lambda *a, **kw: _fake_cursor_iter()
        driver._open_scope((asyncpg_conn, None), began=True)

        rows = []
        async for row in driver.iterate("SELECT 1"):
            rows.append(row)
        assert rows == records

    # -- iterate_batches --

    async def test_iterate_batches_yields_chunks(self, driver, asyncpg_conn_pinned):
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def _fake_txn():
            yield

        asyncpg_conn_pinned.transaction = _fake_txn

        # asyncpg: awaiting conn.cursor(...) opens a Cursor with fetch(n)
        batches = [[{"id": 1}, {"id": 2}], [{"id": 3}], []]
        batch_iter = iter(batches)

        mock_cursor = MagicMock()
        mock_cursor.fetch = AsyncMock(side_effect=lambda n: next(batch_iter))

        async def _open_cursor(*args, **kwargs):
            return mock_cursor

        asyncpg_conn_pinned.cursor = _open_cursor

        driver._pool.acquire = AsyncMock(return_value=asyncpg_conn_pinned)
        driver._pool.release = AsyncMock()

        results = []
        async for batch in driver.iterate_batches("SELECT * FROM t", batch_size=2):
            results.append(batch)
        assert results == [[{"id": 1}, {"id": 2}], [{"id": 3}]]


# ---------------------------------------------------------------------------
# MySQL driver
# ---------------------------------------------------------------------------


class TestMySQLDriver:
    @pytest.fixture
    def driver(self, mysql_pool):
        from auniqorm.drivers import mysql as mysql_mod

        orig = mysql_mod.aiomysql
        mysql_mod.aiomysql = MagicMock()
        d = mysql_mod.MySQLDriver("mysql://user:pass@localhost:3306/db")
        d._pool = mysql_pool
        yield d
        mysql_mod.aiomysql = orig

    @pytest.fixture
    def driver_no_pool(self):
        from auniqorm.drivers import mysql as mysql_mod

        orig = mysql_mod.aiomysql
        mysql_mod.aiomysql = MagicMock()
        d = mysql_mod.MySQLDriver("mysql://localhost/db")
        mysql_mod.aiomysql = orig
        d._pool = None
        yield d

    async def test_is_connected(self, driver):
        assert driver.is_connected is True
        driver._pool = None
        assert driver.is_connected is False

    async def test_disconnect(self, driver):
        pool = driver._pool
        await driver.disconnect()
        pool.close.assert_called_once()
        pool.wait_closed.assert_awaited_once()
        assert driver._pool is None

    async def test_disconnect_when_not_connected(self, driver_no_pool):
        await driver_no_pool.disconnect()  # no-op

    # -- URL parsing --

    def test_parse_url_defaults(self, driver_no_pool):
        driver_no_pool._url = "mysql://localhost/mydb"
        params = driver_no_pool._parse_url()
        assert params["host"] == "localhost"
        assert params["port"] == 3306
        assert params["db"] == "mydb"

    def test_parse_url_with_credentials(self, driver_no_pool):
        driver_no_pool._url = "mysql://admin:secret@dbhost:3307/prod"
        params = driver_no_pool._parse_url()
        assert params["user"] == "admin"
        assert params["password"] == "secret"
        assert params["host"] == "dbhost"
        assert params["port"] == 3307
        assert params["db"] == "prod"

    def test_parse_url_with_query_params(self, driver_no_pool):
        driver_no_pool._url = "mysql://localhost/db?charset=utf8mb4"
        params = driver_no_pool._parse_url()
        assert params["charset"] == "utf8mb4"

    # -- not connected guards --

    @pytest.mark.parametrize(("method", "args"), NOT_CONNECTED_CALLS)
    async def test_not_connected(self, driver_no_pool, method, args):
        with pytest.raises(QueryError, match="Not connected"):
            await _call(driver_no_pool, method, *args)

    @pytest.mark.parametrize("method", STREAM_CALLS)
    async def test_stream_not_connected(self, driver_no_pool, method):
        with pytest.raises(QueryError, match="Not connected"):
            await _call(driver_no_pool, method, "SELECT 1")

    @pytest.mark.parametrize(
        ("method", "args", "cursor_method", "message"),
        [
            ("execute", ("BAD",), "execute", "Execute failed"),
            ("execute_many", ("BAD", [[1]]), "executemany", "Execute many failed"),
            ("fetch_one", ("BAD",), "execute", "Fetch one failed"),
            ("fetch_all", ("BAD",), "execute", "Fetch all failed"),
            ("fetch_val", ("BAD",), "execute", "Fetch val failed"),
        ],
    )
    async def test_error_wraps(
        self, driver, mysql_cursor, method, args, cursor_method, message
    ):
        setattr(
            mysql_cursor, cursor_method, AsyncMock(side_effect=RuntimeError("boom"))
        )
        with pytest.raises(QueryError, match=message):
            await _call(driver, method, *args)

    # -- execute --

    async def test_execute(self, driver):
        result = await driver.execute("INSERT INTO t VALUES (%s)", [1])
        assert result == 1  # lastrowid

    # -- execute_many --

    async def test_execute_many(self, driver):
        await driver.execute_many("INSERT INTO t VALUES (%s)", [[1], [2]])

    # -- fetch_one --

    async def test_fetch_one(self, driver):
        row = await driver.fetch_one("SELECT * FROM t WHERE id=%s", [1])
        assert row == {"id": 1}

    # -- fetch_all --

    async def test_fetch_all(self, driver):
        rows = await driver.fetch_all("SELECT * FROM t")
        assert rows == [{"id": 1}]

    @pytest.mark.parametrize("mysql_cursor", [{"fetchall": None}], indirect=True)
    async def test_fetch_all_returns_empty(self, driver):
        rows = await driver.fetch_all("SELECT * FROM empty")
        assert rows == []

    # -- fetch_val --

    @pytest.mark.parametrize("mysql_cursor", [{"fetchone": (42, "x")}], indirect=True)
    async def test_fetch_val(self, driver):
        val = await driver.fetch_val("SELECT count(*) FROM t")
        assert val == 42

    @pytest.mark.parametrize("mysql_cursor", [{"fetchone": None}], indirect=True)
    async def test_fetch_val_none(self, driver):
        val = await driver.fetch_val("SELECT * FROM empty")
        assert val is None

    # -- transactions --

    @pytest.mark.parametrize("method", ["commit", "rollback"])
    async def test_end_transaction(self, driver, mysql_conn, method):
        driver._open_scope((mysql_conn, None), began=True)
        await getattr(driver, method)()
        getattr(mysql_conn, method).assert_awaited_once()
        assert driver._scope() is None

    @pytest.mark.parametrize("method", ["commit", "rollback"])
    async def test_end_without_transaction_is_noop(self, driver, method):
        await getattr(driver, method)()

    # -- transaction conn used for queries --

    async def test_execute_in_transaction(self, driver, mysql_conn):
        driver._open_scope((mysql_conn, None), began=True)
        result = await driver.execute("INSERT INTO t VALUES (%s)", [1])
        assert result == 1

    async def test_execute_many_in_transaction(self, driver, mysql_conn):
        driver._open_scope((mysql_conn, None), began=True)
        await driver.execute_many("INSERT INTO t VALUES (%s)", [[1], [2]])

    async def test_fetch_one_in_transaction(self, driver, mysql_conn):
        driver._open_scope((mysql_conn, None), began=True)
        row = await driver.fetch_one("SELECT * FROM t WHERE id=%s", [1])
        assert row == {"id": 1}

    async def test_fetch_all_in_transaction(self, driver, mysql_conn):
        driver._open_scope((mysql_conn, None), began=True)
        rows = await driver.fetch_all("SELECT * FROM t")
        assert rows == [{"id": 1}]

    @pytest.mark.parametrize("mysql_cursor", [{"fetchone": (99,)}], indirect=True)
    async def test_fetch_val_in_transaction(self, driver, mysql_conn):
        driver._open_scope((mysql_conn, None), began=True)
        val = await driver.fetch_val("SELECT count(*) FROM t")
        assert val == 99

    # -- iterate --

    async def test_iterate_yields_rows(self, driver, mysql_conn):
        """iterate() uses SSDictCursor and fetchmany."""
        from contextlib import asynccontextmanager

        batches = [[{"id": 1}, {"id": 2}], [{"id": 3}], []]
        batch_iter = iter(batches)

        ss_cursor = AsyncMock()
        ss_cursor.execute = AsyncMock()
        ss_cursor.fetchmany = AsyncMock(side_effect=lambda n: next(batch_iter))

        @asynccontextmanager
        async def _cursor(cursor_cls=None):
            yield ss_cursor

        mysql_conn.cursor = _cursor

        rows = []
        async for row in driver.iterate("SELECT * FROM t", [1]):
            rows.append(row)
        assert rows == [{"id": 1}, {"id": 2}, {"id": 3}]

    async def test_iterate_in_transaction(self, driver, mysql_conn):
        """iterate() uses transaction_conn when in a transaction."""
        from contextlib import asynccontextmanager

        batches = [[{"id": 1}], []]
        batch_iter = iter(batches)

        ss_cursor = AsyncMock()
        ss_cursor.execute = AsyncMock()
        ss_cursor.fetchmany = AsyncMock(side_effect=lambda n: next(batch_iter))

        @asynccontextmanager
        async def _cursor(cursor_cls=None):
            yield ss_cursor

        mysql_conn.cursor = _cursor
        driver._open_scope((mysql_conn, None), began=True)

        rows = []
        async for row in driver.iterate("SELECT 1"):
            rows.append(row)
        assert rows == [{"id": 1}]

    @pytest.mark.parametrize("method", STREAM_CALLS)
    async def test_stream_error_wraps(self, driver, mysql_conn, method):
        ss_cursor = AsyncMock()
        ss_cursor.execute = AsyncMock(side_effect=RuntimeError("boom"))

        @asynccontextmanager
        async def _cursor(cursor_cls=None):
            yield ss_cursor

        mysql_conn.cursor = _cursor

        with pytest.raises(QueryError, match="Iterate failed"):
            await _call(driver, method, "BAD")

    # -- iterate_batches --

    async def test_iterate_batches_yields_chunks(self, driver, mysql_conn):
        from contextlib import asynccontextmanager

        batches = [[{"id": 1}, {"id": 2}], [{"id": 3}], []]
        batch_iter = iter(batches)

        ss_cursor = AsyncMock()
        ss_cursor.execute = AsyncMock()
        ss_cursor.fetchmany = AsyncMock(side_effect=lambda n: next(batch_iter))

        @asynccontextmanager
        async def _cursor(cursor_cls=None):
            yield ss_cursor

        mysql_conn.cursor = _cursor

        results = []
        async for batch in driver.iterate_batches("SELECT * FROM t", batch_size=2):
            results.append(batch)
        assert results == [[{"id": 1}, {"id": 2}], [{"id": 3}]]

    async def test_iterate_batches_in_transaction(self, driver, mysql_conn):
        from contextlib import asynccontextmanager

        batches = [[{"id": 1}], []]
        batch_iter = iter(batches)

        ss_cursor = AsyncMock()
        ss_cursor.execute = AsyncMock()
        ss_cursor.fetchmany = AsyncMock(side_effect=lambda n: next(batch_iter))

        @asynccontextmanager
        async def _cursor(cursor_cls=None):
            yield ss_cursor

        mysql_conn.cursor = _cursor
        driver._open_scope((mysql_conn, None), began=True)

        results = []
        async for batch in driver.iterate_batches("SELECT 1"):
            results.append(batch)
        assert results == [[{"id": 1}]]


# ---------------------------------------------------------------------------
# _init_connection (PostgreSQL JSON codec setup)
# ---------------------------------------------------------------------------


class TestInitConnection:
    async def test_registers_json_codecs(self):
        from auniqorm.drivers.postgresql import _init_connection

        conn = AsyncMock()
        await _init_connection(conn)
        assert conn.set_type_codec.await_count == 2
        calls = [c.kwargs["schema"] for c in conn.set_type_codec.await_args_list]
        assert all(s == "pg_catalog" for s in calls)


# ---------------------------------------------------------------------------
# ImportError guards (aiosqlite / asyncpg / aiomysql missing)
# ---------------------------------------------------------------------------


class TestImportGuards:
    def test_sqlite_missing_aiosqlite(self):
        from auniqorm.drivers import sqlite as sqlite_mod

        orig = sqlite_mod.aiosqlite
        sqlite_mod.aiosqlite = None
        try:
            with pytest.raises(ImportError, match="aiosqlite is required"):
                sqlite_mod.SQLiteDriver("sqlite://:memory:")
        finally:
            sqlite_mod.aiosqlite = orig

    def test_pg_missing_asyncpg(self):
        from auniqorm.drivers import postgresql as pg_mod

        orig = pg_mod.asyncpg
        pg_mod.asyncpg = None
        try:
            with pytest.raises(ImportError, match="asyncpg is required"):
                pg_mod.PostgresAsyncpgDriver("postgresql://localhost/db")
        finally:
            pg_mod.asyncpg = orig

    def test_mysql_missing_aiomysql(self):
        from auniqorm.drivers import mysql as mysql_mod

        orig = mysql_mod.aiomysql
        mysql_mod.aiomysql = None
        try:
            with pytest.raises(ImportError, match="aiomysql is required"):
                mysql_mod.MySQLDriver("mysql://localhost/db")
        finally:
            mysql_mod.aiomysql = orig


# ---------------------------------------------------------------------------
# SQLite driver: error paths and not-connected guards
# ---------------------------------------------------------------------------


class TestSQLiteDriver:
    @pytest.fixture
    def driver(self):
        from auniqorm.drivers.sqlite import SQLiteDriver

        d = SQLiteDriver("sqlite://:memory:")
        d._db = None  # force disconnected
        yield d

    @pytest.fixture
    def connected_driver(self):
        from auniqorm.drivers.sqlite import SQLiteDriver

        d = SQLiteDriver("sqlite://:memory:")
        d._db = AsyncMock()
        yield d

    # -- _get_db_path additional branches --

    @pytest.mark.parametrize(
        ("url", "expected"),
        [
            ("sqlite://///absolute/path/db.sqlite", "absolute/path/db.sqlite"),
            ("sqlite:///./relative/db.sqlite", "relative/db.sqlite"),
            ("sqlite:", ""),
            ("sqlite:///:memory:", ":memory:"),
        ],
    )
    def test_get_db_path(self, driver, url, expected):
        driver._url = url
        assert driver._get_db_path() == expected

    # -- not-connected guards --

    @pytest.mark.parametrize(("method", "args"), NOT_CONNECTED_CALLS)
    async def test_not_connected(self, driver, method, args):
        with pytest.raises(QueryError, match="Not connected"):
            await _call(driver, method, *args)

    # -- exception wrapping --

    @pytest.mark.parametrize(
        ("method", "args", "db_method", "message"),
        [
            ("execute", ("BAD SQL",), "execute", "Execute failed"),
            ("execute_many", ("BAD", [[1]]), "executemany", "Execute many failed"),
            ("fetch_one", ("BAD",), "execute", "Fetch one failed"),
            ("fetch_all", ("BAD",), "execute", "Fetch all failed"),
            ("fetch_val", ("BAD",), "execute", "Fetch val failed"),
        ],
    )
    async def test_error_wraps(
        self, connected_driver, method, args, db_method, message
    ):
        setattr(
            connected_driver._db, db_method, AsyncMock(side_effect=RuntimeError("oops"))
        )
        with pytest.raises(QueryError, match=message):
            await _call(connected_driver, method, *args)

    # -- fetch_val returns None --

    async def test_fetch_val_returns_none(self, connected_driver):
        cursor = AsyncMock()
        cursor.fetchone = AsyncMock(return_value=None)
        connected_driver._db.execute = AsyncMock(return_value=cursor)
        val = await connected_driver.fetch_val("SELECT * FROM empty")
        assert val is None

    # -- connect already connected --

    async def test_connect_already_connected(self, connected_driver):
        db_ref = connected_driver._db
        await connected_driver.connect()
        assert connected_driver._db is db_ref  # unchanged


# ---------------------------------------------------------------------------
# PostgreSQL connect() path
# ---------------------------------------------------------------------------


class TestPostgresConnect:
    async def test_connect_creates_pool(self):
        from auniqorm.drivers import postgresql as pg_mod

        mock_asyncpg = MagicMock()
        mock_pool = AsyncMock()
        mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)

        orig = pg_mod.asyncpg
        pg_mod.asyncpg = mock_asyncpg
        try:
            d = pg_mod.PostgresAsyncpgDriver("postgresql://user:pw@host/db")
            await d.connect()
            assert d._pool is mock_pool
        finally:
            pg_mod.asyncpg = orig


# ---------------------------------------------------------------------------
# MySQL connect() and begin() paths
# ---------------------------------------------------------------------------


class TestMySQLConnect:
    async def test_connect_creates_pool(self):
        from auniqorm.drivers import mysql as mysql_mod

        mock_aiomysql = MagicMock()
        mock_pool = AsyncMock()
        mock_aiomysql.create_pool = AsyncMock(return_value=mock_pool)

        orig = mysql_mod.aiomysql
        mysql_mod.aiomysql = mock_aiomysql
        try:
            d = mysql_mod.MySQLDriver("mysql://user:pw@host:3306/db")
            await d.connect()
            assert d._pool is mock_pool
        finally:
            mysql_mod.aiomysql = orig

    async def test_connect_already_connected(self):
        from auniqorm.drivers import mysql as mysql_mod

        mock_aiomysql = MagicMock()
        orig = mysql_mod.aiomysql
        mysql_mod.aiomysql = mock_aiomysql
        try:
            d = mysql_mod.MySQLDriver("mysql://localhost/db")
            d._pool = MagicMock()  # pretend already connected
            await d.connect()
            # create_pool should NOT have been called
            mock_aiomysql.create_pool.assert_not_called()
        finally:
            mysql_mod.aiomysql = orig

    @pytest.mark.parametrize(
        ("url", "options", "expected"),
        [
            ("mysql://localhost/db", {}, True),
            ("mysql://localhost/db?autocommit=false", {}, False),
            ("mysql://localhost/db?autocommit=1", {}, True),
            ("mysql://localhost/db?autocommit=true", {"autocommit": False}, False),
        ],
    )
    async def test_pool_autocommits_by_default(self, url, options, expected):
        # Without autocommit, pooled reads leave InnoDB transactions open that
        # keep stale snapshots and block DDL after returning to the pool.
        from auniqorm.drivers import mysql as mysql_mod

        mock_aiomysql = MagicMock()
        mock_aiomysql.create_pool = AsyncMock(return_value=AsyncMock())
        orig = mysql_mod.aiomysql
        mysql_mod.aiomysql = mock_aiomysql
        try:
            d = mysql_mod.MySQLDriver(url, **options)
            await d.connect()
        finally:
            mysql_mod.aiomysql = orig
        kwargs = mock_aiomysql.create_pool.await_args.kwargs
        assert kwargs["autocommit"] is expected
        assert d._autocommit is expected

    @pytest.mark.parametrize("autocommit", [True, False])
    async def test_pooled_writes_commit_only_without_autocommit(
        self, mysql_pool, mysql_conn, autocommit
    ):
        from auniqorm.drivers import mysql as mysql_mod

        orig = mysql_mod.aiomysql
        mysql_mod.aiomysql = MagicMock()
        try:
            driver = mysql_mod.MySQLDriver("mysql://localhost/db")
        finally:
            mysql_mod.aiomysql = orig
        driver._pool = mysql_pool
        driver._autocommit = autocommit
        await driver.execute("INSERT INTO t VALUES (%s)", [1])
        await driver.execute_many("INSERT INTO t VALUES (%s)", [[1], [2]])
        assert mysql_conn.commit.await_count == (0 if autocommit else 2)

    async def test_begin_acquires_conn(self, mysql_conn):
        from auniqorm.drivers import mysql as mysql_mod

        mock_pool = MagicMock()
        mock_pool.acquire = AsyncMock(return_value=mysql_conn)

        orig = mysql_mod.aiomysql
        mysql_mod.aiomysql = MagicMock()
        d = mysql_mod.MySQLDriver("mysql://localhost/db")
        mysql_mod.aiomysql = orig
        d._pool = mock_pool

        await d.begin()
        assert d._scope().conn is mysql_conn
        mysql_conn.begin.assert_awaited_once()
