"""Transactions and pinned connections belong to the task that opened them.

Runs against every SQL driver: aiosqlite and native SQLite always, and the
asyncpg, native PostgreSQL, aiomysql and native MySQL drivers when the
benchmark compose services are reachable (``benchmarks/compose.yaml``).
"""

from __future__ import annotations

import asyncio
import importlib.util
import socket
import threading
import time
import uuid

import pytest

from auniqorm import Database
from auniqorm.exceptions import ConnectionError, QueryError
from auniqorm.sync import SyncDatabase

PG_URL = "postgresql://bench:bench@localhost:5432/bench"
MYSQL_URL = "mysql://bench:bench@localhost:3306/bench"

# (id, url or None for a temporary SQLite file, env switch, native, module, port)
DRIVERS = [
    ("sqlite", None, "AUNIQORM_SQLITE_NATIVE", False, "aiosqlite", None),
    ("sqlite-native", None, "AUNIQORM_SQLITE_NATIVE", True, "auniqorm_native", None),
    ("asyncpg", PG_URL, "AUNIQORM_PG_NATIVE", False, "asyncpg", 5432),
    ("postgres-native", PG_URL, "AUNIQORM_PG_NATIVE", True, "auniqorm_native", 5432),
    ("aiomysql", MYSQL_URL, "AUNIQORM_MYSQL_NATIVE", False, "aiomysql", 3306),
    ("mysql-native", MYSQL_URL, "AUNIQORM_MYSQL_NATIVE", True, "auniqorm_native", 3306),
]


def _reachable(port: int) -> bool:
    try:
        socket.create_connection(("localhost", port), timeout=0.5).close()
    except OSError:
        return False
    return True


def _configure(param, monkeypatch, tmp_path) -> str:
    _, url, env, native, module, port = param
    if importlib.util.find_spec(module) is None:
        pytest.skip(f"{module} is not installed")
    if port is not None and not _reachable(port):
        pytest.skip(f"no database server on localhost:{port}")
    monkeypatch.setenv(env, "1" if native else "0")
    return url or f"sqlite:///{tmp_path / 'isolation.db'}"


@pytest.fixture(params=DRIVERS, ids=[driver[0] for driver in DRIVERS])
async def db(request, monkeypatch, tmp_path):
    url = _configure(request.param, monkeypatch, tmp_path)
    database = Database(url, models=False)
    try:
        await database.connect()
    except ConnectionError as exc:
        pytest.skip(f"cannot connect: {exc}")
    is_native = "Native" in type(database._driver).__name__
    assert is_native == request.param[3], type(database._driver).__name__
    table = f"txn_iso_{uuid.uuid4().hex[:10]}"
    await database.execute(
        f"CREATE TABLE {table} (id INT PRIMARY KEY, label VARCHAR(20))"
    )
    try:
        yield database, table
    finally:
        await database.execute(f"DROP TABLE {table}")
        assert not database._driver._live_scopes
        await database.disconnect()


async def _insert(database: Database, table: str, row_id: int, label: str) -> None:
    await database.execute(
        f"INSERT INTO {table} (id, label) VALUES (?, ?)",
        [row_id, label],
        paramstyle="qmark",
    )


async def _labels(database: Database, table: str) -> list[str]:
    rows = await database.fetch_all(f"SELECT label FROM {table} ORDER BY id")
    return [row["label"] for row in rows]


async def test_other_tasks_neither_join_nor_see_an_open_transaction(db) -> None:
    database, table = db
    started = asyncio.Event()
    seen: dict[str, int] = {}

    async def rolled_back() -> None:
        with pytest.raises(RuntimeError, match="roll back"):
            async with await database.transaction():
                await _insert(database, table, 1, "A")
                started.set()
                await asyncio.sleep(0.1)
                raise RuntimeError("roll back")

    async def outside() -> None:
        await started.wait()
        assert not database._driver.in_transaction
        await _insert(database, table, 2, "C")
        seen["A"] = await database.fetch_val(
            f"SELECT COUNT(*) FROM {table} WHERE label = ?", ["A"], paramstyle="qmark"
        )

    await asyncio.wait_for(asyncio.gather(rolled_back(), outside()), 10)

    assert seen["A"] == 0
    assert await _labels(database, table) == ["C"]


async def test_concurrent_transactions_commit_and_roll_back_independently(db) -> None:
    database, table = db

    async def commit_one() -> None:
        async with await database.transaction():
            await _insert(database, table, 1, "A")
            await asyncio.sleep(0.05)

    async def roll_back_one() -> None:
        with pytest.raises(RuntimeError, match="roll back"):
            async with await database.transaction():
                await _insert(database, table, 2, "B")
                await asyncio.sleep(0.05)
                raise RuntimeError("roll back")

    await asyncio.wait_for(asyncio.gather(commit_one(), roll_back_one()), 10)

    assert await _labels(database, table) == ["A"]


async def test_many_concurrent_transactions_release_their_connections(db) -> None:
    database, table = db

    async def one(row_id: int) -> None:
        async with await database.transaction():
            await _insert(database, table, row_id, f"r{row_id}")
            await asyncio.sleep(0)
            assert database._driver.in_transaction

    await asyncio.wait_for(asyncio.gather(*(one(i) for i in range(20))), 20)

    assert len(await _labels(database, table)) == 20
    assert not database._driver._live_scopes


async def test_tasks_outliving_a_transaction_cannot_reuse_it(db) -> None:
    database, table = db
    go = asyncio.Event()

    async def late_write() -> None:
        await go.wait()
        await _insert(database, table, 9, "late")

    async with await database.transaction():
        await _insert(database, table, 1, "inside")
        straggler = asyncio.create_task(late_write())
    go.set()

    with pytest.raises(QueryError, match="already ended"):
        await straggler
    assert await _labels(database, table) == ["inside"]


async def test_nested_transactions_use_savepoints(db) -> None:
    database, table = db

    async with await database.transaction():
        await _insert(database, table, 1, "outer")
        with pytest.raises(RuntimeError, match="inner"):
            async with await database.transaction():
                await _insert(database, table, 2, "inner")
                raise RuntimeError("inner")
        async with await database.transaction():
            await _insert(database, table, 3, "kept")
        assert database._driver.in_transaction

    assert not database._driver.in_transaction
    assert await _labels(database, table) == ["outer", "kept"]


async def test_streaming_respects_transaction_scope(db) -> None:
    database, table = db
    query = f"SELECT label FROM {table} ORDER BY id"
    for row_id in range(5):
        await _insert(database, table, row_id, f"r{row_id}")

    async with await database.transaction():
        await _insert(database, table, 10, "mine")
        rows = [row["label"] async for row in database.iterate(query, batch_size=2)]
        assert rows[-1] == "mine"
        batches = [
            batch async for batch in database.iterate_batches(query, batch_size=2)
        ]
        assert [len(batch) for batch in batches] == [2, 2, 2]

    started = asyncio.Event()

    async def other() -> None:
        with pytest.raises(RuntimeError, match="roll back"):
            async with await database.transaction():
                await _insert(database, table, 20, "other")
                started.set()
                await asyncio.sleep(0.05)
                raise RuntimeError("roll back")

    async def stream() -> list[str]:
        await started.wait()
        labels = [row["label"] async for row in database.iterate(query, batch_size=2)]
        labels += [
            row["label"]
            async for batch in database.iterate_batches(query, batch_size=4)
            for row in batch
        ]
        return labels

    _, labels = await asyncio.wait_for(asyncio.gather(other(), stream()), 10)
    expected = ["r0", "r1", "r2", "r3", "r4", "mine"]
    assert labels == expected * 2


async def test_cancelled_transaction_rolls_back_and_releases(db) -> None:
    database, table = db
    entered = asyncio.Event()

    async def work() -> None:
        async with await database.transaction():
            await _insert(database, table, 1, "cancelled")
            entered.set()
            await asyncio.sleep(10)

    task = asyncio.create_task(work())
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert not database._driver._live_scopes
    async with await database.transaction():
        await _insert(database, table, 2, "after")
    assert await _labels(database, table) == ["after"]


async def test_pinned_connection_scopes_its_transactions(db) -> None:
    database, table = db

    async with await database.connection():
        assert not database._driver.in_transaction
        await _insert(database, table, 1, "pinned")
        async with await database.transaction():
            assert database._driver.in_transaction
            await _insert(database, table, 2, "committed")
        assert not database._driver.in_transaction
        with pytest.raises(RuntimeError, match="roll back"):
            async with await database.transaction():
                await _insert(database, table, 3, "rolled back")
                raise RuntimeError("roll back")
        await _insert(database, table, 4, "after")

    assert not database._driver._live_scopes
    assert await _labels(database, table) == ["pinned", "committed", "after"]


@pytest.mark.parametrize("param", DRIVERS, ids=[driver[0] for driver in DRIVERS])
def test_sync_threads_have_independent_transactions(
    param, monkeypatch, tmp_path
) -> None:
    url = _configure(param, monkeypatch, tmp_path)
    table = f"txn_sync_{uuid.uuid4().hex[:10]}"
    insert = f"INSERT INTO {table} (id, label) VALUES (?, ?)"
    begun = threading.Event()
    seen: dict[str, int] = {}
    errors: list[BaseException] = []

    with SyncDatabase(url, models=False) as sdb:
        sdb.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, label VARCHAR(20))")

        def rolled_back() -> None:
            try:
                with sdb.transaction():
                    sdb.execute(insert, [1, "A"], paramstyle="qmark")
                    begun.set()
                    time.sleep(0.2)
                    raise RuntimeError("roll back")
            except RuntimeError:
                pass
            except BaseException as exc:  # pragma: no cover - reported below
                errors.append(exc)

        def outside() -> None:
            try:
                assert begun.wait(5)
                sdb.execute(insert, [2, "B"], paramstyle="qmark")
                seen["A"] = sdb.fetch_val(
                    f"SELECT COUNT(*) FROM {table} WHERE label = ?",
                    ["A"],
                    paramstyle="qmark",
                )
            except BaseException as exc:  # pragma: no cover - reported below
                errors.append(exc)

        threads = [
            threading.Thread(target=rolled_back),
            threading.Thread(target=outside),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)

        try:
            assert not errors, errors
            assert seen["A"] == 0
            rows = sdb.fetch_all(f"SELECT label FROM {table} ORDER BY id")
            assert [row["label"] for row in rows] == ["B"]
        finally:
            sdb.execute(f"DROP TABLE {table}")
