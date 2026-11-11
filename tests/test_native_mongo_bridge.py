# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Integration tests for the native completion bridge with MongoDB.

They exercise ``auniqorm_native.Bridge`` through the native MongoDB engine:
results of many concurrent operations reach the right callers, errors and
cancellation are reported correctly, and engines keep working when used
from another event loop. They are skipped unless the native extension is
built with the bridge and a MongoDB server is reachable (the benchmark
compose stack: ``mongodb://localhost:27017``).
"""

from __future__ import annotations

import asyncio
import threading

import pytest

auniqorm_native = pytest.importorskip("auniqorm_native")

from auniqorm import Database, QueryError  # noqa: E402
from auniqorm.sync import SyncDatabase  # noqa: E402

from ._services import MONGO_URL  # noqa: E402

pytestmark = [
    pytest.mark.needs_mongo,
    pytest.mark.skipif(
        not hasattr(auniqorm_native, "Bridge"), reason="native bridge not built"
    ),
]


@pytest.fixture
async def db():
    async with Database(MONGO_URL) as database:
        assert database._driver._bridge is not None
        await database.collection("items").drop()
        yield database
        await database.collection("items").drop()
        assert database._driver._bridge[1].pending == 0


async def test_concurrent_results_reach_their_callers(db) -> None:
    items = db.collection("items")
    await items.insert_many([{"_id": i, "value": i * 10} for i in range(200)])

    # Many rounds of overlapping completions guard against lost wake-ups.
    for _ in range(20):
        rows = await asyncio.wait_for(
            asyncio.gather(*(items.find_one({"_id": i}) for i in range(200))), 10
        )
        assert [row["value"] for row in rows] == [i * 10 for i in range(200)]


async def test_errors_are_raised_to_the_caller(db) -> None:
    items = db.collection("items")
    await items.insert_one({"_id": 1})

    results = await asyncio.gather(
        items.insert_one({"_id": 1}),
        items.find_one({"_id": 1}),
        return_exceptions=True,
    )

    assert isinstance(results[0], QueryError)
    assert "E11000" in str(results[0])
    assert results[1] == {"_id": 1}


async def test_cancelled_operations_do_not_break_the_bridge(db) -> None:
    items = db.collection("items")
    await items.insert_many([{"_id": i} for i in range(50)])

    tasks = [asyncio.ensure_future(items.find({})) for _ in range(20)]
    await asyncio.sleep(0)
    for task in tasks[::2]:
        task.cancel()
    results = await asyncio.gather(*tasks, return_exceptions=True)

    assert all(isinstance(r, asyncio.CancelledError) for r in results[::2])
    assert all(len(r) == 50 for r in results[1::2])
    for _ in range(100):
        if db._driver._bridge[1].pending == 0:
            break
        await asyncio.sleep(0.01)
    assert await items.count_documents() == 50


async def test_streaming_and_transactions_use_the_bridge(db) -> None:
    items = db.collection("items")
    await items.insert_many([{"_id": i} for i in range(2500)])

    seen = [row["_id"] async for row in items.iterate(sort="_id", batch_size=300)]
    assert seen == list(range(2500))

    async with await db.transaction():
        await items.update_one({"_id": 0}, set={"touched": True})
        assert (await items.find_one({"_id": 0}))["touched"] is True


async def test_engine_works_from_another_event_loop(db) -> None:
    items = db.collection("items")
    await items.insert_one({"_id": 7})
    native = db._driver._native
    result: dict[str, object] = {}

    def other_loop() -> None:
        async def call() -> bytes:
            encoded, _ = await native.fetch_bson(
                __import__("bson").encode(
                    {
                        "operation": "find_one",
                        "collection": "items",
                        "filter": {"_id": 7},
                    }
                )
            )
            return encoded

        result["encoded"] = asyncio.run(call())

    thread = threading.Thread(target=other_loop)
    thread.start()
    await asyncio.to_thread(thread.join)

    assert __import__("bson").decode(result["encoded"]) == {"_id": 7}


def test_sync_database_uses_the_bridge() -> None:
    with SyncDatabase(MONGO_URL) as db:
        items = db.collection("sync_items")
        items.drop()
        items.insert_many([{"_id": i} for i in range(10)])
        assert items.count_documents() == 10
        assert db.async_database._driver._bridge is not None
        items.drop()


async def test_concurrent_transactions_are_independent(db) -> None:
    items = db.collection("items")
    await items.insert_many([{"_id": i, "n": 0} for i in range(3)])
    both_open = asyncio.Barrier(3)

    async def commit_one() -> None:
        async with await db.transaction():
            await items.update_one({"_id": 0}, inc={"n": 1})
            await both_open.wait()

    async def roll_back_one() -> None:
        try:
            async with await db.transaction():
                await items.update_one({"_id": 1}, inc={"n": 1})
                await both_open.wait()
                raise RuntimeError("roll back")
        except RuntimeError:
            pass

    async def outside() -> None:
        await items.update_one({"_id": 2}, inc={"n": 1})
        await both_open.wait()
        assert not db._driver.in_transaction

    await asyncio.wait_for(asyncio.gather(commit_one(), roll_back_one(), outside()), 10)

    assert [row["n"] for row in await items.find(sort="_id")] == [1, 0, 1]


async def test_tasks_outliving_a_transaction_cannot_reuse_it() -> None:
    database = Database(MONGO_URL)
    await database.connect()
    items = database.collection("items")
    await items.drop()
    go = asyncio.Event()

    async def late_write() -> None:
        await go.wait()
        await items.insert_one({"_id": "late"})

    async with await database.transaction():
        await items.insert_one({"_id": "inside"})
        straggler = asyncio.create_task(late_write())
    go.set()

    with pytest.raises(QueryError, match="already ended"):
        await straggler
    assert [row["_id"] for row in await items.find()] == ["inside"]
    await items.drop()
    # The finished transaction's handle is still referenced by the task's
    # context; disconnecting must not wait for it.
    await asyncio.wait_for(database.disconnect(), 10)
