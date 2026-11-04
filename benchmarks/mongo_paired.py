"""Paired comparison of aUniqORM's MongoDB collection API with PyMongo.

Each scenario runs as alternating blocks: one block for aUniqORM, one for
PyMongo, with the order swapped every round. A block times the same
operations with each library's natural API, and the paired differences
between neighbouring blocks cancel slow drift on the host. The report shows
medians per operation, the median paired difference, and how many rounds
each library won, so noise cannot masquerade as a win.

Scenarios run in three modes:

``sequential``  operations awaited back to back
``idle``        sequential with an idle gap between operations, like a request
                handler waiting for its next request; only operation time counts
``concurrent``  32 operations in flight at a time; reports time per operation

    docker compose -f benchmarks/compose.yaml up -d --wait mongodb
    python benchmarks/mongo_paired.py

Environment variables:

``AUNIQORM_PAIRED_ROUNDS``     rounds per scenario (default: 10)
``AUNIQORM_PAIRED_OPS``        operations per block (default: 200)
``AUNIQORM_PAIRED_MODES``      comma-separated modes (default: all)
``AUNIQORM_PAIRED_SCENARIOS``  comma-separated scenarios (default: all)
``AUNIQORM_PAIRED_GAP_MS``     idle gap in milliseconds (default: 2)
"""

from __future__ import annotations

import asyncio
import os
import statistics
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from pymongo import AsyncMongoClient

from auniqorm import Database

MONGO_URL = os.getenv("AUNIQORM_MONGO_BENCH_URL", "mongodb://localhost:27017")
DATABASE_NAME = "auniqorm_paired"
ROUNDS = int(os.getenv("AUNIQORM_PAIRED_ROUNDS", "10"))
OPS = int(os.getenv("AUNIQORM_PAIRED_OPS", "200"))
GAP = float(os.getenv("AUNIQORM_PAIRED_GAP_MS", "2")) / 1000
CONCURRENCY = 32
DOCUMENTS = 1000
MODES = ("sequential", "idle", "concurrent")

Operation = Callable[[int], Awaitable[Any]]


def small(index: int) -> dict[str, Any]:
    return {
        "email": f"user-{index}@example.com",
        "name": f"User {index}",
        "score": index % 100,
        "visits": 0,
        "tags": ["a", "b"],
    }


def large(index: int) -> dict[str, Any]:
    document = small(index)
    document["history"] = [
        {"at": step, "event": f"event-{step}", "value": step * 1.5, "ok": True}
        for step in range(150)
    ]
    return document


@dataclass(frozen=True, slots=True)
class Scenario:
    name: str
    ours: Operation
    theirs: Operation
    per_block: int = OPS


async def build(db: Database, client: AsyncMongoClient[Any]) -> list[Scenario]:
    ours_small, ours_large = db.collection("small"), db.collection("large")
    theirs_small = client[DATABASE_NAME]["small"]
    theirs_large = client[DATABASE_NAME]["large"]
    ours_insert, theirs_insert = (
        db.collection("inserted"),
        client[DATABASE_NAME]["inserted"],
    )
    projection = {"_id": 0, "email": 1, "score": 1}

    def email(index: int) -> str:
        return f"user-{index % DOCUMENTS}@example.com"

    async def transaction_ours(index: int) -> None:
        async with await db.transaction():
            for step in range(10):
                await ours_small.update_one(
                    {"email": email(index * 10 + step)}, inc={"visits": 1}
                )

    async def transaction_theirs(index: int) -> None:
        session = client.start_session()
        await session.start_transaction()
        for step in range(10):
            await theirs_small.update_one(
                {"email": email(index * 10 + step)},
                {"$inc": {"visits": 1}},
                session=session,
            )
        await session.commit_transaction()
        await session.end_session()

    group = [{"$group": {"_id": "$score", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]
    return [
        Scenario(
            "point read",
            lambda i: ours_small.find_one({"email": email(i)}),
            lambda i: theirs_small.find_one({"email": email(i)}),
        ),
        Scenario(
            "point read, 10 KB",
            lambda i: ours_large.find_one({"email": email(i)}),
            lambda i: theirs_large.find_one({"email": email(i)}),
        ),
        Scenario(
            "filtered read",
            lambda i: ours_small.find({"score": i % 100}, projection=projection),
            lambda i: theirs_small.find({"score": i % 100}, projection).to_list(),
        ),
        Scenario(
            "sorted find, limit 5",
            lambda i: ours_small.find({"score": i % 100}, sort="email", limit=5),
            lambda i: theirs_small.find(
                {"score": i % 100}, sort=[("email", 1)], limit=5
            ).to_list(),
        ),
        Scenario(
            "read 1,000 documents",
            lambda i: ours_small.find({}),
            lambda i: theirs_small.find({}).to_list(),
            per_block=max(OPS // 20, 5),
        ),
        Scenario(
            "insert_one",
            lambda i: ours_insert.insert_one(small(i)),
            lambda i: theirs_insert.insert_one(small(i)),
        ),
        Scenario(
            "insert_one, 10 KB",
            lambda i: ours_insert.insert_one(large(i)),
            lambda i: theirs_insert.insert_one(large(i)),
        ),
        Scenario(
            "update_one",
            lambda i: ours_small.update_one(
                {"email": email(i)}, set={"name": "Updated"}, inc={"visits": 1}
            ),
            lambda i: theirs_small.update_one(
                {"email": email(i)},
                {"$set": {"name": "Updated"}, "$inc": {"visits": 1}},
            ),
        ),
        Scenario(
            "count_documents",
            lambda i: ours_small.count_documents({"score": {"$lt": i % 100}}),
            lambda i: theirs_small.count_documents({"score": {"$lt": i % 100}}),
        ),
        Scenario(
            "aggregate $group",
            lambda i: ours_small.aggregate(group),
            lambda i: _aggregate(theirs_small, group),
        ),
        Scenario(
            "transaction, 10 updates",
            transaction_ours,
            transaction_theirs,
            per_block=max(OPS // 10, 5),
        ),
    ]


async def _aggregate(collection: Any, pipeline: list[dict[str, Any]]) -> list[Any]:
    return await (await collection.aggregate(pipeline)).to_list()


async def block(operation: Operation, count: int, mode: str) -> float:
    """Return microseconds per operation for one block."""
    if mode == "concurrent":
        semaphore = asyncio.Semaphore(CONCURRENCY)

        async def one(index: int) -> None:
            async with semaphore:
                await operation(index)

        start = perf_counter()
        await asyncio.gather(*(one(index) for index in range(count)))
        return (perf_counter() - start) * 1e6 / count
    busy = 0.0
    for index in range(count):
        if mode == "idle":
            await asyncio.sleep(GAP)
        start = perf_counter()
        await operation(index)
        busy += perf_counter() - start
    return busy * 1e6 / count


async def seed(client: AsyncMongoClient[Any]) -> None:
    await client.drop_database(DATABASE_NAME)
    database = client[DATABASE_NAME]
    for name, factory in (("small", small), ("large", large)):
        await database[name].insert_many([factory(i) for i in range(DOCUMENTS)])
        await database[name].create_index("email", unique=True)
        await database[name].create_index("score")


async def main() -> None:
    modes = os.getenv("AUNIQORM_PAIRED_MODES", ",".join(MODES)).split(",")
    selected = os.getenv("AUNIQORM_PAIRED_SCENARIOS")
    client: AsyncMongoClient[Any] = AsyncMongoClient(MONGO_URL)
    await seed(client)
    async with Database(f"{MONGO_URL}/{DATABASE_NAME}") as db:
        native = db._driver._native is not None
        bridge = getattr(db._driver, "_bridge", None) is not None
        print(
            f"aUniqORM native MongoDB: {'enabled' if native else 'disabled'}"
            f" (completion bridge: {'on' if bridge else 'off'})"
        )
        print(
            f"{ROUNDS} paired rounds; microseconds per operation; idle gap "
            f"{GAP * 1000:g} ms; {CONCURRENCY} in flight when concurrent\n"
        )
        scenarios = await build(db, client)
        if selected:
            names = set(selected.split(","))
            scenarios = [s for s in scenarios if s.name in names]
        header = (
            f"{'scenario':26} {'mode':11} {'aUniqORM':>9} {'PyMongo':>9} "
            f"{'paired diff':>12} {'wins':>7}"
        )
        print(header)
        print("-" * len(header))
        wins_total = rounds_total = 0
        for scenario in scenarios:
            for mode in modes:
                ours, theirs = [], []
                await block(scenario.ours, min(scenario.per_block, 20), mode)
                await block(scenario.theirs, min(scenario.per_block, 20), mode)
                for round_index in range(ROUNDS):
                    pair = [(ours, scenario.ours), (theirs, scenario.theirs)]
                    for results, operation in pair[:: 1 if round_index % 2 else -1]:
                        results.append(await block(operation, scenario.per_block, mode))
                diffs = [a - b for a, b in zip(ours, theirs, strict=True)]
                wins = sum(diff < 0 for diff in diffs)
                wins_total += wins
                rounds_total += len(diffs)
                print(
                    f"{scenario.name:26} {mode:11} {statistics.median(ours):9.1f} "
                    f"{statistics.median(theirs):9.1f} "
                    f"{statistics.median(diffs):+12.1f} {wins:>3}/{len(diffs):<3}"
                )
        print(f"\naUniqORM won {wins_total} of {rounds_total} paired rounds")
    await client.drop_database(DATABASE_NAME)
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
