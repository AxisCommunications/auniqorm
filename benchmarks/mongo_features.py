"""Benchmark MongoDB features that are candidates for the native Rust path.

Each scenario compares aUniqORM with the equivalent direct PyMongo async code
against the same live MongoDB replica set.  The script detects which aUniqORM
APIs exist, so the same file measures the code before and after a feature
lands; features that do not exist yet fall back to how users would write the
operation today.

    docker compose -f benchmarks/compose.yaml up -d --wait mongodb
    python benchmarks/mongo_features.py

Environment variables:

``AUNIQORM_FEATURE_BENCH``         comma-separated scenarios (default: all)
``AUNIQORM_FEATURE_BENCH_ROUNDS``  rotated rounds per scenario (default: 5)
``AUNIQORM_FEATURE_BENCH_OUT``     write median results as JSON to this path
``AUNIQORM_FEATURE_BENCH_BASE``    compare against a previously written JSON
"""

from __future__ import annotations

import asyncio
import json
import os
import statistics
from collections.abc import Awaitable, Callable
from time import perf_counter
from typing import Any

import pymongo
from pymongo import AsyncMongoClient

import auniqorm
from auniqorm import Database

MONGO_URL = os.getenv("AUNIQORM_MONGO_BENCH_URL", "mongodb://localhost:27017")
DATABASE_NAME = "auniqorm_features"
ROUNDS = int(os.getenv("AUNIQORM_FEATURE_BENCH_ROUNDS", "5"))
USERS = 1000
STREAM_DOCUMENTS = 50_000
OPERATIONS = 1000
TRANSACTIONS = 100
TRANSACTION_SIZE = 10

Runner = Callable[[], Awaitable[Any]]


def user(index: int) -> dict[str, Any]:
    return {
        "email": f"bench-{index}@example.com",
        "name": f"User {index}",
        "score": index % 100,
        "visits": 0,
        "profile": {"name": f"User {index}", "active": True},
        "tags": ["benchmark", "user"],
    }


def email(index: int) -> str:
    return f"bench-{index % USERS}@example.com"


class Context:
    def __init__(self, db: Database, client: AsyncMongoClient[Any]) -> None:
        self.db = db
        self.client = client
        self.users = db.collection("users")
        self.stream = db.collection("stream")
        self.pm_users = client[DATABASE_NAME]["users"]
        self.pm_stream = client[DATABASE_NAME]["stream"]


def has(obj: Any, name: str) -> bool:
    return callable(getattr(obj, name, None))


def json_query(**command: Any) -> str:
    return json.dumps(command)


# ── Scenarios ──────────────────────────────────────────────────────────────


def bulk_update(ctx: Context) -> dict[str, Runner]:
    async def auniqorm_run() -> None:
        if has(ctx.users, "bulk_write"):
            await ctx.users.bulk_write(
                [
                    auniqorm.UpdateOne(
                        {"email": email(i)},
                        set={"profile.name": "U"},
                        inc={"visits": 1},
                    )
                    for i in range(OPERATIONS)
                ]
            )
        else:
            for i in range(OPERATIONS):
                await ctx.users.update_one(
                    {"email": email(i)}, set={"profile.name": "U"}, inc={"visits": 1}
                )

    async def pymongo_bulk() -> None:
        await ctx.pm_users.bulk_write(
            [
                pymongo.UpdateOne(
                    {"email": email(i)},
                    {"$set": {"profile.name": "U"}, "$inc": {"visits": 1}},
                )
                for i in range(OPERATIONS)
            ]
        )

    async def pymongo_loop() -> None:
        for i in range(OPERATIONS):
            await ctx.pm_users.update_one(
                {"email": email(i)},
                {"$set": {"profile.name": "U"}, "$inc": {"visits": 1}},
            )

    return {
        "auniqorm": auniqorm_run,
        "pymongo bulk_write": pymongo_bulk,
        "pymongo loop": pymongo_loop,
    }


def stream(ctx: Context) -> dict[str, Runner]:
    query = json_query(operation="find", collection="stream", filter={})

    async def auniqorm_rows() -> int:
        total = 0
        if has(ctx.stream, "iterate"):
            rows = ctx.stream.iterate(batch_size=1000)
        else:
            rows = ctx.db.iterate(query, batch_size=1000)
        async for row in rows:
            total += row["score"]
        return total

    async def auniqorm_batches() -> int:
        total = 0
        async for batch in ctx.db.iterate_batches(query, batch_size=1000):
            for row in batch:
                total += row["score"]
        return total

    async def pymongo_rows() -> int:
        total = 0
        async for row in ctx.pm_stream.find({}, batch_size=1000):
            total += row["score"]
        return total

    return {
        "auniqorm iterate": auniqorm_rows,
        "auniqorm iterate_batches": auniqorm_batches,
        "pymongo cursor": pymongo_rows,
    }


GROUP_PIPELINE = [
    {"$group": {"_id": "$score", "n": {"$sum": 1}, "visits": {"$sum": "$visits"}}},
    {"$sort": {"_id": 1}},
]
LARGE_PIPELINE = [
    {"$match": {"score": {"$lt": 50}}},
    {"$project": {"_id": 0, "email": 1, "score": 1, "profile": 1}},
]


def aggregate(ctx: Context) -> dict[str, Runner]:
    runners: dict[str, Runner] = {}
    if has(ctx.users, "aggregate"):

        async def auniqorm_group() -> None:
            for _ in range(200):
                await ctx.users.aggregate(GROUP_PIPELINE)

        async def auniqorm_large() -> None:
            for _ in range(10):
                await ctx.stream.aggregate(LARGE_PIPELINE)

        async def auniqorm_count_distinct() -> None:
            for i in range(200):
                await ctx.users.count_documents({"score": {"$lt": i % 100}})
                await ctx.users.distinct("score")

        runners["auniqorm group x200"] = auniqorm_group
        runners["auniqorm 25k docs x10"] = auniqorm_large
        runners["auniqorm count+distinct x200"] = auniqorm_count_distinct

    async def pymongo_group() -> None:
        for _ in range(200):
            await (await ctx.pm_users.aggregate(GROUP_PIPELINE)).to_list()

    async def pymongo_large() -> None:
        for _ in range(10):
            await (await ctx.pm_stream.aggregate(LARGE_PIPELINE)).to_list()

    async def pymongo_count_distinct() -> None:
        for i in range(200):
            await ctx.pm_users.count_documents({"score": {"$lt": i % 100}})
            await ctx.pm_users.distinct("score")

    runners["pymongo group x200"] = pymongo_group
    runners["pymongo 25k docs x10"] = pymongo_large
    runners["pymongo count+distinct x200"] = pymongo_count_distinct
    return runners


def transaction(ctx: Context) -> dict[str, Runner]:
    async def auniqorm_run() -> None:
        for t in range(TRANSACTIONS):
            async with await ctx.db.transaction():
                for j in range(TRANSACTION_SIZE):
                    await ctx.users.update_one(
                        {"email": email(t * TRANSACTION_SIZE + j)}, inc={"visits": 1}
                    )

    async def pymongo_run() -> None:
        for t in range(TRANSACTIONS):
            session = ctx.client.start_session()
            await session.start_transaction()
            for j in range(TRANSACTION_SIZE):
                await ctx.pm_users.update_one(
                    {"email": email(t * TRANSACTION_SIZE + j)},
                    {"$inc": {"visits": 1}},
                    session=session,
                )
            await session.commit_transaction()
            await session.end_session()

    return {"auniqorm": auniqorm_run, "pymongo": pymongo_run}


def json_reads(ctx: Context) -> dict[str, Runner]:
    async def auniqorm_point() -> None:
        for i in range(OPERATIONS):
            await ctx.db.fetch_one(
                json_query(
                    operation="find_one", collection="users", filter={"email": email(i)}
                )
            )

    async def auniqorm_sorted() -> None:
        for i in range(OPERATIONS):
            await ctx.db.fetch_all(
                json_query(
                    operation="find",
                    collection="users",
                    filter={"score": i % 100},
                    sort=[["email", 1]],
                    limit=5,
                )
            )

    async def pymongo_point() -> None:
        for i in range(OPERATIONS):
            await ctx.pm_users.find_one({"email": email(i)})

    async def pymongo_sorted() -> None:
        for i in range(OPERATIONS):
            await ctx.pm_users.find(
                {"score": i % 100}, sort=[("email", 1)], limit=5
            ).to_list()

    return {
        "auniqorm JSON find_one": auniqorm_point,
        "pymongo find_one": pymongo_point,
        "auniqorm JSON sorted find": auniqorm_sorted,
        "pymongo sorted find": pymongo_sorted,
    }


COLLATION = {"locale": "en", "strength": 2}


def options(ctx: Context) -> dict[str, Runner]:
    async def auniqorm_hint() -> None:
        for i in range(OPERATIONS):
            await ctx.db.fetch_all(
                json_query(
                    operation="find",
                    collection="users",
                    filter={"score": i % 100},
                    hint=[["score", 1]],
                    max_time_ms=5000,
                )
            )

    async def auniqorm_collation() -> None:
        for i in range(OPERATIONS):
            await ctx.db.fetch_all(
                json_query(
                    operation="find",
                    collection="users",
                    filter={"name": f"user {i % USERS}"},
                    collation=COLLATION,
                )
            )

    async def auniqorm_array_filters() -> None:
        for i in range(OPERATIONS):
            await ctx.db.execute(
                json_query(
                    operation="update_one",
                    collection="users",
                    filter={"email": email(i)},
                    update={"$set": {"tags.$[t]": "user"}},
                    array_filters=[{"t": "user"}],
                )
            )

    async def pymongo_hint() -> None:
        for i in range(OPERATIONS):
            await ctx.pm_users.find(
                {"score": i % 100}, hint=[("score", 1)], max_time_ms=5000
            ).to_list()

    async def pymongo_collation() -> None:
        for i in range(OPERATIONS):
            await ctx.pm_users.find(
                {"name": f"user {i % USERS}"}, collation=COLLATION
            ).to_list()

    async def pymongo_array_filters() -> None:
        for i in range(OPERATIONS):
            await ctx.pm_users.update_one(
                {"email": email(i)},
                {"$set": {"tags.$[t]": "user"}},
                array_filters=[{"t": "user"}],
            )

    return {
        "auniqorm JSON hint+max_time_ms": auniqorm_hint,
        "pymongo hint+max_time_ms": pymongo_hint,
        "auniqorm JSON collation": auniqorm_collation,
        "pymongo collation": pymongo_collation,
        "auniqorm JSON array_filters": auniqorm_array_filters,
        "pymongo array_filters": pymongo_array_filters,
    }


SCENARIOS: dict[str, Callable[[Context], dict[str, Runner]]] = {
    "bulk_update": bulk_update,
    "stream": stream,
    "aggregate": aggregate,
    "transaction": transaction,
    "json_reads": json_reads,
    "options": options,
}


async def seed(client: AsyncMongoClient[Any]) -> None:
    await client.drop_database(DATABASE_NAME)
    database = client[DATABASE_NAME]
    await database["users"].insert_many([user(i) for i in range(USERS)])
    await database["users"].create_index("email", unique=True)
    await database["users"].create_index("score")
    batch = [user(i) for i in range(STREAM_DOCUMENTS)]
    await database["stream"].insert_many(batch)


async def main() -> None:
    selected = os.getenv("AUNIQORM_FEATURE_BENCH", ",".join(SCENARIOS)).split(",")
    baseline_path = os.getenv("AUNIQORM_FEATURE_BENCH_BASE")
    baseline = json.loads(open(baseline_path).read()) if baseline_path else {}
    client: AsyncMongoClient[Any] = AsyncMongoClient(MONGO_URL)
    await seed(client)
    results: dict[str, float] = {}
    async with Database(f"{MONGO_URL}/{DATABASE_NAME}") as db:
        native = db._driver._native is not None
        print(f"aUniqORM native MongoDB: {'enabled' if native else 'disabled'}")
        print(f"{ROUNDS} rotated rounds; median milliseconds\n")
        ctx = Context(db, client)
        for name in selected:
            runners = SCENARIOS[name](ctx)
            timings: dict[str, list[float]] = {label: [] for label in runners}
            labels = list(runners)
            for round_index in range(ROUNDS):
                offset = round_index % len(labels)
                for label in labels[offset:] + labels[:offset]:
                    start = perf_counter()
                    await runners[label]()
                    timings[label].append((perf_counter() - start) * 1000)
            print(f"[{name}]")
            for label in labels:
                key = f"{name}/{label}"
                median = statistics.median(timings[label])
                results[key] = median
                before = baseline.get(key)
                change = (
                    f"   before {before:9.2f}  ({(median / before - 1) * 100:+6.1f}%)"
                    if before
                    else ""
                )
                print(f"  {label:32} {median:9.2f}{change}")
            print()
    await client.drop_database(DATABASE_NAME)
    await client.close()
    out = os.getenv("AUNIQORM_FEATURE_BENCH_OUT")
    if out:
        with open(out, "w") as handle:
            json.dump(results, handle, indent=2, sort_keys=True)


if __name__ == "__main__":
    asyncio.run(main())
