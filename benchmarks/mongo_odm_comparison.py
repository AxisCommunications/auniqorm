"""Compare asynchronous MongoDB document APIs against a live MongoDB server.

Install the benchmark dependencies with:

    uv run --with pymongo --with beanie --with odmantic \
        python benchmarks/mongo_odm_comparison.py

The benchmark deliberately reports API-to-database timings for equivalent
operations. It is not a benchmark of MongoDB itself; all participants use the
same server and indexes.
"""

from __future__ import annotations

import asyncio
import json
import os
import statistics
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from beanie import Document, init_beanie
from motor.motor_asyncio import AsyncIOMotorClient
from odmantic import AIOEngine, Model
from pymongo import AsyncMongoClient

from auniqorm import Database

MONGO_URL = os.getenv("AUNIQORM_MONGO_BENCH_URL", "mongodb://localhost:27017")
DATABASE_NAME = "auniqorm_benchmark"
DOCUMENT_COUNT = int(os.getenv("AUNIQORM_MONGO_BENCH_DOCUMENTS", "1000"))
ITERATIONS = int(os.getenv("AUNIQORM_MONGO_BENCH_ITERATIONS", "100"))
ROUNDS = int(os.getenv("AUNIQORM_MONGO_BENCH_ROUNDS", "5"))
LIBRARIES = os.getenv(
    "AUNIQORM_MONGO_BENCH_LIBRARIES", "auniqorm,auniqorm_json,pymongo,beanie,odmantic"
).split(",")
OPERATIONS = ("insert_many", "point_read", "filtered_read", "atomic_update")


class BeanieUser(Document):
    email: str
    name: str
    score: int
    profile: dict[str, Any]
    tags: list[str]

    class Settings:
        name = "beanie_users"


class OdmanticUser(Model):
    email: str
    name: str
    score: int
    profile: dict[str, Any]
    tags: list[str]


def payload(index: int) -> dict[str, Any]:
    return {
        "email": f"bench-{index}@example.com",
        "name": f"User {index}",
        "score": index % 100,
        "profile": {"name": f"User {index}", "active": True},
        "tags": ["benchmark", "user"],
    }


@dataclass(slots=True)
class Result:
    library: str
    operation: str
    milliseconds: float
    documents: int


async def measure(
    library: str,
    operation: str,
    action: Any,
    documents: int,
) -> Result:
    start = perf_counter()
    count = await action()
    completed = count if isinstance(count, int) else documents
    return Result(library, operation, (perf_counter() - start) * 1000, completed)


async def benchmark_auniqorm() -> list[Result]:
    async with Database(f"{MONGO_URL}/{DATABASE_NAME}") as db:
        users = db.collection("auniqorm_users")
        await users.drop()
        documents = [payload(index) for index in range(DOCUMENT_COUNT)]
        results = [
            await measure(
                "auniqorm",
                "insert_many",
                lambda: users.insert_many(documents),
                DOCUMENT_COUNT,
            )
        ]
        await users.create_index("email", unique=True)
        results.extend(
            [
                await measure(
                    "auniqorm",
                    "point_read",
                    lambda: read_collection_api(users),
                    ITERATIONS,
                ),
                await measure(
                    "auniqorm",
                    "filtered_read",
                    lambda: filter_collection_api(users),
                    ITERATIONS,
                ),
                await measure(
                    "auniqorm",
                    "atomic_update",
                    lambda: update_collection_api(users),
                    ITERATIONS,
                ),
            ]
        )
    return results


async def read_collection_api(users: Any) -> int:
    for index in range(ITERATIONS):
        await users.find_one({"email": f"bench-{index % DOCUMENT_COUNT}@example.com"})
    return ITERATIONS


async def filter_collection_api(users: Any) -> int:
    for index in range(ITERATIONS):
        await users.find(
            {"score": index % 100},
            projection={"_id": 0, "email": 1, "score": 1},
        )
    return ITERATIONS


async def update_collection_api(users: Any) -> int:
    for index in range(ITERATIONS):
        await users.update_one(
            {"email": f"bench-{index % DOCUMENT_COUNT}@example.com"},
            set={"profile.name": "Updated"},
            inc={"score": 1},
        )
    return ITERATIONS


async def benchmark_auniqorm_json() -> list[Result]:
    results: list[Result] = []
    async with Database(f"{MONGO_URL}/{DATABASE_NAME}") as db:
        await db.execute(
            json.dumps(
                {
                    "operation": "drop",
                    "collection": "auniqorm_json_users",
                }
            )
        )
        documents = [payload(index) for index in range(DOCUMENT_COUNT)]
        results.append(
            await measure(
                "auniqorm_json",
                "insert_many",
                lambda: db.execute(
                    json.dumps(
                        {
                            "operation": "insert_many",
                            "collection": "auniqorm_json_users",
                            "documents": documents,
                        }
                    )
                ),
                DOCUMENT_COUNT,
            )
        )
        await db.execute(
            json.dumps(
                {
                    "operation": "create_index",
                    "collection": "auniqorm_json_users",
                    "keys": [["email", 1]],
                    "unique": True,
                }
            )
        )
        results.append(
            await measure(
                "auniqorm_json",
                "point_read",
                lambda: read_auniqorm(db),
                ITERATIONS,
            )
        )
        results.append(
            await measure(
                "auniqorm_json",
                "filtered_read",
                lambda: filter_auniqorm(db),
                ITERATIONS,
            )
        )
        results.append(
            await measure(
                "auniqorm_json",
                "atomic_update",
                lambda: update_auniqorm(db),
                ITERATIONS,
            )
        )
    return results


async def read_auniqorm(db: Database) -> int:
    for index in range(ITERATIONS):
        await db.fetch_one(
            json.dumps(
                {
                    "operation": "find_one",
                    "collection": "auniqorm_json_users",
                    "filter": {"email": f"bench-{index % DOCUMENT_COUNT}@example.com"},
                }
            )
        )
    return ITERATIONS


async def filter_auniqorm(db: Database) -> int:
    for index in range(ITERATIONS):
        await db.fetch_all(
            json.dumps(
                {
                    "operation": "find",
                    "collection": "auniqorm_json_users",
                    "filter": {"score": index % 100},
                    "projection": {"_id": 0, "email": 1, "score": 1},
                }
            )
        )
    return ITERATIONS


async def update_auniqorm(db: Database) -> int:
    for index in range(ITERATIONS):
        await db.execute(
            json.dumps(
                {
                    "operation": "update_one",
                    "collection": "auniqorm_json_users",
                    "filter": {"email": f"bench-{index % DOCUMENT_COUNT}@example.com"},
                    "update": {
                        "$set": {"profile.name": "Updated"},
                        "$inc": {"score": 1},
                    },
                }
            )
        )
    return ITERATIONS


async def benchmark_pymongo() -> list[Result]:
    client = AsyncMongoClient(MONGO_URL)
    collection = client[DATABASE_NAME]["pymongo_users"]
    await collection.drop()
    documents = [payload(index) for index in range(DOCUMENT_COUNT)]
    results = [
        await measure(
            "pymongo",
            "insert_many",
            lambda: collection.insert_many(documents),
            DOCUMENT_COUNT,
        )
    ]
    await collection.create_index("email", unique=True)
    results.extend(
        [
            await measure(
                "pymongo",
                "point_read",
                lambda: read_collection(collection),
                ITERATIONS,
            ),
            await measure(
                "pymongo",
                "filtered_read",
                lambda: filter_collection(collection),
                ITERATIONS,
            ),
            await measure(
                "pymongo",
                "atomic_update",
                lambda: update_collection(collection),
                ITERATIONS,
            ),
        ]
    )
    await client.close()
    return results


async def benchmark_beanie() -> list[Result]:
    client = AsyncMongoClient(MONGO_URL)
    await init_beanie(database=client[DATABASE_NAME], document_models=[BeanieUser])
    await client[DATABASE_NAME].drop_collection("beanie_users")
    documents = [BeanieUser(**payload(index)) for index in range(DOCUMENT_COUNT)]
    results = [
        await measure(
            "beanie",
            "insert_many",
            lambda: BeanieUser.insert_many(documents),
            DOCUMENT_COUNT,
        )
    ]
    await client[DATABASE_NAME]["beanie_users"].create_index("email", unique=True)
    results.extend(
        [
            await measure("beanie", "point_read", read_beanie, ITERATIONS),
            await measure("beanie", "filtered_read", filter_beanie, ITERATIONS),
            await measure("beanie", "atomic_update", update_beanie, ITERATIONS),
        ]
    )
    await client.close()
    return results


async def read_beanie() -> int:
    for index in range(ITERATIONS):
        await BeanieUser.find_one(
            BeanieUser.email == f"bench-{index % DOCUMENT_COUNT}@example.com"
        )
    return ITERATIONS


async def filter_beanie() -> int:
    for index in range(ITERATIONS):
        await (
            BeanieUser.find(BeanieUser.score == index % 100)
            .project(BeanieUser)
            .to_list()
        )
    return ITERATIONS


async def update_beanie() -> int:
    for index in range(ITERATIONS):
        await BeanieUser.find_one(
            BeanieUser.email == f"bench-{index % DOCUMENT_COUNT}@example.com"
        ).update(
            {
                "$set": {"profile.name": "Updated"},
                "$inc": {"score": 1},
            }
        )
    return ITERATIONS


async def benchmark_odmantic() -> list[Result]:
    client = AsyncIOMotorClient(MONGO_URL)
    engine = AIOEngine(client=client, database=DATABASE_NAME)
    collection = client[DATABASE_NAME][OdmanticUser.__name__.lower()]
    await collection.drop()
    documents = [OdmanticUser(**payload(index)) for index in range(DOCUMENT_COUNT)]
    results = [
        await measure(
            "odmantic",
            "insert_many",
            lambda: engine.save_all(documents),
            DOCUMENT_COUNT,
        )
    ]
    await collection.create_index("email", unique=True)
    results.extend(
        [
            await measure(
                "odmantic",
                "point_read",
                lambda: read_odmantic(engine),
                ITERATIONS,
            ),
            await measure(
                "odmantic",
                "filtered_read",
                lambda: filter_odmantic(engine),
                ITERATIONS,
            ),
            await measure(
                "odmantic",
                "atomic_update",
                lambda: update_odmantic(engine),
                ITERATIONS,
            ),
        ]
    )
    client.close()
    return results


async def read_odmantic(engine: AIOEngine) -> int:
    for index in range(ITERATIONS):
        await engine.find_one(
            OdmanticUser,
            OdmanticUser.email == f"bench-{index % DOCUMENT_COUNT}@example.com",
        )
    return ITERATIONS


async def filter_odmantic(engine: AIOEngine) -> int:
    for index in range(ITERATIONS):
        [
            user
            async for user in engine.find(
                OdmanticUser, OdmanticUser.score == index % 100
            )
        ]
    return ITERATIONS


async def update_odmantic(engine: AIOEngine) -> int:
    for index in range(ITERATIONS):
        user = await engine.find_one(
            OdmanticUser,
            OdmanticUser.email == f"bench-{index % DOCUMENT_COUNT}@example.com",
        )
        assert user is not None
        user.profile["name"] = "Updated"
        user.score += 1
        await engine.save(user)
    return ITERATIONS


async def read_collection(collection: Any) -> int:
    for index in range(ITERATIONS):
        await collection.find_one(
            {"email": f"bench-{index % DOCUMENT_COUNT}@example.com"}
        )
    return ITERATIONS


async def filter_collection(collection: Any) -> int:
    for index in range(ITERATIONS):
        await collection.find(
            {"score": index % 100},
            {"_id": 0, "email": 1, "score": 1},
        ).to_list()
    return ITERATIONS


async def update_collection(collection: Any) -> int:
    for index in range(ITERATIONS):
        await collection.update_one(
            {"email": f"bench-{index % DOCUMENT_COUNT}@example.com"},
            {"$set": {"profile.name": "Updated"}, "$inc": {"score": 1}},
        )
    return ITERATIONS


def native_mongo_enabled() -> bool:
    from auniqorm.drivers import mongo

    driver = mongo.MongoDriver(f"{MONGO_URL}/{DATABASE_NAME}")
    return driver._use_native


async def main() -> None:
    runners = {
        "auniqorm": benchmark_auniqorm,
        "auniqorm_json": benchmark_auniqorm_json,
        "pymongo": benchmark_pymongo,
        "beanie": benchmark_beanie,
        "odmantic": benchmark_odmantic,
    }
    libraries = [library for library in LIBRARIES if library in runners]
    native_state = "enabled" if native_mongo_enabled() else "disabled"
    print(f"aUniqORM native MongoDB: {native_state}")
    print(
        f"{DOCUMENT_COUNT} documents, {ITERATIONS} operations, "
        f"{ROUNDS} rotated rounds; median milliseconds\n"
    )
    timings: dict[tuple[str, str], list[float]] = {}
    for round_index in range(ROUNDS):
        offset = round_index % len(libraries)
        for library in libraries[offset:] + libraries[:offset]:
            for result in await runners[library]():
                timings.setdefault((result.library, result.operation), []).append(
                    result.milliseconds
                )

    print(f"{'operation':15}" + "".join(f"{library:>15}" for library in libraries))
    for operation in OPERATIONS:
        cells = []
        for library in libraries:
            values = timings.get((library, operation))
            cells.append(
                f"{statistics.median(values):15.2f}" if values else f"{'-':>15}"
            )
        print(f"{operation:15}" + "".join(cells))


if __name__ == "__main__":
    asyncio.run(main())
