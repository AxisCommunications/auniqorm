"""Unit tests for the keyword-based MongoDB collection API."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from auniqorm import (
    BulkWriteResult,
    Collection,
    Database,
    DeleteMany,
    DeleteOne,
    InsertOne,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
)
from auniqorm.exceptions import BulkWriteError, ConnectionError, QueryError
from auniqorm.sync import SyncDatabase


@pytest.fixture
def driver():
    driver = MagicMock()
    driver.SUPPORTS_COLLECTIONS = True
    driver.execute_operation = AsyncMock(return_value=1)
    driver.fetch_operation = AsyncMock(return_value=[])
    return driver


@pytest.fixture
def users(driver) -> Collection:
    db = Database("sqlite:///:memory:")
    db._driver = driver
    return db.collection("users")


def _command(mock: AsyncMock) -> dict:
    return mock.await_args.args[0]


async def test_insert_one_and_many(users, driver) -> None:
    driver.execute_operation.return_value = "id"
    assert await users.insert_one({"name": "Ada"}) == "id"
    assert _command(driver.execute_operation) == {
        "operation": "insert_one",
        "collection": "users",
        "document": {"name": "Ada"},
    }

    driver.execute_operation.return_value = ["a", "b"]
    assert await users.insert_many(iter([{"n": 1}, {"n": 2}])) == ["a", "b"]
    assert _command(driver.execute_operation)["documents"] == [{"n": 1}, {"n": 2}]


async def test_find_omits_unset_options(users, driver) -> None:
    driver.fetch_operation.return_value = [{"n": 1}]
    assert await users.find({"n": 1}, projection={"n": 1}, limit=5) == [{"n": 1}]
    assert _command(driver.fetch_operation) == {
        "operation": "find",
        "collection": "users",
        "filter": {"n": 1},
        "projection": {"n": 1},
        "limit": 5,
    }


@pytest.mark.parametrize(
    ("sort", "expected"),
    [
        ("name", {"name": 1}),
        ([("name", -1), ("age", 1)], {"name": -1, "age": 1}),
        ({"name": -1}, {"name": -1}),
    ],
)
async def test_sort_forms(users, driver, sort, expected) -> None:
    await users.find(sort=sort, skip=2)
    command = _command(driver.fetch_operation)
    assert command["sort"] == expected
    assert command["skip"] == 2
    assert "filter" not in command


async def test_find_one_returns_first_or_none(users, driver) -> None:
    assert await users.find_one({"n": 1}) is None
    assert _command(driver.fetch_operation)["operation"] == "find_one"
    driver.fetch_operation.return_value = [{"n": 1}]
    assert await users.find_one(sort="n") == {"n": 1}


async def test_update_operators_map_to_mongodb(users, driver) -> None:
    assert (
        await users.update_one(
            {"_id": 1},
            set={"profile.name": "Ada"},
            inc={"logins": 1},
            unset=["old", "older"],
            current_date={"seen": True},
            add_to_set={"tags": "x"},
            upsert=True,
        )
        == 1
    )
    assert _command(driver.execute_operation) == {
        "operation": "update_one",
        "collection": "users",
        "filter": {"_id": 1},
        "update": {
            "$set": {"profile.name": "Ada"},
            "$unset": {"old": "", "older": ""},
            "$inc": {"logins": 1},
            "$currentDate": {"seen": True},
            "$addToSet": {"tags": "x"},
        },
        "upsert": True,
    }


@pytest.mark.parametrize(
    ("unset", "expected"),
    [("old", {"old": ""}), ({"old": 1}, {"old": 1})],
)
async def test_unset_forms(users, driver, unset, expected) -> None:
    await users.update_many(unset=unset)
    command = _command(driver.execute_operation)
    assert command["operation"] == "update_many"
    assert command["filter"] == {}
    assert command["update"] == {"$unset": expected}
    assert command["upsert"] is False


async def test_raw_update_and_pipeline(users, driver) -> None:
    await users.update_one({"n": 1}, update={"$max": {"n": 3}})
    assert _command(driver.execute_operation)["update"] == {"$max": {"n": 3}}
    await users.update_many(update=({"$set": {"m": "$n"}},))
    assert _command(driver.execute_operation)["update"] == [{"$set": {"m": "$n"}}]


async def test_update_validation(users) -> None:
    with pytest.raises(ValueError, match="at least one operator"):
        await users.update_one({"n": 1})
    with pytest.raises(ValueError, match="not both"):
        await users.update_many(update={"$set": {"n": 1}}, inc={"n": 1})


async def test_replace_and_delete(users, driver) -> None:
    await users.replace_one({"n": 1}, {"n": 2}, upsert=True)
    assert _command(driver.execute_operation) == {
        "operation": "replace_one",
        "collection": "users",
        "filter": {"n": 1},
        "replacement": {"n": 2},
        "upsert": True,
    }
    await users.delete_one({"n": 2})
    assert _command(driver.execute_operation)["operation"] == "delete_one"
    await users.delete_many()
    assert _command(driver.execute_operation) == {
        "operation": "delete_many",
        "collection": "users",
        "filter": {},
    }


async def test_collection_requires_connection_and_support(driver) -> None:
    db = Database("sqlite:///:memory:")
    users = db.collection("users")
    assert users.name == "users"
    assert repr(users) == "Collection('users')"
    with pytest.raises(ConnectionError):
        await users.find()

    driver.SUPPORTS_COLLECTIONS = False
    db._driver = driver
    with pytest.raises(QueryError, match="does not support collections"):
        await users.find()

    with pytest.raises(ValueError, match="must not be empty"):
        db.collection("")


async def test_sql_driver_rejects_collections() -> None:
    async with Database("sqlite:///:memory:") as db:
        with pytest.raises(QueryError, match="sqlite does not support collections"):
            await db.collection("users").find()


def test_sync_collection(driver) -> None:
    sync_db = SyncDatabase("sqlite:///:memory:")
    sync_db.async_database._driver = driver
    driver.fetch_operation.return_value = [{"n": 1}]

    users = sync_db.collection("users")

    assert repr(users) == "Collection('users')"
    assert users.find({"n": 1}) == [{"n": 1}]
    assert users.update_one({"n": 1}, set={"n": 2}) == 1


async def test_bulk_write_builds_wire_statements(users, driver) -> None:
    driver.bulk_write_operation = AsyncMock(
        return_value={
            "writeErrors": [],
            "writeConcernErrors": [],
            "nInserted": 1,
            "nUpserted": 1,
            "nMatched": 2,
            "nModified": 2,
            "nRemoved": 3,
            "upserted": [{"index": 2, "_id": "new"}],
        }
    )

    result = await users.bulk_write(
        [
            InsertOne({"n": 1}),
            UpdateOne({"n": 1}, set={"m": 1}),
            UpdateMany(inc={"m": 1}, upsert=True),
            ReplaceOne({"n": 2}, {"n": 3}),
            DeleteOne({"n": 3}),
            DeleteMany(),
        ],
        ordered=False,
    )

    assert result == BulkWriteResult(1, 2, 2, 3, 1, {2: "new"})
    assert _command(driver.bulk_write_operation) == {
        "operation": "bulk_write",
        "collection": "users",
        "ordered": False,
        "requests": [
            {"insert": {"n": 1}},
            {
                "update": {
                    "q": {"n": 1},
                    "u": {"$set": {"m": 1}},
                    "multi": False,
                    "upsert": False,
                }
            },
            {
                "update": {
                    "q": {},
                    "u": {"$inc": {"m": 1}},
                    "multi": True,
                    "upsert": True,
                }
            },
            {
                "replace": {
                    "q": {"n": 2},
                    "u": {"n": 3},
                    "multi": False,
                    "upsert": False,
                }
            },
            {"delete": {"q": {"n": 3}, "limit": 1}},
            {"delete": {"q": {}, "limit": 0}},
        ],
    }


async def test_bulk_write_raises_with_details(users, driver) -> None:
    details = {
        "writeErrors": [{"index": 1, "code": 11000, "errmsg": "E11000 duplicate"}],
        "writeConcernErrors": [],
        "nInserted": 1,
        "nUpserted": 0,
        "nMatched": 0,
        "nModified": 0,
        "nRemoved": 0,
        "upserted": [],
    }
    driver.bulk_write_operation = AsyncMock(return_value=details)

    with pytest.raises(BulkWriteError, match="E11000 duplicate") as caught:
        await users.bulk_write([InsertOne({"_id": 1}), InsertOne({"_id": 1})])
    assert caught.value.details == details


async def test_bulk_write_validation(users) -> None:
    with pytest.raises(ValueError, match="at least one request"):
        await users.bulk_write([])
    with pytest.raises(ValueError, match="update operators"):
        ReplaceOne({}, {"$set": {"n": 1}})
    assert repr(DeleteOne({"n": 1})) == "DeleteOne({'q': {'n': 1}, 'limit': 1})"


async def test_iterate_streams_rows_and_batches(users, driver) -> None:
    async def batches(command, *, batch_size):
        assert command == {
            "operation": "find",
            "collection": "users",
            "filter": {"n": 1},
            "sort": {"n": -1},
            "limit": 3,
        }
        assert batch_size == 2
        yield [{"n": 1}, {"n": 2}]
        yield [{"n": 3}]

    driver.iterate_operation = batches

    rows = [
        r
        async for r in users.iterate({"n": 1}, sort=[("n", -1)], limit=3, batch_size=2)
    ]
    assert rows == [{"n": 1}, {"n": 2}, {"n": 3}]
    chunks = [
        b
        async for b in users.iterate_batches(
            {"n": 1}, sort={"n": -1}, limit=3, batch_size=2
        )
    ]
    assert chunks == [[{"n": 1}, {"n": 2}], [{"n": 3}]]

    with pytest.raises(ValueError, match="batch_size"):
        [b async for b in users.iterate_batches(batch_size=0)]


async def test_aggregate_count_and_distinct(users, driver) -> None:
    driver.fetch_operation.return_value = [{"_id": 1, "n": 2}]
    assert await users.aggregate(
        ({"$group": {"_id": "$a", "n": {"$sum": 1}}},), allow_disk_use=True
    ) == [{"_id": 1, "n": 2}]
    assert _command(driver.fetch_operation) == {
        "operation": "aggregate",
        "collection": "users",
        "pipeline": [{"$group": {"_id": "$a", "n": {"$sum": 1}}}],
        "allow_disk_use": True,
    }

    driver.execute_operation.return_value = 7
    assert await users.count_documents({"a": 1}, limit=10) == 7
    assert _command(driver.execute_operation) == {
        "operation": "count_documents",
        "collection": "users",
        "filter": {"a": 1},
        "limit": 10,
    }

    driver.execute_operation.return_value = [1, 2]
    assert await users.distinct("a") == [1, 2]
    assert _command(driver.execute_operation) == {
        "operation": "distinct",
        "collection": "users",
        "key": "a",
        "filter": {},
    }


CI = {"locale": "en", "strength": 2}


async def test_read_options_pass_through(users, driver) -> None:
    options = {"hint": "a_1", "collation": CI, "max_time_ms": 50}
    await users.find({"a": 1}, **options)
    assert _command(driver.fetch_operation) == {
        "operation": "find",
        "collection": "users",
        "filter": {"a": 1},
        **options,
    }
    await users.find_one(**options)
    assert _command(driver.fetch_operation)["max_time_ms"] == 50
    await users.aggregate([], **options)
    assert _command(driver.fetch_operation)["collation"] == CI

    async def batches(command, *, batch_size):
        assert command["hint"] == [("a", 1)]
        yield []

    driver.iterate_operation = batches
    assert [r async for r in users.iterate(hint=[("a", 1)])] == []

    await users.count_documents(**options)
    assert _command(driver.execute_operation)["hint"] == "a_1"
    driver.execute_operation.return_value = []
    await users.distinct("a", collation=CI, max_time_ms=5)
    assert _command(driver.execute_operation) == {
        "operation": "distinct",
        "collection": "users",
        "key": "a",
        "filter": {},
        "collation": CI,
        "max_time_ms": 5,
    }


async def test_write_options_pass_through(users, driver) -> None:
    await users.update_many(
        set={"g.$[x]": 1}, array_filters=({"x": 2},), collation=CI, hint="a_1"
    )
    command = _command(driver.execute_operation)
    assert command["array_filters"] == [{"x": 2}]
    assert command["collation"] == CI
    assert command["hint"] == "a_1"
    await users.replace_one({}, {"a": 1}, collation=CI, hint="a_1")
    assert _command(driver.execute_operation)["hint"] == "a_1"
    await users.delete_one({}, collation=CI)
    assert _command(driver.execute_operation) == {
        "operation": "delete_one",
        "collection": "users",
        "filter": {},
        "collation": CI,
    }
    await users.delete_many(hint="a_1")
    assert "collation" not in _command(driver.execute_operation)


def test_request_options_use_wire_names() -> None:
    update = UpdateOne(
        {"a": 1}, set={"g.$[x]": 1}, array_filters=[{"x": 2}], collation=CI, hint="a_1"
    )
    assert update._request() == {
        "update": {
            "q": {"a": 1},
            "u": {"$set": {"g.$[x]": 1}},
            "multi": False,
            "upsert": False,
            "arrayFilters": [{"x": 2}],
            "collation": CI,
            "hint": "a_1",
        }
    }
    assert ReplaceOne({}, {"a": 1}, hint="a_1")._request()["replace"]["hint"] == "a_1"
    assert DeleteMany({}, collation=CI)._request() == {
        "delete": {"q": {}, "limit": 0, "collation": CI}
    }


async def test_admin_operations(users, driver) -> None:
    driver.execute_operation.return_value = "ab"
    assert (
        await users.create_index(
            [("a", 1), ("b", "text")],
            name="ab",
            unique=True,
            expire_after_seconds=60,
            partial_filter_expression={"a": {"$gt": 0}},
        )
        == "ab"
    )
    assert _command(driver.execute_operation) == {
        "operation": "create_index",
        "collection": "users",
        "keys": {"a": 1, "b": "text"},
        "name": "ab",
        "unique": True,
        "expire_after_seconds": 60,
        "partial_filter_expression": {"a": {"$gt": 0}},
    }
    await users.drop_index("ab")
    assert _command(driver.execute_operation)["index"] == "ab"
    await users.drop_index([("a", 1)])
    assert _command(driver.execute_operation)["index"] == {"a": 1}
    await users.drop()
    assert _command(driver.execute_operation) == {
        "operation": "drop",
        "collection": "users",
    }
