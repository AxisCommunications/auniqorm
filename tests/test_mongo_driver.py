"""Unit tests for the MongoDB driver without a live MongoDB server."""

import asyncio
import socket
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from bson import ObjectId, decode, encode
from bson.codec_options import DEFAULT_CODEC_OPTIONS
from pymongo.errors import BulkWriteError as PyMongoBulkWriteError
from pymongo.errors import OperationFailure

import auniqorm.drivers.mongo as mongo
from auniqorm.connection import Database
from auniqorm.drivers.registry import DriverRegistry
from auniqorm.exceptions import QueryError


def test_mongo_protocols_are_registered() -> None:
    assert Database._parse_protocol("mongodb://localhost/example") == "mongodb"
    assert Database._parse_protocol("mongo://localhost/example") == "mongodb"
    assert Database._parse_protocol("mongodb+srv://cluster/example") == "mongodb"
    assert DriverRegistry._drivers["mongodb"] == DriverRegistry._drivers["mongo"]


@pytest.fixture
def mongo_client(monkeypatch):
    client = MagicMock()
    client.aconnect = AsyncMock()
    client.close = MagicMock()
    client.codec_options = DEFAULT_CODEC_OPTIONS
    database = MagicMock()
    client.__getitem__.return_value = database
    monkeypatch.setattr(mongo, "AsyncMongoClient", MagicMock(return_value=client))
    monkeypatch.setattr(mongo, "auniqorm_native", None)
    return client, database


async def test_connect_and_insert_use_async_pymongo(mongo_client) -> None:
    client, database = mongo_client
    collection = database.__getitem__.return_value
    result = MagicMock(inserted_id="new-id")
    collection.insert_one = AsyncMock(return_value=result)

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    inserted = await driver.execute(
        '{"operation":"insert_one","collection":"users","document":{"name":"Ada"}}'
    )

    assert driver.is_connected
    assert inserted == "new-id"
    client.aconnect.assert_awaited_once()
    collection.insert_one.assert_awaited_once_with({"name": "Ada"})


async def test_fetch_all_and_iteration(mongo_client) -> None:
    _, database = mongo_client
    collection = database.__getitem__.return_value
    collection.find.return_value = _AsyncRows([{"_id": 1}, {"_id": 2}])

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    query = (
        '{"operation":"find","collection":"users","filter":{"active":true},'
        '"projection":{"name":1}}'
    )
    assert await driver.fetch_all(query) == [{"_id": 1}, {"_id": 2}]
    assert collection.find.call_args_list[0] == (
        ({"active": True},),
        {"projection": {"name": 1}},
    )
    assert [row async for row in driver.iterate(query, batch_size=10)] == [
        {"_id": 1},
        {"_id": 2},
    ]


async def test_native_json_reads_use_bson_path(monkeypatch, mongo_client) -> None:
    native = _native_engine(monkeypatch)
    rows = encode({"_id": 1, "name": "Ada"}) + encode({"_id": 2, "name": "Grace"})
    native.fetch_bson = AsyncMock(return_value=(rows, None))

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    query = (
        '{"operation":"find","collection":"users","filter":{"active":true},'
        '"projection":{"name":1},"sort":[["name",1]],"skip":1,"limit":2}'
    )

    assert await driver.fetch_all(query) == [
        {"_id": 1, "name": "Ada"},
        {"_id": 2, "name": "Grace"},
    ]
    assert decode(native.fetch_bson.await_args.args[0]) == {
        "operation": "find",
        "collection": "users",
        "filter": {"active": True},
        "projection": {"name": 1},
        "sort": {"name": 1},
        "skip": 1,
        "limit": 2,
    }
    find_one = '{"operation":"find_one","collection":"users","filter":{"_id":1}}'
    assert await driver.fetch_one(find_one) == {"_id": 1, "name": "Ada"}
    assert await driver.fetch_val(find_one) == 1
    native.fetch_bson.return_value = (b"", None)
    assert await driver.fetch_one(find_one) is None


async def test_native_fetch_falls_back_for_unsupported_options(
    monkeypatch, mongo_client
) -> None:
    _, database = mongo_client
    native = _native_engine(monkeypatch)
    collection = database.__getitem__.return_value
    collection.find.return_value = _AsyncRows([{"_id": 1}])
    collection.find_one = AsyncMock(return_value={"_id": 2})

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()

    query = '{"operation":"find","collection":"users","filter":{},"comment":"x"}'
    assert await driver.fetch_all(query) == [{"_id": 1}]
    query = '{"operation":"find_one","collection":"users","comment":"x"}'
    assert await driver.fetch_one(query) == {"_id": 2}
    native.fetch_bson.assert_not_awaited()


def _native_engine(monkeypatch) -> MagicMock:
    native = MagicMock()
    native.fetch_bson = AsyncMock(return_value=(b"", None))
    native.close = AsyncMock()
    monkeypatch.setattr(
        mongo,
        "auniqorm_native",
        SimpleNamespace(connect_mongo=AsyncMock(return_value=native)),
    )
    return native


async def test_json_execute_uses_native_bson_path(monkeypatch, mongo_client) -> None:
    _, database = mongo_client
    native = _native_engine(monkeypatch)
    native.execute_bson = AsyncMock(return_value=encode({"result": ["a", "b"]}))
    collection = database.__getitem__.return_value
    collection.insert_many = AsyncMock()

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    query = '{"operation":"insert_many","collection":"users","documents":[{},{}]}'

    assert await driver.execute(query) == ["a", "b"]
    assert decode(native.execute_bson.await_args.args[0]) == {
        "operation": "insert_many",
        "collection": "users",
        "documents": [{}, {}],
    }
    collection.insert_many.assert_not_awaited()


@pytest.mark.parametrize(
    ("query", "values", "kwargs"),
    [
        pytest.param(
            '{"operation":"update_one","collection":"users","filter":{},'
            '"update":{"$set":{"a":1}},"comment":"x"}',
            None,
            {},
            id="unsupported-option",
        ),
        pytest.param(
            '{"operation":"update_one","collection":"users","filter":{},'
            '"update":{"$set":{"a":1}}}',
            {"upsert": True},
            {"upsert": True},
            id="mapping-values",
        ),
    ],
)
async def test_json_execute_falls_back_when_not_native(
    monkeypatch, mongo_client, query, values, kwargs
) -> None:
    _, database = mongo_client
    native = _native_engine(monkeypatch)
    native.execute_bson = AsyncMock()
    collection = database.__getitem__.return_value
    collection.update_one = AsyncMock(return_value=MagicMock(modified_count=1))

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()

    assert await driver.execute(query, values) == 1
    native.execute_bson.assert_not_awaited()
    collection.update_one.assert_awaited_once_with({}, {"$set": {"a": 1}}, **kwargs)


@pytest.mark.parametrize(
    ("error", "document"),
    [
        pytest.param(RuntimeError("duplicate key"), "{}", id="server-error"),
        pytest.param(None, '{"v":18446744073709551616}', id="int-overflow"),
    ],
)
async def test_json_execute_reports_errors(
    monkeypatch, mongo_client, error, document
) -> None:
    native = _native_engine(monkeypatch)
    native.execute_bson = AsyncMock(side_effect=error)

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()

    with pytest.raises(QueryError, match="duplicate key|8-byte ints"):
        await driver.execute(
            f'{{"operation":"insert_one","collection":"users","document":{document}}}'
        )


async def test_pymongo_transaction_passes_session(mongo_client) -> None:
    client, database = mongo_client
    session = MagicMock()
    session.start_transaction = AsyncMock()
    session.end_session = AsyncMock()
    session.abort_transaction = AsyncMock()
    client.start_session = MagicMock(return_value=session)
    collection = database.__getitem__.return_value
    collection.insert_one = AsyncMock(return_value=MagicMock(inserted_id="id"))

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    await driver.begin()

    assert (
        await driver.execute(
            '{"operation":"insert_one","collection":"users","document":{}}'
        )
        == "id"
    )
    collection.insert_one.assert_awaited_once_with({}, session=session)


def test_native_opt_out(monkeypatch, mongo_client) -> None:
    _native_engine(monkeypatch)
    monkeypatch.setenv("AUNIQORM_MONGODB_NATIVE", "0")

    assert not mongo.MongoDriver("mongodb://localhost/example")._use_native


async def test_execute_operation_sends_bson_to_native(
    monkeypatch, mongo_client
) -> None:
    native = _native_engine(monkeypatch)
    oid = ObjectId()
    native.execute_bson = AsyncMock(return_value=encode({"result": oid}))

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    command = {"operation": "insert_one", "collection": "u", "document": {"_id": oid}}

    assert await driver.execute_operation(command) == oid
    assert decode(native.execute_bson.await_args.args[0]) == command


async def test_execute_operation_reports_native_errors(
    monkeypatch, mongo_client
) -> None:
    native = _native_engine(monkeypatch)
    native.execute_bson = AsyncMock(side_effect=RuntimeError("E11000"))

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()

    with pytest.raises(QueryError, match="E11000"):
        await driver.execute_operation(
            {"operation": "insert_one", "collection": "u", "document": {}}
        )


async def test_execute_operation_fallback_copies_documents(mongo_client) -> None:
    _, database = mongo_client
    collection = database.__getitem__.return_value
    collection.insert_one = AsyncMock(return_value=MagicMock(inserted_id="one"))
    collection.insert_many = AsyncMock(return_value=MagicMock(inserted_ids=["a"]))

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    document = {"n": 1}

    await driver.execute_operation(
        {"operation": "insert_one", "collection": "u", "document": document}
    )
    await driver.execute_operation(
        {"operation": "insert_many", "collection": "u", "documents": [document]}
    )

    sent_one = collection.insert_one.await_args.args[0]
    sent_many = collection.insert_many.await_args.args[0]
    assert sent_one == document and sent_one is not document
    assert sent_many == [document] and sent_many[0] is not document


async def test_fetch_operation_native_drains_prefetch(
    monkeypatch, mongo_client
) -> None:
    native = _native_engine(monkeypatch)
    rest = _NativeCursor([encode({"n": 3}), encode({"n": 4}) + encode({"n": 5})])
    native.fetch_bson = AsyncMock(
        return_value=(encode({"n": 1}) + encode({"n": 2}), rest)
    )

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    find = {"operation": "find", "collection": "u", "sort": {"n": -1}}

    assert await driver.fetch_operation(find) == [{"n": i} for i in range(1, 6)]
    assert rest.closed


async def test_fetch_operation_pymongo_transaction(mongo_client) -> None:
    client, database = mongo_client
    session = MagicMock()
    session.start_transaction = AsyncMock()
    session.end_session = AsyncMock()
    session.abort_transaction = AsyncMock()
    client.start_session = MagicMock(return_value=session)
    collection = database.__getitem__.return_value
    collection.find_one = AsyncMock(return_value={"n": 3})
    collection.find.return_value = _AsyncRows([{"n": 4}])

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    find = {"operation": "find", "collection": "u", "sort": {"n": -1}}

    await driver.begin()
    assert await driver.fetch_operation(find) == [{"n": 4}]
    assert collection.find.call_args.kwargs["sort"] == [("n", -1)]
    assert await driver.fetch_operation(
        {"operation": "find_one", "collection": "u"}
    ) == [{"n": 3}]
    collection.find_one.return_value = None
    assert (
        await driver.fetch_operation({"operation": "find_one", "collection": "u"}) == []
    )
    assert collection.find_one.await_args.kwargs == {"session": session}


async def test_fetch_operation_reports_native_errors(monkeypatch, mongo_client) -> None:
    native = _native_engine(monkeypatch)
    native.fetch_bson = AsyncMock(side_effect=RuntimeError("boom"))

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()

    with pytest.raises(QueryError, match="boom"):
        await driver.fetch_operation({"operation": "find", "collection": "u"})


BULK_COMMAND = {
    "operation": "bulk_write",
    "collection": "u",
    "ordered": True,
    "requests": [
        {"insert": {"n": 1}},
        {"update": {"q": {}, "u": {"$set": {"n": 2}}, "multi": False, "upsert": True}},
        {"update": {"q": {}, "u": {"$inc": {"n": 1}}, "multi": True, "upsert": False}},
        {"replace": {"q": {"n": 2}, "u": {"n": 3}, "multi": False, "upsert": False}},
        {"delete": {"q": {"n": 3}, "limit": 1}},
        {"delete": {"q": {}, "limit": 0}},
    ],
}


async def test_bulk_write_operation_native(monkeypatch, mongo_client) -> None:
    native = _native_engine(monkeypatch)
    native.bulk_write_bson = AsyncMock(
        return_value=encode({"result": {"nInserted": 1}})
    )

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()

    assert await driver.bulk_write_operation(BULK_COMMAND) == {"nInserted": 1}
    assert decode(native.bulk_write_bson.await_args.args[0]) == BULK_COMMAND

    native.bulk_write_bson.side_effect = RuntimeError("network")
    with pytest.raises(QueryError, match="network"):
        await driver.bulk_write_operation(BULK_COMMAND)


async def test_bulk_write_operation_fallback(mongo_client) -> None:
    _, database = mongo_client
    collection = database.__getitem__.return_value
    collection.bulk_write = AsyncMock(
        return_value=MagicMock(bulk_api_result={"nInserted": 1})
    )

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()

    assert await driver.bulk_write_operation(BULK_COMMAND) == {"nInserted": 1}
    requests = collection.bulk_write.await_args.args[0]
    assert [type(r).__name__ for r in requests] == [
        "InsertOne",
        "UpdateOne",
        "UpdateMany",
        "ReplaceOne",
        "DeleteOne",
        "DeleteMany",
    ]
    assert collection.bulk_write.await_args.kwargs == {"ordered": True}

    details = {"writeErrors": [{"index": 0}], "nInserted": 0}
    collection.bulk_write.side_effect = PyMongoBulkWriteError(details)
    assert await driver.bulk_write_operation(BULK_COMMAND) == details

    collection.bulk_write.side_effect = RuntimeError("down")
    with pytest.raises(QueryError, match="down"):
        await driver.bulk_write_operation(BULK_COMMAND)


class _NativeCursor:
    def __init__(self, batches, error=None):
        self._batches = list(batches)
        self._error = error
        self.closed = False

    async def next_batch(self):
        if self._error and not self._batches:
            raise self._error
        return self._batches.pop(0) if self._batches else None

    def close(self):
        self.closed = True


async def test_iterate_operation_native_streams_batches(
    monkeypatch, mongo_client
) -> None:
    native = _native_engine(monkeypatch)
    cursor = _NativeCursor([encode({"n": 1}) + encode({"n": 2}), encode({"n": 3})])
    native.open_cursor_bson = AsyncMock(return_value=cursor)

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    query = '{"operation":"find","collection":"u","filter":{},"sort":[["n",-1],"m"]}'

    assert [b async for b in driver.iterate_batches(query, batch_size=2)] == [
        [{"n": 1}, {"n": 2}],
        [{"n": 3}],
    ]
    assert cursor.closed
    sent = decode(native.open_cursor_bson.await_args.args[0])
    assert sent == {
        "operation": "find",
        "collection": "u",
        "filter": {},
        "sort": {"n": -1, "m": 1},
        "batch_size": 2,
    }


async def test_iterate_operation_native_errors(monkeypatch, mongo_client) -> None:
    native = _native_engine(monkeypatch)
    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    command = {"operation": "find", "collection": "u", "sort": "n"}

    native.open_cursor_bson = AsyncMock(side_effect=RuntimeError("bad query"))
    with pytest.raises(QueryError, match="bad query"):
        [b async for b in driver.iterate_operation(command, batch_size=1)]

    cursor = _NativeCursor([], error=RuntimeError("lost connection"))
    native.open_cursor_bson = AsyncMock(return_value=cursor)
    with pytest.raises(QueryError, match="lost connection"):
        [b async for b in driver.iterate_operation(command, batch_size=1)]
    assert cursor.closed


async def test_iterate_falls_back_for_unsupported_options(
    monkeypatch, mongo_client
) -> None:
    _, database = mongo_client
    native = _native_engine(monkeypatch)
    native.open_cursor_bson = AsyncMock()
    collection = database.__getitem__.return_value
    rows = _AsyncRows([{"n": 1}, {"n": 2}, {"n": 3}])
    collection.find.return_value = rows

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    query = (
        '{"operation":"find","collection":"u","filter":{},"hint":"n_1","comment":"x"}'
    )

    assert [r async for r in driver.iterate(query, batch_size=2)] == [
        {"n": 1},
        {"n": 2},
        {"n": 3},
    ]
    native.open_cursor_bson.assert_not_awaited()
    assert collection.find.call_args.kwargs == {"batch_size": 2, "hint": "n_1"}
    assert rows.closed

    collection.find.return_value = _AsyncRows([], error=OperationFailure("boom"))
    with pytest.raises(QueryError, match="boom"):
        [r async for r in driver.iterate(query)]

    with pytest.raises(QueryError, match="find operation"):
        [r async for r in driver.iterate('{"operation":"find_one","collection":"u"}')]


async def test_json_aggregate_native_and_fallback(monkeypatch, mongo_client) -> None:
    _, database = mongo_client
    native = _native_engine(monkeypatch)
    native.fetch_bson = AsyncMock(return_value=(encode({"n": 1}), None))
    collection = database.__getitem__.return_value
    collection.aggregate = AsyncMock(return_value=_AsyncRows([{"n": 2}]))

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    native_query = (
        '{"operation":"aggregate","collection":"u","pipeline":[{"$match":{}}],'
        '"allow_disk_use":true,"batch_size":5}'
    )
    assert await driver.fetch_all(native_query) == [{"n": 1}]
    assert decode(native.fetch_bson.await_args.args[0])["pipeline"] == [{"$match": {}}]

    fallback_query = (
        '{"operation":"aggregate","collection":"u","pipeline":[],'
        '"allow_disk_use":true,"batch_size":5,"hint":"a_1","let":{"x":1}}'
    )
    assert await driver.fetch_all(fallback_query) == [{"n": 2}]
    collection.aggregate.assert_awaited_once_with(
        [], hint="a_1", let={"x": 1}, allowDiskUse=True, batchSize=5
    )

    collection.aggregate.side_effect = OperationFailure("bad stage")
    with pytest.raises(QueryError, match="bad stage"):
        await driver.fetch_all(fallback_query)
    with pytest.raises(QueryError, match="find or aggregate"):
        await driver.fetch_all('{"operation":"count","collection":"u"}')


async def test_fallback_count_distinct_and_aggregate(mongo_client) -> None:
    _, database = mongo_client
    collection = database.__getitem__.return_value
    collection.count_documents = AsyncMock(return_value=3)
    collection.distinct = AsyncMock(return_value=["a"])
    collection.aggregate = AsyncMock(return_value=_AsyncRows([{"n": 1}]))

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()

    count = {"operation": "count_documents", "collection": "u", "filter": {}, "skip": 1}
    assert await driver.execute_operation(count) == 3
    collection.count_documents.assert_awaited_once_with({}, skip=1)
    distinct = {"operation": "distinct", "collection": "u", "key": "k", "filter": {}}
    assert await driver.execute_operation(distinct) == ["a"]
    collection.distinct.assert_awaited_once_with("k", {})
    pipeline = {"operation": "aggregate", "collection": "u", "pipeline": []}
    assert await driver.fetch_operation(pipeline) == [{"n": 1}]


async def test_native_transaction_routes_through_session(
    monkeypatch, mongo_client
) -> None:
    client, database = mongo_client
    native = _native_engine(monkeypatch)
    session = MagicMock(
        commit=AsyncMock(),
        abort=AsyncMock(),
        end_if_unused=MagicMock(return_value=False),
    )
    native.begin_transaction = MagicMock(return_value=session)
    native.execute_bson = AsyncMock(return_value=encode({"result": 1}))
    native.fetch_bson = AsyncMock(return_value=(encode({"n": 1}), None))
    native.bulk_write_bson = AsyncMock(
        return_value=encode({"result": {"nInserted": 1}})
    )
    collection = database.__getitem__.return_value
    collection.find.return_value = _AsyncRows([])

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    await driver.begin()
    assert driver.in_transaction
    with pytest.raises(QueryError, match="already active"):
        await driver.begin()

    update = (
        '{"operation":"update_one","collection":"u","filter":{},'
        '"update":{"$set":{"n":1}}}'
    )
    assert await driver.execute(update) == 1
    assert native.execute_bson.await_args.args[1] is session
    find_one = '{"operation":"find_one","collection":"u","filter":{}}'
    assert await driver.fetch_one(find_one) == {"n": 1}
    assert native.fetch_bson.await_args.args[1] is session
    assert await driver.fetch_all(
        '{"operation":"find","collection":"u","sort":[["n",1]]}'
    ) == [{"n": 1}]
    assert decode(native.fetch_bson.await_args.args[0])["sort"] == {"n": 1}
    assert await driver.bulk_write_operation(BULK_COMMAND) == {"nInserted": 1}
    assert native.bulk_write_bson.await_args.args[1] is session
    native.fetch_bson.return_value = (encode({"n": 1}) + encode({"n": 2}), None)
    batches = [
        b
        async for b in driver.iterate_operation(
            {"operation": "find", "collection": "u"}, batch_size=1
        )
    ]
    assert batches == [[{"n": 1}], [{"n": 2}]]

    with pytest.raises(QueryError, match="not supported by the native driver"):
        await driver.execute('{"operation":"drop","collection":"u"}')
    with pytest.raises(QueryError, match="not supported by the native driver"):
        await driver.fetch_all('{"operation":"find","collection":"u","comment":"x"}')
    with pytest.raises(QueryError, match="not supported by the native driver"):
        await driver.fetch_one(
            '{"operation":"find_one","collection":"u","comment":"x"}'
        )

    await driver.commit()
    session.commit.assert_awaited_once_with()
    assert not driver.in_transaction


async def test_native_transaction_rollback_and_errors(
    monkeypatch, mongo_client
) -> None:
    native = _native_engine(monkeypatch)
    session = MagicMock(
        commit=AsyncMock(side_effect=RuntimeError("WriteConflict")),
        abort=AsyncMock(side_effect=RuntimeError("already aborted")),
        end_if_unused=MagicMock(return_value=False),
    )
    native.begin_transaction = MagicMock(return_value=session)

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()

    await driver.begin()
    await driver.rollback()
    session.abort.assert_awaited_once_with()
    assert not driver.in_transaction

    await driver.begin()
    with pytest.raises(QueryError, match="WriteConflict"):
        await driver.commit()
    assert not driver.in_transaction

    await driver.begin()
    await driver.disconnect()
    assert session.abort.await_count == 2

    await driver.connect()
    native.execute_bson = AsyncMock(side_effect=RuntimeError("no replica set"))
    await driver.begin()
    with pytest.raises(QueryError, match="no replica set"):
        await driver.execute_operation(
            {"operation": "insert_one", "collection": "u", "document": {}}
        )
    await driver.rollback()
    assert not driver.in_transaction


async def test_native_empty_transaction_skips_round_trips(
    monkeypatch, mongo_client
) -> None:
    native = _native_engine(monkeypatch)
    session = MagicMock(
        commit=AsyncMock(),
        abort=AsyncMock(),
        end_if_unused=MagicMock(return_value=True),
    )
    native.begin_transaction = MagicMock(return_value=session)

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()

    await driver.begin()
    assert driver.in_transaction
    await driver.commit()
    await driver.begin()
    await driver.rollback()

    assert native.begin_transaction.call_count == 2
    session.commit.assert_not_awaited()
    session.abort.assert_not_awaited()
    assert not driver.in_transaction


async def test_pymongo_commit_ends_session_on_failure(mongo_client) -> None:
    client, _ = mongo_client
    session = MagicMock()
    session.start_transaction = AsyncMock()
    session.commit_transaction = AsyncMock(side_effect=RuntimeError("boom"))
    session.end_session = AsyncMock()
    client.start_session = MagicMock(return_value=session)

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    await driver.begin()
    with pytest.raises(RuntimeError, match="boom"):
        await driver.commit()
    session.end_session.assert_awaited_once()
    assert not driver.in_transaction


async def test_fallback_options_use_pymongo_names(mongo_client) -> None:
    _, database = mongo_client
    collection = database.__getitem__.return_value
    collection.count_documents = AsyncMock(return_value=1)
    collection.distinct = AsyncMock(return_value=[])
    collection.delete_many = AsyncMock(return_value=MagicMock(deleted_count=2))
    collection.aggregate = AsyncMock(return_value=_AsyncRows([]))

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    base = {"collection": "u", "filter": {}}

    await driver.execute_operation(
        {**base, "operation": "count_documents", "max_time_ms": 5, "hint": "a_1"}
    )
    collection.count_documents.assert_awaited_once_with({}, hint="a_1", maxTimeMS=5)
    await driver.execute_operation(
        {**base, "operation": "distinct", "key": "k", "max_time_ms": 5}
    )
    collection.distinct.assert_awaited_once_with("k", {}, maxTimeMS=5)
    await driver.execute_operation(
        {**base, "operation": "delete_many", "collation": {"locale": "en"}}
    )
    collection.delete_many.assert_awaited_once_with({}, collation={"locale": "en"})
    await driver.fetch_operation(
        {"operation": "aggregate", "collection": "u", "pipeline": [], "max_time_ms": 5}
    )
    collection.aggregate.assert_awaited_once_with([], maxTimeMS=5)


def test_pymongo_bulk_requests_keep_options() -> None:
    collation = {"locale": "en"}
    update = mongo._pymongo_request(
        {
            "update": {
                "q": {},
                "u": {"$set": {"g.$[x]": 1}},
                "multi": True,
                "upsert": False,
                "arrayFilters": [{"x": 1}],
                "collation": collation,
                "hint": "a_1",
            }
        }
    )
    assert update == mongo.pymongo.UpdateMany(
        {},
        {"$set": {"g.$[x]": 1}},
        upsert=False,
        array_filters=[{"x": 1}],
        collation=collation,
        hint="a_1",
    )
    delete = mongo._pymongo_request(
        {"delete": {"q": {}, "limit": 1, "collation": collation}}
    )
    assert delete == mongo.pymongo.DeleteOne({}, collation=collation)
    replace = mongo._pymongo_request(
        {
            "replace": {
                "q": {},
                "u": {"a": 1},
                "multi": False,
                "upsert": True,
                "hint": "h",
            }
        }
    )
    assert replace == mongo.pymongo.ReplaceOne({}, {"a": 1}, upsert=True, hint="h")


async def test_admin_operations_use_pymongo(monkeypatch, mongo_client) -> None:
    _, database = mongo_client
    native = _native_engine(monkeypatch)
    native.execute_bson = AsyncMock()
    collection = database.__getitem__.return_value
    collection.create_index = AsyncMock(return_value="a_1")
    collection.drop_index = AsyncMock()
    collection.drop = AsyncMock()

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    base = {"collection": "u"}

    assert (
        await driver.execute_operation(
            {
                **base,
                "operation": "create_index",
                "keys": {"a": 1},
                "expire_after_seconds": 5,
                "partial_filter_expression": {"a": 1},
                "unique": None,
            }
        )
        == "a_1"
    )
    collection.create_index.assert_awaited_once_with(
        [("a", 1)], expireAfterSeconds=5, partialFilterExpression={"a": 1}
    )
    await driver.execute_operation({**base, "operation": "drop_index", "index": "a_1"})
    collection.drop_index.assert_awaited_with("a_1")
    await driver.execute_operation(
        {**base, "operation": "drop_index", "index": {"a": -1}}
    )
    collection.drop_index.assert_awaited_with([("a", -1)])
    await driver.execute_operation({**base, "operation": "drop"})
    collection.drop.assert_awaited_once_with()
    native.execute_bson.assert_not_awaited()


class _FakeBridge:
    def __init__(self, event_loop):
        self.event_loop = event_loop
        self._reader, self._writer = socket.socketpair()
        self.fd = self._reader.fileno()
        self.drained = 0

    def drain(self):
        self.drained += 1
        self._reader.recv(64)


async def test_bridge_attaches_to_running_loop(monkeypatch, mongo_client) -> None:
    native = _native_engine(monkeypatch)
    native.use_bridge = MagicMock()
    mongo.auniqorm_native.Bridge = _FakeBridge

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()

    event_loop, bridge = driver._bridge
    assert event_loop is asyncio.get_running_loop()
    assert bridge.event_loop is event_loop
    native.use_bridge.assert_called_once_with(bridge)
    bridge._writer.send(b"x")
    await asyncio.sleep(0.01)
    assert bridge.drained == 1

    await driver.disconnect()
    assert driver._bridge is None
    bridge._writer.send(b"x")
    await asyncio.sleep(0.01)
    assert bridge.drained == 1


async def test_bridge_skipped_when_unavailable(monkeypatch, mongo_client) -> None:
    native = _native_engine(monkeypatch)
    native.use_bridge = MagicMock()

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    assert driver._bridge is None

    mongo.auniqorm_native.Bridge = _FakeBridge
    event_loop = asyncio.get_running_loop()
    monkeypatch.setattr(
        event_loop, "add_reader", MagicMock(side_effect=NotImplementedError)
    )
    await driver.disconnect()
    await driver.connect()
    assert driver._bridge is None
    native.use_bridge.assert_not_called()


def test_native_url_strips_python_codec_options() -> None:
    assert mongo._native_url("mongodb://h/db") == "mongodb://h/db"
    assert (
        mongo._native_url("mongodb://h/db?uuidRepresentation=standard&tz_aware=true")
        == "mongodb://h/db"
    )
    assert (
        mongo._native_url("mongodb://a,b/db?replicaSet=rs&TZ_AWARE=1&w=majority")
        == "mongodb://a,b/db?replicaSet=rs&w=majority"
    )


async def test_connect_falls_back_when_native_rejects_url(
    monkeypatch, mongo_client, caplog
) -> None:
    monkeypatch.setattr(
        mongo,
        "auniqorm_native",
        SimpleNamespace(
            connect_mongo=AsyncMock(side_effect=RuntimeError("bad option"))
        ),
    )

    driver = mongo.MongoDriver("mongodb://localhost/example?appName=x")
    await driver.connect()

    assert driver.is_connected
    assert driver._native is None
    assert "bad option" in caplog.text


async def test_transaction_lifecycle(mongo_client) -> None:
    client, _ = mongo_client
    session = MagicMock()
    session.start_transaction = AsyncMock()
    session.commit_transaction = AsyncMock()
    session.abort_transaction = AsyncMock()
    session.end_session = AsyncMock()
    client.start_session = MagicMock(return_value=session)

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    await driver.begin()
    assert driver.in_transaction
    await driver.commit()
    session.start_transaction.assert_awaited_once_with()
    session.commit_transaction.assert_awaited_once()
    session.end_session.assert_awaited_once()
    assert not driver.in_transaction


async def test_invalid_query_is_reported(mongo_client) -> None:
    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    with pytest.raises(QueryError, match="JSON operation"):
        await driver.fetch_all("SELECT * FROM users")


class _AsyncRows:
    def __init__(self, rows, error=None):
        self._rows = rows
        self._error = error
        self.closed = False

    def batch_size(self, _size):
        return self

    def __aiter__(self):
        return self._iter()

    async def _iter(self):
        for row in self._rows:
            yield row
        if self._error:
            raise self._error

    async def close(self):
        self.closed = True


async def test_transactions_are_scoped_to_tasks(monkeypatch, mongo_client) -> None:
    native = _native_engine(monkeypatch)
    sessions = []

    def begin_transaction():
        session = MagicMock(
            commit=AsyncMock(),
            abort=AsyncMock(),
            end_if_unused=MagicMock(return_value=False),
        )
        sessions.append(session)
        return session

    native.begin_transaction = begin_transaction
    native.execute_bson = AsyncMock(return_value=encode({"result": 1}))
    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    update = {"operation": "update_one", "collection": "u", "filter": {}}
    update["update"] = {"$set": {"a": 1}}
    started = asyncio.Event()
    release = asyncio.Event()

    async def in_transaction(name):
        await driver.begin()
        started.set()
        await release.wait()
        await driver.execute_operation(update)
        child = asyncio.create_task(driver.execute_operation(update))
        await child
        await driver.commit()
        return name

    async def outside():
        await started.wait()
        assert not driver.in_transaction
        await driver.execute_operation(update)
        release.set()

    results = await asyncio.gather(in_transaction("a"), in_transaction("b"), outside())

    assert results[:2] == ["a", "b"]
    assert len(sessions) == 2
    calls = [call.args[1] for call in native.execute_bson.await_args_list]
    assert calls.count(None) == 1
    assert calls.count(sessions[0]) == 2
    assert calls.count(sessions[1]) == 2
    assert all(session.commit.await_count == 1 for session in sessions)
    assert not driver.in_transaction


async def test_native_counts_skip_bson_decoding(monkeypatch, mongo_client) -> None:
    native = _native_engine(monkeypatch)
    native.execute_bson = AsyncMock(return_value=3)
    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()

    command = {"operation": "delete_many", "collection": "u", "filter": {}}
    assert await driver.execute_operation(command) == 3
    assert native.execute_bson.await_args.args[1] is None
