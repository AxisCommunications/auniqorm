"""Unit tests for the MongoDB driver without a live MongoDB server."""

from unittest.mock import AsyncMock, MagicMock

import pytest

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
    database = MagicMock()
    client.__getitem__.return_value = database
    monkeypatch.setattr(mongo, "AsyncMongoClient", MagicMock(return_value=client))
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
    collection.insert_one.assert_awaited_once_with({"name": "Ada"}, session=None)


async def test_fetch_all_and_iteration(mongo_client) -> None:
    _, database = mongo_client
    collection = database.__getitem__.return_value
    collection.find.return_value = _AsyncRows([{"_id": 1}, {"_id": 2}])

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    query = '{"operation":"find","collection":"users","filter":{"active":true}}'
    assert await driver.fetch_all(query) == [{"_id": 1}, {"_id": 2}]
    assert [row async for row in driver.iterate(query, batch_size=10)] == [
        {"_id": 1},
        {"_id": 2},
    ]


async def test_transaction_lifecycle(mongo_client) -> None:
    client, _ = mongo_client
    session = MagicMock()
    session.commit_transaction = AsyncMock()
    session.abort_transaction = AsyncMock()
    session.end_session = AsyncMock()
    client.start_session = AsyncMock(return_value=session)

    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    await driver.begin()
    assert driver.in_transaction
    await driver.commit()
    session.start_transaction.assert_called_once_with()
    session.commit_transaction.assert_awaited_once()
    session.end_session.assert_awaited_once()
    assert not driver.in_transaction


async def test_invalid_query_is_reported(mongo_client) -> None:
    driver = mongo.MongoDriver("mongodb://localhost/example")
    await driver.connect()
    with pytest.raises(QueryError, match="JSON operation"):
        await driver.fetch_all("SELECT * FROM users")


class _AsyncRows:
    def __init__(self, rows):
        self._rows = rows

    def batch_size(self, _size):
        return self

    def __aiter__(self):
        return self._iter()

    async def _iter(self):
        for row in self._rows:
            yield row
