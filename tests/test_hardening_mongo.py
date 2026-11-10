# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Hardening of the MongoDB JSON operation surface."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from bson.codec_options import DEFAULT_CODEC_OPTIONS

import auniqorm.drivers.mongo as mongo
from auniqorm.exceptions import QueryError


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


def _nested(depth: int) -> str:
    return '{"a":' * depth + "1" + "}" * depth


class TestJsonOperationLimits:
    def test_rejects_oversized_documents(self):
        oversized = '{"operation":"find","collection":"c","pad":"' + "x" * 17_000_000
        with pytest.raises(QueryError, match="too large"):
            mongo.MongoDriver._command(oversized + '"}', None)

    @pytest.mark.parametrize("depth", [101, 5_000])
    def test_rejects_deep_nesting(self, depth):
        query = '{"operation":"find","collection":"c","filter":' + _nested(depth) + "}"
        with pytest.raises(QueryError, match="nested"):
            mongo.MongoDriver._command(query, None)

    def test_accepts_documents_at_the_nesting_limit(self):
        query = '{"operation":"find","collection":"c","filter":' + _nested(90) + "}"
        assert mongo.MongoDriver._command(query, None)["operation"] == "find"


class TestOptionMapping:
    async def _driver(self, mongo_client):
        _, database = mongo_client
        collection = database.__getitem__.return_value
        collection.update_one = AsyncMock(return_value=MagicMock(modified_count=1))
        driver = mongo.MongoDriver("mongodb://localhost/example")
        await driver.connect()
        return driver, collection

    QUERY = (
        '{"operation":"update_one","collection":"users","filter":{},'
        '"update":{"$set":{"a":1}}}'
    )

    async def test_values_may_set_documented_options(self, mongo_client):
        driver, collection = await self._driver(mongo_client)

        assert await driver.execute(self.QUERY, {"upsert": True}) == 1
        collection.update_one.assert_awaited_once_with(
            {}, {"$set": {"a": 1}}, upsert=True
        )

    @pytest.mark.parametrize(
        "option", ["comment", "bypass_document_validation", "session", "let"]
    )
    async def test_values_rejects_undocumented_options(self, mongo_client, option):
        driver, collection = await self._driver(mongo_client)

        with pytest.raises(QueryError, match=option):
            await driver.execute(self.QUERY, {option: True})
        collection.update_one.assert_not_awaited()

    async def test_values_cannot_override_explicit_options(self, mongo_client):
        driver, collection = await self._driver(mongo_client)
        query = self.QUERY[:-1] + ',"upsert":false}'

        with pytest.raises(QueryError, match="upsert"):
            await driver.execute(query, {"upsert": True})
        collection.update_one.assert_not_awaited()


class TestNativeCredentials:
    @pytest.mark.parametrize(
        "options",
        [
            {"username": "u", "password": "p"},
            {"password": "p", "username": "u"},
        ],
    )
    def test_username_precedes_password(self, options):
        assert mongo._native_url("mongodb://h/db", options) == "mongodb://u:p@h/db"

    def test_credentials_are_percent_encoded(self):
        url = mongo._native_url(
            "mongodb://h/db", {"password": "p@:/", "username": "u s"}
        )
        assert url == "mongodb://u%20s:p%40%3A%2F@h/db"
