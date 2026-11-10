# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Mongo batch sizes are capped."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import auniqorm.drivers.mongo as mongo
from auniqorm.collection import MAX_BATCH_SIZE, Collection
from auniqorm.exceptions import QueryError


class TestMongoBatchSizeCap:
    async def test_collection_rejects_oversized_batches(self):
        collection = Collection(MagicMock(), "items")

        with pytest.raises(ValueError, match="at most"):
            async for _ in collection.iterate_batches(batch_size=MAX_BATCH_SIZE + 1):
                pass

    @pytest.mark.parametrize("batch_size", [MAX_BATCH_SIZE + 1, 2**31])
    def test_json_operations_reject_oversized_batches(self, batch_size):
        query = (
            '{"operation":"find","collection":"c","batch_size":' + str(batch_size) + "}"
        )
        with pytest.raises(QueryError, match="batch_size"):
            mongo.MongoDriver._command(query, None)

    async def test_driver_iteration_rejects_oversized_batches(self):
        driver = mongo.MongoDriver("mongodb://localhost/example")
        command = {"operation": "find", "collection": "c"}

        with pytest.raises(QueryError, match="batch_size"):
            async for _ in driver.iterate_operation(
                command, batch_size=MAX_BATCH_SIZE + 1
            ):
                pass

    def test_the_cap_itself_is_allowed(self):
        query = (
            '{"operation":"find","collection":"c","batch_size":'
            + str(MAX_BATCH_SIZE)
            + "}"
        )
        assert mongo.MongoDriver._command(query, None)["batch_size"] == MAX_BATCH_SIZE
