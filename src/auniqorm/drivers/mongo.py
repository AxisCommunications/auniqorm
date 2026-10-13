# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Focused asynchronous MongoDB driver.

MongoDB operations are passed as JSON documents through the existing
``BaseDriver`` interface.  This keeps the connection and transaction APIs
consistent with the SQL drivers without pretending that SQL strings map
directly to document queries.  For example::

    await db.execute(
        '{"operation":"insert_one","collection":"users",'
        '"document":{"name":"Ada"}}'
    )
    users = await db.fetch_all(
        '{"operation":"find","collection":"users","filter":{"active":true}}'
    )

The official PyMongo async API is optional and imported lazily.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote, urlparse

from auniqorm.drivers import BaseDriver
from auniqorm.exceptions import QueryError

try:
    from pymongo import AsyncMongoClient
except ImportError:
    if TYPE_CHECKING:
        raise
    AsyncMongoClient = None  # type: ignore[assignment,misc]


class MongoDriver(BaseDriver):
    """MongoDB driver backed by PyMongo's native async client."""

    SUPPORTS_MODELS = False

    def __init__(self, url: str, **options: Any) -> None:
        super().__init__(url, **options)
        if AsyncMongoClient is None:
            raise ImportError(
                "pymongo is required for MongoDB support. "
                "Install it with: pip install auniqorm[mongodb]"
            )
        parsed = urlparse(url)
        database = parsed.path.lstrip("/")
        if not database:
            raise ValueError("MongoDB URL must include a database name")
        self._database_name = unquote(database.split("/", 1)[0])
        self._client: Any = None
        self._db: Any = None
        self._session: Any = None

    @property
    def is_connected(self) -> bool:
        return self._client is not None and self._db is not None

    @property
    def in_transaction(self) -> bool:
        return self._session is not None

    async def connect(self) -> None:
        if self.is_connected:
            return
        options = dict(self._options)
        self._client = AsyncMongoClient(self._url, **options)
        await self._client.aconnect()
        self._db = self._client[self._database_name]

    async def disconnect(self) -> None:
        if self._session is not None:
            await self.rollback()
        if self._client is not None:
            self._client.close()
        self._client = None
        self._db = None

    @staticmethod
    def _command(query: str, values: list[Any] | None) -> dict[str, Any]:
        try:
            command = json.loads(query)
        except (TypeError, json.JSONDecodeError) as exc:
            raise QueryError(
                "MongoDB queries must be JSON operation documents"
            ) from exc
        if not isinstance(command, dict) or "operation" not in command:
            raise QueryError("MongoDB query requires an 'operation' field")
        if values is not None:
            command.setdefault("values", values)
        return command

    def _collection(self, command: dict[str, Any]) -> Any:
        if self._db is None:
            raise QueryError("Not connected")
        collection = command.get("collection")
        if not isinstance(collection, str) or not collection:
            raise QueryError("MongoDB operation requires a collection")
        return self._db[collection]

    def _kwargs(self, command: dict[str, Any]) -> dict[str, Any]:
        values = command.get("values")
        return values if isinstance(values, dict) else {}

    async def execute(self, query: str, values: list[Any] | None = None) -> Any:
        command = self._command(query, values)
        collection = self._collection(command)
        operation = command["operation"]
        try:
            if operation == "insert_one":
                result = await collection.insert_one(
                    command["document"], session=self._session
                )
                return result.inserted_id
            if operation == "insert_many":
                result = await collection.insert_many(
                    command["documents"], session=self._session
                )
                return result.inserted_ids
            if operation in {"update_one", "update_many", "replace_one"}:
                method = getattr(collection, operation)
                result = await method(
                    command.get("filter", {}),
                    command.get("update", command.get("replacement")),
                    session=self._session,
                    **self._kwargs(command),
                )
                return result.modified_count
            if operation in {"delete_one", "delete_many"}:
                result = await getattr(collection, operation)(
                    command.get("filter", {}), session=self._session
                )
                return result.deleted_count
            if operation == "drop":
                return await collection.drop(session=self._session)
            if operation == "create_index":
                return await collection.create_index(
                    command["keys"], session=self._session, **self._kwargs(command)
                )
            raise QueryError(f"Unsupported MongoDB execute operation: {operation}")
        except QueryError:
            raise
        except Exception as exc:
            raise QueryError(f"MongoDB execute failed: {exc}") from exc

    async def execute_many(self, query: str, values_list: list[list[Any]]) -> None:
        for values in values_list:
            await self.execute(query, values)

    async def fetch_one(
        self, query: str, values: list[Any] | None = None
    ) -> dict[str, Any] | None:
        command = self._command(query, values)
        if command["operation"] != "find_one":
            raise QueryError("fetch_one requires the find_one operation")
        try:
            result = await self._collection(command).find_one(
                command.get("filter", {}),
                session=self._session,
                **self._kwargs(command),
            )
            return dict(result) if result is not None else None
        except Exception as exc:
            raise QueryError(f"MongoDB fetch_one failed: {exc}") from exc

    async def fetch_all(
        self, query: str, values: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        command = self._command(query, values)
        if command["operation"] != "find":
            raise QueryError("fetch_all requires the find operation")
        try:
            cursor = self._collection(command).find(
                command.get("filter", {}),
                session=self._session,
                **self._kwargs(command),
            )
            return [dict(row) async for row in cursor]
        except Exception as exc:
            raise QueryError(f"MongoDB fetch_all failed: {exc}") from exc

    async def fetch_val(
        self, query: str, values: list[Any] | None = None, column: int = 0
    ) -> Any:
        row = await self.fetch_one(query, values)
        if row is None:
            return None
        return next(iter(row.values())) if column == 0 else list(row.values())[column]

    async def begin(self) -> None:
        if self._client is None:
            raise QueryError("Not connected")
        if self._session is not None:
            raise QueryError("Transaction already active")
        self._session = await self._client.start_session()
        self._session.start_transaction()

    async def commit(self) -> None:
        if self._session is not None:
            await self._session.commit_transaction()
            await self._session.end_session()
            self._session = None

    async def rollback(self) -> None:
        if self._session is not None:
            await self._session.abort_transaction()
            await self._session.end_session()
            self._session = None

    async def iterate(
        self, query: str, values: list[Any] | None = None, *, batch_size: int = 500
    ) -> AsyncIterator[dict[str, Any]]:
        command = self._command(query, values)
        if command["operation"] != "find":
            raise QueryError("iterate requires the find operation")
        cursor = self._collection(command).find(
            command.get("filter", {}), session=self._session, **self._kwargs(command)
        )
        cursor.batch_size(batch_size)
        async for row in cursor:
            yield dict(row)
