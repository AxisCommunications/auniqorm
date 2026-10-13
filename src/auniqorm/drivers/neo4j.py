# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Neo4j driver using the official asynchronous Python driver."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Mapping
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote, urlparse

from auniqorm.drivers import BaseDriver
from auniqorm.exceptions import QueryError

try:
    from neo4j import AsyncGraphDatabase
except ImportError:
    if TYPE_CHECKING:
        raise
    AsyncGraphDatabase = None


class Neo4jDriver(BaseDriver):
    """Focused Cypher backend backed by ``neo4j.AsyncGraphDatabase``."""

    SUPPORTS_MODELS = False

    def __init__(self, url: str, **options: Any) -> None:
        super().__init__(url, **options)
        if AsyncGraphDatabase is None:
            raise ImportError(
                "neo4j is required for Neo4j support. "
                "Install it with: pip install auniqorm[neo4j]"
            )
        self._driver: Any = None
        self._session: Any = None
        self._transaction: Any = None

    @property
    def is_connected(self) -> bool:
        return self._driver is not None

    @property
    def in_transaction(self) -> bool:
        return self._transaction is not None

    def _connection_options(self) -> dict[str, Any]:
        parsed = urlparse(self._url)
        options = dict(self._options)
        auth = options.pop("auth", None)
        if auth is None and parsed.username is not None:
            auth = (unquote(parsed.username), unquote(parsed.password or ""))
        if auth is not None:
            options["auth"] = auth
        return options

    def _database(self) -> str | None:
        database = self._options.get("database")
        if database is not None:
            return database
        path = urlparse(self._url).path.strip("/")
        return path or None

    @staticmethod
    def _parameters(
        query: str, values: list[Any] | Mapping[str, Any] | None
    ) -> tuple[str, dict[str, Any]]:
        if values is None:
            return query, {}
        if isinstance(values, Mapping):
            return query, dict(values)
        parameters = {f"p{i}": value for i, value in enumerate(values)}
        index = 0

        def replace(_match: re.Match[str]) -> str:
            nonlocal index
            name = f"$p{index}"
            index += 1
            return name

        return re.sub(r"\?", replace, query), parameters

    async def connect(self) -> None:
        if self._driver is None:
            self._driver = AsyncGraphDatabase.driver(
                self._url, **self._connection_options()
            )

    async def disconnect(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
            self._transaction = None
        if self._driver is not None:
            await self._driver.close()
            self._driver = None

    async def _run(self, query: str, values: list[Any] | None = None) -> Any:
        if self._driver is None:
            raise QueryError("Not connected")
        query, parameters = self._parameters(query, values)
        if self._transaction is not None:
            result = await self._transaction.run(query, parameters)
            return await result.consume()
        async with self._driver.session(database=self._database()) as session:
            result = await session.run(query, parameters)
            return await result.consume()

    async def execute(self, query: str, values: list[Any] | None = None) -> Any:
        try:
            return await self._run(query, values)
        except QueryError:
            raise
        except Exception as e:
            raise QueryError(f"Execute failed: {e}") from e

    async def execute_many(self, query: str, values_list: list[list[Any]]) -> None:
        for values in values_list:
            await self.execute(query, values)

    async def _records(
        self, query: str, values: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        try:
            if self._driver is None:
                raise QueryError("Not connected")
            query, parameters = self._parameters(query, values)
            if self._transaction is not None:
                result = await self._transaction.run(query, parameters)
                return [record.data() async for record in result]
            async with self._driver.session(database=self._database()) as session:
                result = await session.run(query, parameters)
                return [record.data() async for record in result]
        except QueryError:
            raise
        except Exception as e:
            raise QueryError(f"Fetch failed: {e}") from e

    async def fetch_one(
        self, query: str, values: list[Any] | None = None
    ) -> dict[str, Any] | None:
        rows = await self._records(query, values)
        return rows[0] if rows else None

    async def fetch_all(
        self, query: str, values: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        return await self._records(query, values)

    async def fetch_val(
        self, query: str, values: list[Any] | None = None, column: int = 0
    ) -> Any:
        row = await self.fetch_one(query, values)
        return list(row.values())[column] if row else None

    async def begin(self) -> None:
        if self._driver is None:
            raise QueryError("Not connected")
        if self._transaction is None:
            self._session = self._driver.session(database=self._database())
            self._transaction = await self._session.begin_transaction()

    async def commit(self) -> None:
        if self._transaction is not None:
            await self._transaction.commit()
            await self._session.close()
            self._transaction = None
            self._session = None

    async def rollback(self) -> None:
        if self._transaction is not None:
            await self._transaction.rollback()
            await self._session.close()
            self._transaction = None
            self._session = None

    async def iterate(
        self,
        query: str,
        values: list[Any] | None = None,
        *,
        batch_size: int = 500,
    ) -> AsyncIterator[dict[str, Any]]:
        del batch_size
        try:
            if self._driver is None:
                raise QueryError("Not connected")
            query, parameters = self._parameters(query, values)
            if self._transaction is not None:
                result = await self._transaction.run(query, parameters)
                async for record in result:
                    yield record.data()
            else:
                async with self._driver.session(database=self._database()) as session:
                    result = await session.run(query, parameters)
                    async for record in result:
                        yield record.data()
        except QueryError:
            raise
        except Exception as e:
            raise QueryError(f"Iterate failed: {e}") from e
