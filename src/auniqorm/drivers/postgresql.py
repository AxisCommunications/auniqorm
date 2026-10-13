# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""PostgreSQL driver using asyncpg."""

import json
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from auniqorm.drivers import BaseDriver
from auniqorm.exceptions import QueryError

# Optional dependency: absent at runtime is handled by the None guard in
# __init__, but checkers must see the real module to resolve its types.
try:
    import asyncpg
except ImportError:
    if TYPE_CHECKING:
        raise
    asyncpg = None


async def _init_connection(conn: "asyncpg.Connection") -> None:
    """Register JSON/JSONB codecs so values are auto-decoded."""
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    await conn.set_type_codec(
        "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )


class PostgresAsyncpgDriver(BaseDriver):
    """PostgreSQL database driver using asyncpg.

    Uses the connection pool for all non-transaction operations.
    Transactions get a dedicated connection for isolation.
    """

    def __init__(self, url: str, **options: Any) -> None:
        super().__init__(url, **options)
        if asyncpg is None:
            raise ImportError(
                "asyncpg is required for PostgreSQL support. "
                "Install it with: pip install auniqorm[postgres]"
            )
        self._pool: asyncpg.Pool | None = None
        self._txn_conn: asyncpg.Connection | None = None  # transaction connection

    @property
    def is_connected(self) -> bool:
        return self._pool is not None

    @property
    def in_transaction(self) -> bool:
        return self._txn_conn is not None

    async def connect(self) -> None:
        if self._pool is not None:
            return

        min_size = self._options.get("min_size", 2)
        max_size = self._options.get("max_size", 10)

        self._pool = await asyncpg.create_pool(
            self._url,
            min_size=min_size,
            max_size=max_size,
            init=_init_connection,
            **{
                k: v
                for k, v in self._options.items()
                if k not in ("min_size", "max_size")
            },
        )

    async def disconnect(self) -> None:
        if self._pool is not None:
            if self._txn_conn is not None:
                await self._pool.release(self._txn_conn)
                self._txn_conn = None
            await self._pool.close()
            self._pool = None

    async def execute(self, query: str, values: list[Any] | None = None) -> Any:
        if self._txn_conn is not None:
            conn = self._txn_conn
            try:
                if values:
                    return await conn.execute(query, *values)
                return await conn.execute(query)
            except Exception as e:
                raise QueryError(f"Execute failed: {e}") from e

        if self._pool is None:
            raise QueryError("Not connected")
        async with self._pool.acquire() as conn:
            try:
                if values:
                    return await conn.execute(query, *values)
                return await conn.execute(query)
            except Exception as e:
                raise QueryError(f"Execute failed: {e}") from e

    async def execute_many(self, query: str, values_list: list[list[Any]]) -> None:
        if self._txn_conn is not None:
            try:
                await self._txn_conn.executemany(query, values_list)
                return
            except Exception as e:
                raise QueryError(f"Execute many failed: {e}") from e

        if self._pool is None:
            raise QueryError("Not connected")
        async with self._pool.acquire() as conn:
            try:
                await conn.executemany(query, values_list)
            except Exception as e:
                raise QueryError(f"Execute many failed: {e}") from e

    async def fetch_one(
        self, query: str, values: list[Any] | None = None
    ) -> dict[str, Any] | None:
        if self._txn_conn is not None:
            conn = self._txn_conn
            try:
                if values:
                    row = await conn.fetchrow(query, *values)
                else:
                    row = await conn.fetchrow(query)
                return dict(row) if row is not None else None
            except Exception as e:
                raise QueryError(f"Fetch one failed: {e}") from e

        if self._pool is None:
            raise QueryError("Not connected")
        async with self._pool.acquire() as conn:
            try:
                if values:
                    row = await conn.fetchrow(query, *values)
                else:
                    row = await conn.fetchrow(query)
                return dict(row) if row is not None else None
            except Exception as e:
                raise QueryError(f"Fetch one failed: {e}") from e

    async def fetch_all(
        self, query: str, values: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        if self._txn_conn is not None:
            conn = self._txn_conn
            try:
                if values:
                    rows = await conn.fetch(query, *values)
                else:
                    rows = await conn.fetch(query)
                return [dict(row) for row in rows]
            except Exception as e:
                raise QueryError(f"Fetch all failed: {e}") from e

        if self._pool is None:
            raise QueryError("Not connected")
        async with self._pool.acquire() as conn:
            try:
                if values:
                    rows = await conn.fetch(query, *values)
                else:
                    rows = await conn.fetch(query)
                return [dict(row) for row in rows]
            except Exception as e:
                raise QueryError(f"Fetch all failed: {e}") from e

    async def fetch_val(
        self, query: str, values: list[Any] | None = None, column: int = 0
    ) -> Any:
        if self._txn_conn is not None:
            conn = self._txn_conn
            try:
                if values:
                    return await conn.fetchval(query, *values, column=column)
                return await conn.fetchval(query, column=column)
            except Exception as e:
                raise QueryError(f"Fetch val failed: {e}") from e

        if self._pool is None:
            raise QueryError("Not connected")
        async with self._pool.acquire() as conn:
            try:
                if values:
                    return await conn.fetchval(query, *values, column=column)
                return await conn.fetchval(query, column=column)
            except Exception as e:
                raise QueryError(f"Fetch val failed: {e}") from e

    async def begin(self) -> None:
        if self._pool is None:
            raise QueryError("Not connected")
        self._txn_conn = await self._pool.acquire()
        await self._txn_conn.execute("BEGIN")

    async def acquire(self) -> None:
        """Pin a separate connection (for explicit connection() context)."""
        if self._pool is None:
            raise QueryError("Not connected")
        self._txn_conn = await self._pool.acquire()

    async def release(self) -> None:
        """Release the separately pinned connection."""
        if self._txn_conn is not None and self._pool is not None:
            await self._pool.release(self._txn_conn)
            self._txn_conn = None

    async def commit(self) -> None:
        if self._txn_conn is not None:
            await self._txn_conn.execute("COMMIT")
            await self._pool.release(self._txn_conn)  # type: ignore[union-attr]
            self._txn_conn = None

    async def rollback(self) -> None:
        if self._txn_conn is not None:
            await self._txn_conn.execute("ROLLBACK")
            await self._pool.release(self._txn_conn)  # type: ignore[union-attr]
            self._txn_conn = None

    async def iterate(
        self,
        query: str,
        values: list[Any] | None = None,
        *,
        batch_size: int = 500,
    ) -> AsyncIterator[dict[str, Any]]:
        if self._pool is None:
            raise QueryError("Not connected")

        conn = self._txn_conn
        acquired = conn is None
        if acquired:
            conn = await self._pool.acquire()
        try:
            assert conn is not None
            async with conn.transaction():
                cursor = conn.cursor(query, *(values or []))
                async for record in cursor:
                    yield dict(record)
        except Exception as e:
            raise QueryError(f"Iterate failed: {e}") from e
        finally:
            if acquired:
                await self._pool.release(conn)

    async def iterate_batches(
        self,
        query: str,
        values: list[Any] | None = None,
        *,
        batch_size: int = 500,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        if self._pool is None:
            raise QueryError("Not connected")

        conn = self._txn_conn
        acquired = conn is None
        if acquired:
            conn = await self._pool.acquire()
        try:
            assert conn is not None
            async with conn.transaction():
                cursor = conn.cursor(query, *(values or []))
                await cursor.forward(0)  # open the cursor
                while True:
                    rows = await cursor.fetch(batch_size)
                    if not rows:
                        break
                    yield [dict(r) for r in rows]
        except Exception as e:
            raise QueryError(f"Iterate failed: {e}") from e
        finally:
            if acquired:
                await self._pool.release(conn)
