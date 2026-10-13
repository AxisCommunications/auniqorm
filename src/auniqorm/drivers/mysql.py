# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""MySQL driver using aiomysql."""

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, cast

from auniqorm.drivers import BaseDriver
from auniqorm.exceptions import QueryError

# Optional dependency: absent at runtime is handled by the None guard in
# __init__, but checkers must see the real module to resolve its types.
try:
    import aiomysql
except ImportError:
    if TYPE_CHECKING:
        raise
    aiomysql = None


class MySQLDriver(BaseDriver):
    """MySQL database driver using aiomysql."""

    def __init__(self, url: str, **options: Any) -> None:
        super().__init__(url, **options)
        if aiomysql is None:
            raise ImportError(
                "aiomysql is required for MySQL support. "
                "Install it with: pip install auniqorm[mysql]"
            )
        self._pool: aiomysql.Pool | None = None
        self._transaction_conn: aiomysql.Connection | None = None

    def _parse_url(self) -> dict[str, Any]:
        """Parse MySQL URL into connection parameters."""
        from urllib.parse import parse_qs, urlparse

        parsed = urlparse(self._url)

        params: dict[str, Any] = {
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 3306,
            "user": parsed.username or "root",
            "password": parsed.password or "",
            "db": parsed.path.lstrip("/") if parsed.path else None,
        }

        # Parse query string options
        if parsed.query:
            query_params = parse_qs(parsed.query)
            for key, values in query_params.items():
                params[key] = values[0] if len(values) == 1 else values

        return params

    @property
    def is_connected(self) -> bool:
        return self._pool is not None

    @property
    def in_transaction(self) -> bool:
        return self._transaction_conn is not None

    async def connect(self) -> None:
        if self._pool is not None:
            return

        conn_params = self._parse_url()
        minsize = self._options.get("min_size", 1)
        maxsize = self._options.get("max_size", 10)

        self._pool = await aiomysql.create_pool(
            minsize=minsize,
            maxsize=maxsize,
            **conn_params,
            **{
                k: v
                for k, v in self._options.items()
                if k not in ("min_size", "max_size")
            },
        )

    async def disconnect(self) -> None:
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    async def execute(self, query: str, values: list[Any] | None = None) -> Any:
        if self._pool is None:
            raise QueryError("Not connected")

        try:
            if self._transaction_conn:
                async with self._transaction_conn.cursor() as cursor:
                    await cursor.execute(query, values or ())
                    return cursor.lastrowid
            else:
                async with self._pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute(query, values or ())
                        await conn.commit()
                        return cursor.lastrowid
        except Exception as e:
            raise QueryError(f"Execute failed: {e}") from e

    async def execute_many(self, query: str, values_list: list[list[Any]]) -> None:
        if self._pool is None:
            raise QueryError("Not connected")

        try:
            if self._transaction_conn:
                async with self._transaction_conn.cursor() as cursor:
                    await cursor.executemany(query, values_list)
            else:
                async with self._pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.executemany(query, values_list)
                    await conn.commit()
        except Exception as e:
            raise QueryError(f"Execute many failed: {e}") from e

    async def fetch_one(
        self, query: str, values: list[Any] | None = None
    ) -> dict[str, Any] | None:
        if self._pool is None:
            raise QueryError("Not connected")

        try:
            if self._transaction_conn:
                async with self._transaction_conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(query, values or ())
                    return cast("dict[str, Any] | None", await cursor.fetchone())
            else:
                async with self._pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cursor:
                        await cursor.execute(query, values or ())
                        return cast("dict[str, Any] | None", await cursor.fetchone())
        except Exception as e:
            raise QueryError(f"Fetch one failed: {e}") from e

    async def fetch_all(
        self, query: str, values: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        if self._pool is None:
            raise QueryError("Not connected")

        try:
            if self._transaction_conn:
                async with self._transaction_conn.cursor(aiomysql.DictCursor) as cursor:
                    await cursor.execute(query, values or ())
                    rows = await cursor.fetchall()
                    return list(rows) if rows else []
            else:
                async with self._pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cursor:
                        await cursor.execute(query, values or ())
                        rows = await cursor.fetchall()
                        return list(rows) if rows else []
        except Exception as e:
            raise QueryError(f"Fetch all failed: {e}") from e

    async def fetch_val(
        self, query: str, values: list[Any] | None = None, column: int = 0
    ) -> Any:
        if self._pool is None:
            raise QueryError("Not connected")

        try:
            if self._transaction_conn:
                async with self._transaction_conn.cursor() as cursor:
                    await cursor.execute(query, values or ())
                    row = await cursor.fetchone()
                    return row[column] if row else None
            else:
                async with self._pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute(query, values or ())
                        row = await cursor.fetchone()
                        return row[column] if row else None
        except Exception as e:
            raise QueryError(f"Fetch val failed: {e}") from e

    async def begin(self) -> None:
        if self._pool is None:
            raise QueryError("Not connected")
        self._transaction_conn = await self._pool.acquire()
        await self._transaction_conn.begin()

    async def commit(self) -> None:
        if self._transaction_conn is not None:
            await self._transaction_conn.commit()
            self._pool.release(self._transaction_conn)  # type: ignore[union-attr]
            self._transaction_conn = None

    async def rollback(self) -> None:
        if self._transaction_conn is not None:
            await self._transaction_conn.rollback()
            self._pool.release(self._transaction_conn)  # type: ignore[union-attr]
            self._transaction_conn = None

    async def iterate(
        self,
        query: str,
        values: list[Any] | None = None,
        *,
        batch_size: int = 500,
    ) -> AsyncIterator[dict[str, Any]]:
        if self._pool is None:
            raise QueryError("Not connected")

        try:
            if self._transaction_conn:
                async with self._transaction_conn.cursor(
                    aiomysql.SSDictCursor
                ) as cursor:
                    await cursor.execute(query, values or ())
                    while True:
                        rows = await cursor.fetchmany(batch_size)
                        if not rows:
                            break
                        for row in rows:
                            yield row
            else:
                async with self._pool.acquire() as conn:
                    async with conn.cursor(aiomysql.SSDictCursor) as cursor:
                        await cursor.execute(query, values or ())
                        while True:
                            rows = await cursor.fetchmany(batch_size)
                            if not rows:
                                break
                            for row in rows:
                                yield row
        except Exception as e:
            raise QueryError(f"Iterate failed: {e}") from e

    async def iterate_batches(
        self,
        query: str,
        values: list[Any] | None = None,
        *,
        batch_size: int = 500,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        if self._pool is None:
            raise QueryError("Not connected")

        try:
            if self._transaction_conn:
                async with self._transaction_conn.cursor(
                    aiomysql.SSDictCursor
                ) as cursor:
                    await cursor.execute(query, values or ())
                    while True:
                        rows = await cursor.fetchmany(batch_size)
                        if not rows:
                            break
                        yield list(rows)
            else:
                async with self._pool.acquire() as conn:
                    async with conn.cursor(aiomysql.SSDictCursor) as cursor:
                        await cursor.execute(query, values or ())
                        while True:
                            rows = await cursor.fetchmany(batch_size)
                            if not rows:
                                break
                            yield list(rows)
        except Exception as e:
            raise QueryError(f"Iterate failed: {e}") from e
