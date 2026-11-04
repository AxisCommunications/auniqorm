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
        self._autocommit = True

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
        return self._scope_in_transaction()

    async def connect(self) -> None:
        if self._pool is not None:
            return

        conn_params = self._parse_url()
        minsize = self._options.get("min_size", 1)
        maxsize = self._options.get("max_size", 10)
        # Autocommit by default: otherwise every pooled read opens an InnoDB
        # transaction that keeps a stale snapshot and metadata locks on the
        # connection after it returns to the pool.
        autocommit = self._options.get(
            "autocommit", conn_params.pop("autocommit", True)
        )
        if isinstance(autocommit, str):
            autocommit = autocommit.strip().lower() in {"1", "true", "yes", "on"}
        self._autocommit = bool(autocommit)

        self._pool = await aiomysql.create_pool(
            minsize=minsize,
            maxsize=maxsize,
            autocommit=self._autocommit,
            **conn_params,
            **{
                k: v
                for k, v in self._options.items()
                if k not in ("min_size", "max_size", "autocommit")
            },
        )

    async def disconnect(self) -> None:
        if self._pool is not None:
            await self._release_all_scopes()
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    async def execute(self, query: str, values: list[Any] | None = None) -> Any:
        if self._pool is None:
            raise QueryError("Not connected")

        try:
            scope = self._scope()
            if scope is not None:
                async with scope.conn.cursor() as cursor:
                    await cursor.execute(query, values or ())
                    if not scope.depth and not self._autocommit:
                        await scope.conn.commit()
                    return cursor.lastrowid
            else:
                async with self._pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute(query, values or ())
                        if not self._autocommit:
                            await conn.commit()
                        return cursor.lastrowid
        except Exception as e:
            raise QueryError(f"Execute failed: {e}") from e

    async def execute_many(self, query: str, values_list: list[list[Any]]) -> None:
        if self._pool is None:
            raise QueryError("Not connected")

        try:
            scope = self._scope()
            if scope is not None:
                async with scope.conn.cursor() as cursor:
                    await cursor.executemany(query, values_list)
                if not scope.depth and not self._autocommit:
                    await scope.conn.commit()
            else:
                async with self._pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.executemany(query, values_list)
                    if not self._autocommit:
                        await conn.commit()
        except Exception as e:
            raise QueryError(f"Execute many failed: {e}") from e

    async def fetch_one(
        self, query: str, values: list[Any] | None = None
    ) -> dict[str, Any] | None:
        if self._pool is None:
            raise QueryError("Not connected")

        try:
            scope = self._scope()
            if scope is not None:
                async with scope.conn.cursor(aiomysql.DictCursor) as cursor:
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
            scope = self._scope()
            if scope is not None:
                async with scope.conn.cursor(aiomysql.DictCursor) as cursor:
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
            scope = self._scope()
            if scope is not None:
                async with scope.conn.cursor() as cursor:
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
        await self._scoped_begin()

    async def acquire(self) -> None:
        """Pin a connection to the current task (explicit connection() context)."""
        if self._pool is None:
            raise QueryError("Not connected")
        await self._scoped_acquire()

    async def release(self) -> None:
        """Release the current task's pinned connection."""
        await self._scoped_release()

    async def commit(self) -> None:
        await self._scoped_end(commit=True)

    async def rollback(self) -> None:
        await self._scoped_end(commit=False)

    async def _pin_connection(self, *, begin: bool) -> tuple[Any, Any]:
        assert self._pool is not None
        conn = await self._pool.acquire()
        if begin:
            try:
                await conn.begin()
            except BaseException:
                self._pool.release(conn)
                raise
        return conn, None

    async def _scope_statement(self, scope: Any, sql: str) -> None:
        async with scope.conn.cursor() as cursor:
            await cursor.execute(sql)

    async def _unpin_connection(self, scope: Any, end: str) -> None:
        try:
            await (scope.conn.commit() if end == "commit" else scope.conn.rollback())
        finally:
            # aiomysql closes connections released inside a transaction.
            if self._pool is not None:
                self._pool.release(scope.conn)

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
            scope = self._scope()
            if scope is not None:
                async with scope.conn.cursor(aiomysql.SSDictCursor) as cursor:
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
            scope = self._scope()
            if scope is not None:
                async with scope.conn.cursor(aiomysql.SSDictCursor) as cursor:
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
