# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""SQLite driver using aiosqlite."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import nullcontext
from typing import TYPE_CHECKING, Any

from auniqorm.drivers import BaseDriver
from auniqorm.exceptions import QueryError

# Optional dependency: absent at runtime is handled by the None guard in
# __init__, but checkers must see the real module to resolve its types.
try:
    import aiosqlite
except ImportError:
    if TYPE_CHECKING:
        raise
    aiosqlite = None


class SQLiteDriver(BaseDriver):
    """SQLite database driver using aiosqlite."""

    def __init__(self, url: str, **options: Any) -> None:
        super().__init__(url, **options)
        if aiosqlite is None:
            raise ImportError(
                "aiosqlite is required for SQLite support. "
                "Install it with: pip install auniqorm[sqlite]"
            )
        self._db: aiosqlite.Connection | None = None
        # Held by the open transaction; other tasks wait for it.
        self._gate = asyncio.Lock()

    def _get_db_path(self) -> str:
        """Extract database path from URL.

        Supports:
            - sqlite:///path/to/db.sqlite (absolute)
            - sqlite://./path/to/db.sqlite (relative)
            - sqlite://:memory: (in-memory)
        """
        from urllib.parse import urlparse

        parsed = urlparse(self._url)
        path = parsed.path

        # Handle in-memory database
        if parsed.netloc == ":memory:" or path == "/:memory:":
            return ":memory:"

        # Remove leading slash for absolute paths on Unix
        # Keep double slash for Windows absolute paths
        if path.startswith("///"):
            return path[3:]
        elif path.startswith("/./"):
            return path[3:]
        elif path.startswith("/"):
            return path[1:]

        return path

    @property
    def is_connected(self) -> bool:
        return self._db is not None

    @property
    def in_transaction(self) -> bool:
        return self._scope_in_transaction()

    async def connect(self) -> None:
        if self._db is not None:
            return

        db_path = self._get_db_path()
        self._db = await aiosqlite.connect(db_path, **self._options)
        self._db.row_factory = aiosqlite.Row
        # Enable WAL mode for better concurrent write performance
        await self._db.execute("PRAGMA journal_mode=WAL")

    async def disconnect(self) -> None:
        if self._db is not None:
            await self._release_all_scopes()
            await self._db.close()
            self._db = None

    def _lock(self) -> tuple[bool, Any]:
        """Whether the task owns the open transaction, and what to hold.

        Other tasks' statements run under the transaction gate, so they never
        execute inside, read, or commit another task's transaction on the
        shared connection.
        """
        scope = self._scope()
        owned = scope is not None and scope.depth > 0
        return owned, nullcontext() if owned else self._gate

    def _open_db(self) -> Any:
        if self._db is None:
            raise QueryError("Not connected")
        return self._db

    async def execute(self, query: str, values: list[Any] | None = None) -> Any:
        db = self._open_db()
        owned, lock = self._lock()
        try:
            async with lock:
                cursor = await db.execute(query, values or [])
                if not owned:
                    await db.commit()
                return cursor.lastrowid
        except Exception as e:
            raise QueryError(f"Execute failed: {e}") from e

    async def execute_many(self, query: str, values_list: list[list[Any]]) -> None:
        db = self._open_db()
        owned, lock = self._lock()
        try:
            async with lock:
                await db.executemany(query, values_list)
                if not owned:
                    await db.commit()
        except Exception as e:
            raise QueryError(f"Execute many failed: {e}") from e

    async def _fetch(
        self, query: str, values: list[Any] | None, many: bool, action: str
    ) -> Any:
        db = self._open_db()
        _, lock = self._lock()
        try:
            async with lock:
                cursor = await db.execute(query, values or [])
                return await (cursor.fetchall() if many else cursor.fetchone())
        except Exception as e:
            raise QueryError(f"{action} failed: {e}") from e

    async def fetch_one(
        self, query: str, values: list[Any] | None = None
    ) -> dict[str, Any] | None:
        row = await self._fetch(query, values, False, "Fetch one")
        return None if row is None else dict(row)

    async def fetch_all(
        self, query: str, values: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        return [
            dict(row) for row in await self._fetch(query, values, True, "Fetch all")
        ]

    async def fetch_val(
        self, query: str, values: list[Any] | None = None, column: int = 0
    ) -> Any:
        row = await self._fetch(query, values, False, "Fetch val")
        return None if row is None else row[column]

    async def begin(self) -> None:
        self._open_db()
        await self._scoped_begin()

    async def acquire(self) -> None:
        """Pin to the current task; on one shared connection this only scopes
        transactions started inside the context."""
        self._open_db()
        await self._scoped_acquire()

    async def release(self) -> None:
        await self._scoped_release()

    async def commit(self) -> None:
        await self._scoped_end(commit=True)

    async def rollback(self) -> None:
        await self._scoped_end(commit=False)

    async def _pin_connection(self, *, begin: bool) -> tuple[Any, Any]:
        if begin:
            await self._start_transaction(None)
        return self._db, None

    async def _start_transaction(self, scope: Any) -> None:
        await self._gate.acquire()
        try:
            await self._open_db().execute("BEGIN")
        except BaseException:
            self._gate.release()
            raise

    async def _finish_transaction(self, scope: Any, *, commit: bool) -> None:
        try:
            db = self._open_db()
            await (db.commit() if commit else db.rollback())
        finally:
            self._gate.release()

    async def _scope_statement(self, scope: Any, sql: str) -> None:
        await self._open_db().execute(sql)

    async def _unpin_connection(self, scope: Any, end: str) -> None:
        if end != "release" or scope.depth:  # the gate is held only then
            await self._finish_transaction(scope, commit=end == "commit")

    async def _batches(
        self, query: str, values: list[Any] | None, batch_size: int
    ) -> AsyncIterator[list[Any]]:
        db = self._open_db()
        owned, _ = self._lock()
        # Outside a transaction, hold the gate per step rather than for the
        # whole iteration, so the consumer may run statements between batches.
        lock = nullcontext() if owned else self._gate
        try:
            async with lock:
                cursor = await db.execute(query, values or [])
            while True:
                async with lock:
                    rows = await cursor.fetchmany(batch_size)
                if not rows:
                    return
                yield rows
        except Exception as e:
            raise QueryError(f"Iterate failed: {e}") from e

    async def iterate(
        self,
        query: str,
        values: list[Any] | None = None,
        *,
        batch_size: int = 500,
    ) -> AsyncIterator[dict[str, Any]]:
        async for rows in self._batches(query, values, batch_size):
            for row in rows:
                yield dict(row)

    async def iterate_batches(
        self,
        query: str,
        values: list[Any] | None = None,
        *,
        batch_size: int = 500,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        async for rows in self._batches(query, values, batch_size):
            yield [dict(row) for row in rows]
