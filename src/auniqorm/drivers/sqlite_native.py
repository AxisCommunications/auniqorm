# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Native SQLite driver backed by the Rust rusqlite engine.

Selected by default when the native extension is available. Set
``AUNIQORM_SQLITE_NATIVE=0`` to use aiosqlite instead. aiosqlite offloads every
statement to a background thread and hops the result back to the event loop;
this driver runs SQLite synchronously with the GIL released, so ``await``
resolves without a thread hop.

Transactions belong to the asyncio task that began them (and tasks it
starts). Because every statement shares one connection, other tasks wait
until an open transaction ends instead of running inside it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from auniqorm.drivers import BaseDriver
from auniqorm.exceptions import QueryError

try:
    import auniqorm_native
except ImportError:
    if TYPE_CHECKING:
        raise
    auniqorm_native = None


class SQLiteNativeDriver(BaseDriver):
    """SQLite driver using the Rust ``auniqorm_native`` engine."""

    #: Sentinel for the ORM's msgpack model-hydration fast path.
    SUPPORTS_MSGPACK = True

    def __init__(self, url: str, **options: Any) -> None:
        super().__init__(url, **options)
        if auniqorm_native is None:
            raise ImportError(
                "auniqorm-native is required for the native SQLite engine. "
                "Build it with: maturin develop --release (in rust/)."
            )
        self._engine: Any = None
        # Held by the open transaction; other tasks wait for it.
        self._gate = asyncio.Lock()

    def _get_db_path(self) -> str:
        """Extract the database path from the URL (mirrors the aiosqlite driver)."""
        from urllib.parse import urlparse

        parsed = urlparse(self._url)
        path = parsed.path

        if parsed.netloc == ":memory:" or path == "/:memory:":
            return ":memory:"
        if path.startswith("///"):
            return path[3:]
        if path.startswith("/./"):
            return path[3:]
        if path.startswith("/"):
            return path[1:]
        return path

    @property
    def is_connected(self) -> bool:
        return self._engine is not None

    @property
    def in_transaction(self) -> bool:
        return self._scope_in_transaction()

    async def connect(self) -> None:
        if self._engine is None:
            self._engine = auniqorm_native.connect_sqlite(self._get_db_path())

    async def disconnect(self) -> None:
        if self._engine is not None:
            await self._release_all_scopes()
            self._engine.close()
            self._engine = None

    async def _run(self, action: str, call: Callable[[Any], Any]) -> Any:
        """Run an engine call once no other task's transaction holds the
        connection. Calls are synchronous, so nothing interleaves after."""
        if self._engine is None:
            raise QueryError("Not connected")
        scope = self._scope()
        if self._gate.locked() and (scope is None or not scope.depth):
            async with self._gate:
                pass
        try:
            return call(self._engine)
        except Exception as e:
            raise QueryError(f"{action} failed: {e}") from e

    async def execute(self, query: str, values: list[Any] | None = None) -> Any:
        return await self._run("Execute", lambda e: e.execute(query, values))

    async def execute_many(self, query: str, values_list: list[list[Any]]) -> None:
        await self._run(
            "Execute many", lambda e: [e.execute(query, list(v)) for v in values_list]
        )

    async def fetch_one(
        self, query: str, values: list[Any] | None = None
    ) -> dict[str, Any] | None:
        return cast(
            "dict[str, Any] | None",
            await self._run("Fetch one", lambda e: e.fetch_one(query, values)),
        )

    async def fetch_all(
        self, query: str, values: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        return cast(
            "list[dict[str, Any]]",
            await self._run("Fetch all", lambda e: e.fetch_all(query, values)),
        )

    async def fetch_all_msgpack(
        self, query: str, values: list[Any] | None = None
    ) -> bytes:
        """Fetch rows as a msgpack buffer for msgspec to decode into Structs."""
        return cast(
            "bytes",
            await self._run(
                "Fetch all msgpack", lambda e: e.fetch_all_msgpack(query, values)
            ),
        )

    async def fetch_one_msgpack(
        self, query: str, values: list[Any] | None = None
    ) -> bytes | None:
        """Fetch a single row as a msgpack buffer, or None."""
        return cast(
            "bytes | None",
            await self._run(
                "Fetch one msgpack", lambda e: e.fetch_one_msgpack(query, values)
            ),
        )

    async def fetch_val(
        self, query: str, values: list[Any] | None = None, column: int = 0
    ) -> Any:
        row = await self.fetch_one(query, values)
        if row is None:
            return None
        cells = list(row.values())
        return cells[column] if 0 <= column < len(cells) else None

    async def begin(self) -> None:
        if self._engine is None:
            raise QueryError("Not connected")
        await self._scoped_begin()

    async def acquire(self) -> None:
        """Pin to the current task; on one shared connection this only scopes
        transactions started inside the context."""
        if self._engine is None:
            raise QueryError("Not connected")
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
        return self._engine, None

    async def _start_transaction(self, scope: Any) -> None:
        await self._gate.acquire()
        try:
            self._engine.begin()
        except BaseException as e:
            self._gate.release()
            raise QueryError(f"Begin failed: {e}") from e

    async def _finish_transaction(self, scope: Any, *, commit: bool) -> None:
        try:
            (self._engine.commit if commit else self._engine.rollback)()
        except Exception as e:
            if commit:
                try:
                    self._engine.rollback()
                except Exception as rollback_error:
                    raise QueryError(
                        f"Commit failed: {e}; rollback failed: {rollback_error}"
                    ) from e
            raise QueryError(f"{'Commit' if commit else 'Rollback'} failed: {e}") from e
        finally:
            self._gate.release()

    async def _scope_statement(self, scope: Any, sql: str) -> None:
        await self._run(sql, lambda e: e.execute(sql, None))

    async def _unpin_connection(self, scope: Any, end: str) -> None:
        if end != "release" or scope.depth:  # the gate is held only then
            await self._finish_transaction(scope, commit=end == "commit")
