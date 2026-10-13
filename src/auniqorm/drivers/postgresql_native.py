# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Native PostgreSQL driver backed by the Rust tokio-postgres engine.

Selected by default when the native extension is available. Set
``AUNIQORM_PG_NATIVE=0`` to use asyncpg instead. Supports real transactions:
``begin()`` pins a connection and issues ``BEGIN``; queries then route to that
connection until ``commit()``/``rollback()``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from auniqorm.drivers import BaseDriver
from auniqorm.exceptions import QueryError

try:
    import auniqorm_native
except ImportError:
    if TYPE_CHECKING:
        raise
    auniqorm_native = None


class PostgresNativeDriver(BaseDriver):
    """PostgreSQL driver using the Rust ``auniqorm_native`` engine."""

    #: Sentinel for the ORM's msgpack model-hydration fast path.
    SUPPORTS_MSGPACK = True

    def __init__(self, url: str, **options: Any) -> None:
        super().__init__(url, **options)
        if auniqorm_native is None:
            raise ImportError(
                "auniqorm-native is required for the native PostgreSQL engine. "
                "Build it with: maturin develop --release (in rust/)."
            )
        self._engine: Any = None
        self._txn: Any = None

    @property
    def is_connected(self) -> bool:
        return self._engine is not None

    @property
    def _active(self) -> Any:
        """The pinned transaction if one is open, else the pooled engine."""
        return self._txn if self._txn is not None else self._engine

    @property
    def in_transaction(self) -> bool:
        return self._txn is not None

    async def connect(self) -> None:
        if self._engine is None:
            self._engine = await auniqorm_native.connect_pg(self._url)

    async def disconnect(self) -> None:
        self._txn = None
        if self._engine is not None:
            await self._engine.close()
            self._engine = None

    async def execute(self, query: str, values: list[Any] | None = None) -> Any:
        if self._engine is None:
            raise QueryError("Not connected")
        try:
            return await self._active.execute(query, values)
        except Exception as e:
            raise QueryError(f"Execute failed: {e}") from e

    async def execute_many(self, query: str, values_list: list[list[Any]]) -> None:
        if self._engine is None:
            raise QueryError("Not connected")
        try:
            active = self._active
            for values in values_list:
                await active.execute(query, list(values))
        except Exception as e:
            raise QueryError(f"Execute many failed: {e}") from e

    async def fetch_one(
        self, query: str, values: list[Any] | None = None
    ) -> dict[str, Any] | None:
        if self._engine is None:
            raise QueryError("Not connected")
        try:
            return cast(
                "dict[str, Any] | None", await self._active.fetch_one(query, values)
            )
        except Exception as e:
            raise QueryError(f"Fetch one failed: {e}") from e

    async def fetch_all(
        self, query: str, values: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        if self._engine is None:
            raise QueryError("Not connected")
        try:
            return cast(
                "list[dict[str, Any]]", await self._active.fetch_all(query, values)
            )
        except Exception as e:
            raise QueryError(f"Fetch all failed: {e}") from e

    async def fetch_all_msgpack(
        self, query: str, values: list[Any] | None = None
    ) -> bytes:
        """Fetch rows as a msgpack buffer for msgspec to decode into Structs."""
        if self._engine is None:
            raise QueryError("Not connected")
        try:
            return cast("bytes", await self._active.fetch_all_msgpack(query, values))
        except Exception as e:
            raise QueryError(f"Fetch all msgpack failed: {e}") from e

    async def fetch_one_msgpack(
        self, query: str, values: list[Any] | None = None
    ) -> bytes | None:
        """Fetch a single row as a msgpack buffer, or None."""
        if self._engine is None:
            raise QueryError("Not connected")
        try:
            return cast(
                "bytes | None", await self._active.fetch_one_msgpack(query, values)
            )
        except Exception as e:
            raise QueryError(f"Fetch one msgpack failed: {e}") from e

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
        if self._txn is not None:
            return  # already in a transaction; nested BEGIN is a no-op
        try:
            self._txn = await self._engine.begin()
        except Exception as e:
            raise QueryError(f"Begin failed: {e}") from e

    async def commit(self) -> None:
        if self._txn is None:
            return
        txn, self._txn = self._txn, None
        try:
            await txn.commit()
        except Exception as e:
            raise QueryError(f"Commit failed: {e}") from e

    async def rollback(self) -> None:
        if self._txn is None:
            return
        txn, self._txn = self._txn, None
        try:
            await txn.rollback()
        except Exception as e:
            raise QueryError(f"Rollback failed: {e}") from e
