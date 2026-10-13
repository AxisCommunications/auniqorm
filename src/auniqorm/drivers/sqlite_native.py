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
        self._in_transaction = False

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
        return self._in_transaction

    async def connect(self) -> None:
        if self._engine is None:
            self._engine = auniqorm_native.connect_sqlite(self._get_db_path())

    async def disconnect(self) -> None:
        self._in_transaction = False
        if self._engine is not None:
            self._engine.close()
            self._engine = None

    async def execute(self, query: str, values: list[Any] | None = None) -> Any:
        if self._engine is None:
            raise QueryError("Not connected")
        try:
            return self._engine.execute(query, values)
        except Exception as e:
            raise QueryError(f"Execute failed: {e}") from e

    async def execute_many(self, query: str, values_list: list[list[Any]]) -> None:
        if self._engine is None:
            raise QueryError("Not connected")
        try:
            for values in values_list:
                self._engine.execute(query, list(values))
        except Exception as e:
            raise QueryError(f"Execute many failed: {e}") from e

    async def fetch_one(
        self, query: str, values: list[Any] | None = None
    ) -> dict[str, Any] | None:
        if self._engine is None:
            raise QueryError("Not connected")
        try:
            return cast("dict[str, Any] | None", self._engine.fetch_one(query, values))
        except Exception as e:
            raise QueryError(f"Fetch one failed: {e}") from e

    async def fetch_all(
        self, query: str, values: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        if self._engine is None:
            raise QueryError("Not connected")
        try:
            return cast("list[dict[str, Any]]", self._engine.fetch_all(query, values))
        except Exception as e:
            raise QueryError(f"Fetch all failed: {e}") from e

    async def fetch_all_msgpack(
        self, query: str, values: list[Any] | None = None
    ) -> bytes:
        """Fetch rows as a msgpack buffer for msgspec to decode into Structs."""
        if self._engine is None:
            raise QueryError("Not connected")
        try:
            return cast("bytes", self._engine.fetch_all_msgpack(query, values))
        except Exception as e:
            raise QueryError(f"Fetch all msgpack failed: {e}") from e

    async def fetch_one_msgpack(
        self, query: str, values: list[Any] | None = None
    ) -> bytes | None:
        """Fetch a single row as a msgpack buffer, or None."""
        if self._engine is None:
            raise QueryError("Not connected")
        try:
            return cast("bytes | None", self._engine.fetch_one_msgpack(query, values))
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
        if self._in_transaction:
            return  # nested BEGIN is a no-op
        try:
            self._engine.begin()
            self._in_transaction = True
        except Exception as e:
            raise QueryError(f"Begin failed: {e}") from e

    async def commit(self) -> None:
        if not self._in_transaction:
            return
        self._in_transaction = False
        try:
            self._engine.commit()
        except Exception as e:
            raise QueryError(f"Commit failed: {e}") from e

    async def rollback(self) -> None:
        if not self._in_transaction:
            return
        self._in_transaction = False
        try:
            self._engine.rollback()
        except Exception as e:
            raise QueryError(f"Rollback failed: {e}") from e
