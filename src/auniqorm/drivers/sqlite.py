# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""SQLite driver using aiosqlite."""

from collections.abc import AsyncIterator
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
        self._in_transaction = False

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
        return self._in_transaction

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
            await self._db.close()
            self._db = None

    async def execute(self, query: str, values: list[Any] | None = None) -> Any:
        if self._db is None:
            raise QueryError("Not connected")

        try:
            cursor = await self._db.execute(query, values or [])
            if not self._in_transaction:
                await self._db.commit()
            return cursor.lastrowid
        except Exception as e:
            raise QueryError(f"Execute failed: {e}") from e

    async def execute_many(self, query: str, values_list: list[list[Any]]) -> None:
        if self._db is None:
            raise QueryError("Not connected")

        try:
            await self._db.executemany(query, values_list)
            if not self._in_transaction:
                await self._db.commit()
        except Exception as e:
            raise QueryError(f"Execute many failed: {e}") from e

    async def fetch_one(
        self, query: str, values: list[Any] | None = None
    ) -> dict[str, Any] | None:
        if self._db is None:
            raise QueryError("Not connected")

        try:
            cursor = await self._db.execute(query, values or [])
            row = await cursor.fetchone()

            if row is None:
                return None
            return dict(row)
        except Exception as e:
            raise QueryError(f"Fetch one failed: {e}") from e

    async def fetch_all(
        self, query: str, values: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        if self._db is None:
            raise QueryError("Not connected")

        try:
            cursor = await self._db.execute(query, values or [])
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            raise QueryError(f"Fetch all failed: {e}") from e

    async def fetch_val(
        self, query: str, values: list[Any] | None = None, column: int = 0
    ) -> Any:
        if self._db is None:
            raise QueryError("Not connected")

        try:
            cursor = await self._db.execute(query, values or [])
            row = await cursor.fetchone()

            if row is None:
                return None
            return row[column]
        except Exception as e:
            raise QueryError(f"Fetch val failed: {e}") from e

    async def begin(self) -> None:
        if self._db is None:
            raise QueryError("Not connected")
        await self._db.execute("BEGIN")
        self._in_transaction = True

    async def commit(self) -> None:
        if self._db is not None:
            await self._db.commit()
            self._in_transaction = False

    async def rollback(self) -> None:
        if self._db is not None:
            await self._db.rollback()
            self._in_transaction = False

    async def iterate(
        self,
        query: str,
        values: list[Any] | None = None,
        *,
        batch_size: int = 500,
    ) -> AsyncIterator[dict[str, Any]]:
        if self._db is None:
            raise QueryError("Not connected")

        try:
            cursor = await self._db.execute(query, values or [])
            while True:
                rows = await cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    yield dict(row)
        except Exception as e:
            raise QueryError(f"Iterate failed: {e}") from e

    async def iterate_batches(
        self,
        query: str,
        values: list[Any] | None = None,
        *,
        batch_size: int = 500,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        if self._db is None:
            raise QueryError("Not connected")

        try:
            cursor = await self._db.execute(query, values or [])
            while True:
                rows = await cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield [dict(row) for row in rows]
        except Exception as e:
            raise QueryError(f"Iterate failed: {e}") from e
