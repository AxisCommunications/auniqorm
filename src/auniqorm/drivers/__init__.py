# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""aUniqORM abstract base driver interface for database backends."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any

from auniqorm.drivers.registry import DriverRegistry


class BaseDriver(ABC):
    """Abstract base class for database drivers.

    All database-specific drivers must implement this interface to ensure
    consistent behavior across different database backends.
    """

    SUPPORTS_MODELS = True

    def __init__(self, url: str, **options: Any) -> None:
        """Initialize the driver.

        Args:
            url: Database connection URL
            **options: Driver-specific options
        """
        self._url = url
        self._options = options
        self._connection: Any = None

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if the driver is connected to the database."""
        ...

    @property
    def in_transaction(self) -> bool:
        """Whether a transaction is currently active on this driver."""
        return False

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the database."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Close the database connection."""
        ...

    @abstractmethod
    async def execute(self, query: str, values: list[Any] | None = None) -> Any:
        """Execute a query without returning results.

        Args:
            query: SQL query string
            values: Query parameters

        Returns:
            Driver-specific result
        """
        ...

    @abstractmethod
    async def execute_many(self, query: str, values_list: list[list[Any]]) -> None:
        """Execute a query multiple times with different parameters.

        Args:
            query: SQL query string
            values_list: List of parameter lists
        """
        ...

    @abstractmethod
    async def fetch_one(
        self, query: str, values: list[Any] | None = None
    ) -> dict[str, Any] | None:
        """Fetch a single row.

        Args:
            query: SQL query string
            values: Query parameters

        Returns:
            Row as dictionary or None
        """
        ...

    @abstractmethod
    async def fetch_all(
        self, query: str, values: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        """Fetch all rows.

        Args:
            query: SQL query string
            values: Query parameters

        Returns:
            List of rows as dictionaries
        """
        ...

    @abstractmethod
    async def fetch_val(
        self, query: str, values: list[Any] | None = None, column: int = 0
    ) -> Any:
        """Fetch a single value.

        Args:
            query: SQL query string
            values: Query parameters
            column: Column index

        Returns:
            Single value or None
        """
        ...

    # Transaction methods

    @abstractmethod
    async def begin(self) -> None:
        """Begin a transaction."""
        ...

    @abstractmethod
    async def commit(self) -> None:
        """Commit the current transaction."""
        ...

    @abstractmethod
    async def rollback(self) -> None:
        """Rollback the current transaction."""
        ...

    async def iterate(
        self, query: str, values: list[Any] | None = None, *, batch_size: int = 500
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield rows one at a time using server-side or batched cursors.

        Default implementation fetches all and yields — drivers may override
        with true cursor-based streaming.
        """
        rows = await self.fetch_all(query, values)
        for row in rows:
            yield row

    async def iterate_batches(
        self,
        query: str,
        values: list[Any] | None = None,
        *,
        batch_size: int = 500,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Yield whole batches from the cursor without per-row overhead.

        Default implementation fetches all and yields as a single batch —
        drivers may override with true cursor-based chunking.
        """
        rows = await self.fetch_all(query, values)
        for i in range(0, len(rows), batch_size):
            yield rows[i : i + batch_size]


__all__ = ["BaseDriver", "DriverRegistry"]
