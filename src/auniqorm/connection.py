# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Database connection management with URL-based driver detection."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from auniqorm._paramstyle import normalize_many, normalize_query
from auniqorm.collection import Collection
from auniqorm.drivers import BaseDriver
from auniqorm.drivers.registry import DriverRegistry
from auniqorm.exceptions import ConnectionError, QueryError
from auniqorm.models import AUniqORMBase, AUniqORMBaseMeta, ModelRegistry

if TYPE_CHECKING:
    from types import TracebackType


class Database:
    """Main database connection class.

    Automatically detects the appropriate driver from the database URL protocol.

    Example:
        async with Database("postgresql://user:pass@localhost/db") as db:
            result = await db.fetch_all("SELECT * FROM users")

        async with Database("sqlite:///path/to/db.sqlite") as db:
            result = await db.fetch_one("SELECT * FROM items WHERE id = ?", [1])
    """

    def __init__(self, url: str, **options: Any) -> None:
        """Initialize database connection.

        Args:
            url: Database URL in the format protocol://user:pass@host:port/database
            models: If False, skip ModelRegistry binding and table creation.
                   Useful for raw-SQL-only connections to external databases.
            **options: Additional driver-specific options
        """
        self._url = url
        self._options = options
        self._driver: BaseDriver | None = None
        self._protocol = self._parse_protocol(url)
        self._manage_models = options.pop("models", True)

    @staticmethod
    def _parse_protocol(url: str) -> str:
        """Extract the protocol from a database URL.

        Args:
            url: Database URL

        Returns:
            Protocol string (e.g., 'postgresql', 'mysql', 'sqlite')
        """
        parsed = urlparse(url)
        protocol = parsed.scheme

        # Handle protocol aliases
        aliases: dict[str, str] = {
            "postgres": "postgresql",
            "pg": "postgresql",
            "cockroachdb": "postgresql",
            "cockroach": "postgresql",
            "crdb": "postgresql",
            "mariadb": "mysql",
            "mysql+pymysql": "mysql",
            "mysql+aiomysql": "mysql",
            "postgresql+asyncpg": "postgresql",
            "sqlite+aiosqlite": "sqlite",
            "mongodb+srv": "mongodb",
            "mongo": "mongodb",
        }

        return aliases.get(protocol, protocol)

    def _get_driver(self) -> BaseDriver:
        """Get the appropriate driver for the database protocol.

        Returns:
            Driver instance

        Raises:
            ConnectionError: If no driver is available for the protocol
        """
        driver_class = DriverRegistry.get(self._protocol, self._options)
        return driver_class(self._url, **self._options)

    async def connect(self) -> None:
        """Establish connection to the database."""
        if self._driver is not None:
            return

        self._driver = self._get_driver()
        try:
            await self._driver.connect()
        except Exception as e:
            self._driver = None
            raise ConnectionError(f"Failed to connect: {e}") from e

    async def disconnect(self) -> None:
        """Close the database connection."""
        if self._driver is not None:
            await self._driver.disconnect()
            self._driver = None

    async def __aenter__(self) -> Database:
        """Async context manager entry."""
        await self.connect()
        if (
            self._manage_models
            and self._driver is not None
            and self._driver.SUPPORTS_MODELS
        ):
            ModelRegistry.bind(self)
            try:
                await ModelRegistry.create_all()
            except BaseException:
                ModelRegistry.unbind(self)
                await self.disconnect()
                raise
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        """Async context manager exit."""
        try:
            await self.disconnect()
        finally:
            ModelRegistry.unbind(self)

    @property
    def is_connected(self) -> bool:
        """Check if the database is connected."""
        return self._driver is not None and self._driver.is_connected

    @property
    def protocol(self) -> str:
        """Get the database protocol."""
        return self._protocol

    def collection(self, name: str) -> Collection:
        """Return the MongoDB collection *name* with a keyword-based API.

        The collection resolves the connection lazily, so it may be created
        before :meth:`connect`.  Only document databases support collections.
        """
        return Collection(self, name)

    def _collection_driver(self) -> Any:
        if self._driver is None:
            raise ConnectionError("Not connected to database")
        if not self._driver.SUPPORTS_COLLECTIONS:
            raise QueryError(
                f"{self._protocol} does not support collections; "
                "use a MongoDB connection"
            )
        return self._driver

    # Query methods

    def _prep(
        self,
        query: str,
        values: Sequence[Any] | Mapping[str, Any] | None,
        paramstyle: str | None,
    ) -> tuple[str, list[Any] | None]:
        """Apply opt-in placeholder normalization to user-supplied raw SQL.

        When ``paramstyle`` is ``None`` (the default, and always the case for
        ORM-internal calls) the query and values pass through untouched.
        """
        if paramstyle is None:
            return query, values  # type: ignore[return-value]
        return normalize_query(query, values, self._protocol, paramstyle)

    async def execute(
        self,
        query: str,
        values: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        paramstyle: str | None = None,
    ) -> Any:
        """Execute a query without returning results.

        Args:
            query: SQL query string
            values: Query parameters
            paramstyle: Opt-in canonical placeholder style for raw SQL —
                ``"qmark"`` (``?``), ``"named"`` (``:name``), or ``"auto"``.
                When set, placeholders are translated to the connected
                dialect's native style.  Defaults to ``None`` (no translation).

        Returns:
            Driver-specific result (e.g., last insert ID, rows affected)
        """
        if self._driver is None:
            raise ConnectionError("Not connected to database")
        query, values = self._prep(query, values, paramstyle)
        return await self._driver.execute(query, values)

    async def execute_many(
        self,
        query: str,
        values_list: Sequence[Sequence[Any] | Mapping[str, Any]],
        *,
        paramstyle: str | None = None,
    ) -> None:
        """Execute a query multiple times with different parameters.

        Args:
            query: SQL query string
            values_list: List of parameter lists
            paramstyle: Opt-in canonical placeholder style (see :meth:`execute`).
        """
        if self._driver is None:
            raise ConnectionError("Not connected to database")
        if paramstyle is not None:
            query, values_list = normalize_many(
                query, values_list, self._protocol, paramstyle
            )
        await self._driver.execute_many(query, values_list)  # type: ignore[arg-type]

    async def fetch_one(
        self,
        query: str,
        values: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        paramstyle: str | None = None,
    ) -> dict[str, Any] | None:
        """Fetch a single row.

        Args:
            query: SQL query string
            values: Query parameters
            paramstyle: Opt-in canonical placeholder style (see :meth:`execute`).

        Returns:
            Row as dictionary or None if no results
        """
        if self._driver is None:
            raise ConnectionError("Not connected to database")
        query, values = self._prep(query, values, paramstyle)
        return await self._driver.fetch_one(query, values)

    async def fetch_all(
        self,
        query: str,
        values: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        paramstyle: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all rows.

        Args:
            query: SQL query string
            values: Query parameters
            paramstyle: Opt-in canonical placeholder style (see :meth:`execute`).

        Returns:
            List of rows as dictionaries
        """
        if self._driver is None:
            raise ConnectionError("Not connected to database")
        query, values = self._prep(query, values, paramstyle)
        return await self._driver.fetch_all(query, values)

    async def fetch_val(
        self,
        query: str,
        values: Sequence[Any] | Mapping[str, Any] | None = None,
        column: int = 0,
        *,
        paramstyle: str | None = None,
    ) -> Any:
        """Fetch a single value.

        Args:
            query: SQL query string
            values: Query parameters
            column: Column index to return
            paramstyle: Opt-in canonical placeholder style (see :meth:`execute`).

        Returns:
            Single value or None
        """
        if self._driver is None:
            raise ConnectionError("Not connected to database")
        query, values = self._prep(query, values, paramstyle)
        return await self._driver.fetch_val(query, values, column)

    async def iterate(
        self,
        query: str,
        values: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 500,
        paramstyle: str | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield rows one at a time using batched cursor reads."""
        if self._driver is None:
            raise ConnectionError("Not connected to database")
        query, values = self._prep(query, values, paramstyle)
        async for row in self._driver.iterate(query, values, batch_size=batch_size):
            yield row

    async def iterate_batches(
        self,
        query: str,
        values: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 500,
        paramstyle: str | None = None,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Yield batches of rows directly from the cursor.

        Unlike :meth:`iterate` (which yields one row at a time), this
        yields the entire ``fetchmany(batch_size)`` list per iteration,
        avoiding per-row overhead when the caller needs chunks.
        """
        if self._driver is None:
            raise ConnectionError("Not connected to database")
        query, values = self._prep(query, values, paramstyle)
        async for batch in self._driver.iterate_batches(
            query, values, batch_size=batch_size
        ):
            yield batch

    # Transaction support

    async def transaction(self) -> Transaction:
        """Start a new transaction.

        Session state set inside the transaction does not outlive it.

        Returns:
            Transaction context manager
        """
        if self._driver is None:
            raise ConnectionError("Not connected to database")
        return Transaction(self._driver)

    async def connection(self) -> PinnedConnection:
        """Pin a connection without starting a transaction.

        Use this for session-level state such as ``SET ROLE``: the native
        drivers reset the session when the pinned connection is released,
        while standalone statements on the pool are not reset.

        Returns:
            PinnedConnection context manager
        """
        if self._driver is None:
            raise ConnectionError("Not connected to database")
        return PinnedConnection(self._driver)


class PinnedConnection:
    """Pin a pool connection for reuse without a transaction."""

    def __init__(self, driver: BaseDriver) -> None:
        self._driver = driver

    async def __aenter__(self) -> PinnedConnection:
        if hasattr(self._driver, "acquire"):
            await self._driver.acquire()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        if hasattr(self._driver, "release"):
            await self._driver.release()


class Transaction:
    """Transaction context manager."""

    def __init__(self, driver: BaseDriver) -> None:
        self._driver = driver

    async def __aenter__(self) -> Transaction:
        await self._driver.begin()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            await self._driver.rollback()
        else:
            await self._driver.commit()


def dynamic_model(
    table_name: str,
    fields: dict[str, Any] | None = None,
    *,
    register: bool = False,
) -> type[AUniqORMBase]:
    """Create a model class bound to a table name chosen at runtime.

    *table_name* (optionally ``schema.``-qualified) and field names must be plain
    identifiers, else ``ValueError``.  That does not restrict *which* table is
    used: map user input onto an allow-list of tables, never pass it through.

    Parameters
    ----------
    table_name:
        The database table to bind to.
    fields:
        Optional dict of ``{column_name: type}`` (e.g. ``{"id": int, "name": str}``).
        If omitted, a minimal model with just ``id: int`` is created — useful
        for raw queries via the model's ``filter()``/``get()`` interface.
    register:
        Whether to register the model in the global ModelRegistry.  Defaults to
        False so dynamic models don't conflict with each other.

    Returns
    -------
    type[AUniqORMBase]
        A new model class with ``__table_name__`` set to *table_name*.

    Example
    -------
    ::

        from auniqorm import Database
        from auniqorm.connection import dynamic_model

        async with Database("postgresql://...") as db:
            Detections = dynamic_model(
                "object_detections_v2",
                {"id": int, "label": str, "score": float},
            )
            Detections.__database__ = db
            results = await Detections.filter(score__gt=0.8)

        # Or for raw SQL via the Database directly:
        rows = await db.fetch_all(
            "SELECT * FROM whatever_table WHERE x = $1", [42]
        )
    """
    from auniqorm.columns import Column, PrimaryKey
    from auniqorm.schema import validate_identifier

    validate_identifier(table_name, "table name", 2)
    if fields is None:
        fields = {"id": int}
    for col_name in fields:
        validate_identifier(col_name, "column name")

    annotations: dict[str, Any] = {}
    namespace: dict[str, Any] = {}

    for i, (col_name, col_type) in enumerate(fields.items()):
        annotations[col_name] = col_type
        if i == 0:
            namespace[col_name] = PrimaryKey()
        else:
            namespace[col_name] = Column(nullable=True)

    namespace["__annotations__"] = annotations

    # Generate a unique class name to avoid collisions
    class_name = "".join(part.capitalize() for part in table_name.split("_")) + "Model"

    # Create without registration by default
    if register:
        model = AUniqORMBaseMeta(
            class_name,
            (AUniqORMBase,),
            namespace,
            table=table_name,
        )
    else:
        # Create with abstract=True to skip registration, then fix the table name
        model = AUniqORMBaseMeta(
            class_name,
            (AUniqORMBase,),
            namespace,
            table=table_name,
            abstract=True,
        )
        model.__table_name__ = table_name
        model.__abstract__ = False

    return model  # type: ignore[return-value]
