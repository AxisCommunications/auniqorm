# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""aUniqORM abstract base driver interface for database backends."""

import contextlib
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Awaitable
from contextvars import ContextVar
from typing import Any

from auniqorm.drivers.registry import DriverRegistry
from auniqorm.exceptions import QueryError


class _Scope:
    """A connection pinned for one asyncio task and the tasks it starts.

    ``depth`` counts open transaction levels (nested ones are savepoints) and
    ``pins`` open ``connection()`` contexts; the connection is released when
    both reach zero. Tasks still holding an ended scope must not reuse it.
    """

    __slots__ = ("began", "conn", "depth", "ended", "pins", "state")

    def __init__(self, conn: Any, state: Any, *, began: bool) -> None:
        self.conn = conn
        self.state = state  # driver-specific, e.g. asyncpg's Transaction
        self.began = began
        self.depth = 1 if began else 0
        self.pins = 0
        self.ended = False


class BaseDriver(ABC):
    """Abstract base class for database drivers.

    All database-specific drivers must implement this interface to ensure
    consistent behavior across different database backends.
    """

    SUPPORTS_MODELS = True
    SUPPORTS_COLLECTIONS = False

    def __init__(self, url: str, **options: Any) -> None:
        """Initialize the driver.

        Args:
            url: Database connection URL
            **options: Driver-specific options
        """
        self._url = url
        self._options = options
        self._connection: Any = None
        # Transactions and pinned connections belong to the asyncio task that
        # opened them (and tasks it starts), never to the shared driver.
        self._scope_var: ContextVar[_Scope | None] = ContextVar(
            f"auniqorm_scope_{id(self)}", default=None
        )
        self._live_scopes: set[_Scope] = set()

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

    # Task-scoped connections. The hook defaults suit native engines, whose
    # begin()/acquire() return handles that end and pool themselves; other
    # drivers override them.

    def _scope(self) -> _Scope | None:
        """The current task's pinned connection, if any."""
        scope = self._scope_var.get()
        if scope is not None and scope.ended:
            raise QueryError(
                "The transaction or pinned connection used by this task has "
                "already ended"
            )
        return scope

    def _scope_in_transaction(self) -> bool:
        scope = self._scope_var.get()
        return scope is not None and not scope.ended and scope.depth > 0

    async def _pin_connection(self, *, begin: bool) -> tuple[Any, Any]:
        """Pin a connection, in a transaction if *begin*; return it and state."""
        engine: Any = getattr(self, "_engine")
        return await (engine.begin() if begin else engine.acquire()), None

    async def _start_transaction(self, scope: _Scope) -> None:
        if scope.began:
            await self._scope_statement(scope, "BEGIN")
        else:
            await scope.conn.begin()

    async def _finish_transaction(self, scope: _Scope, *, commit: bool) -> None:
        """End the outermost transaction but keep the connection pinned."""
        if scope.began:
            # A transaction-mode handle would release itself on commit().
            await self._scope_statement(scope, "COMMIT" if commit else "ROLLBACK")
        else:
            await (scope.conn.commit() if commit else scope.conn.rollback())

    async def _scope_statement(self, scope: _Scope, sql: str) -> None:
        await scope.conn.execute(sql, None)

    async def _unpin_connection(self, scope: _Scope, end: str) -> None:
        """End the scope (``commit``, ``rollback`` or ``release``) and return
        its connection, even if ending fails."""
        handle = scope.conn
        try:
            if end != "release":
                await (handle.commit() if end == "commit" else handle.rollback())
        finally:
            if not handle.closed:
                await handle.release()

    def _end_scope(self, scope: _Scope) -> None:
        scope.ended = True
        self._live_scopes.discard(scope)
        if self._scope_var.get() is scope:
            self._scope_var.set(None)

    def _open_scope(self, pinned: tuple[Any, Any], *, began: bool) -> _Scope:
        scope = _Scope(*pinned, began=began)
        self._live_scopes.add(scope)
        self._scope_var.set(scope)
        return scope

    async def _scoped_begin(self) -> None:
        """Begin a transaction, or a savepoint when one is already open."""
        scope = self._scope()
        if scope is None:
            self._open_scope(await self._pin_connection(begin=True), began=True)
        elif scope.depth == 0:
            await self._start_transaction(scope)
            scope.depth = 1
        else:
            await self._scope_statement(scope, f"SAVEPOINT auniqorm_{scope.depth}")
            scope.depth += 1

    async def _scoped_end(self, *, commit: bool) -> None:
        """Commit or roll back the innermost open transaction level."""
        scope = self._scope_var.get()
        if scope is None or scope.depth == 0:
            return
        if scope.ended:
            if commit:
                raise QueryError("Cannot commit: the transaction has already ended")
            return
        if scope.depth > 1:
            scope.depth -= 1
            savepoint = f"auniqorm_{scope.depth}"
            if not commit:
                await self._scope_statement(scope, f"ROLLBACK TO SAVEPOINT {savepoint}")
            await self._scope_statement(scope, f"RELEASE SAVEPOINT {savepoint}")
            return
        scope.depth = 0
        if scope.pins:
            await self._finish_transaction(scope, commit=commit)
            return
        # Mark the scope ended before awaiting, so tasks sharing it cannot
        # issue statements on a connection that is being returned.
        self._end_scope(scope)
        await self._unpin_connection(scope, "commit" if commit else "rollback")

    async def _scoped_acquire(self) -> None:
        """Pin a connection for the current task without a transaction."""
        scope = self._scope()
        if scope is None:
            scope = self._open_scope(
                await self._pin_connection(begin=False), began=False
            )
        scope.pins += 1

    async def _scoped_release(self) -> None:
        scope = self._scope_var.get()
        if scope is None or scope.ended or scope.pins == 0:
            return
        scope.pins -= 1
        if scope.pins == 0 and scope.depth == 0:
            self._end_scope(scope)
            await self._unpin_connection(scope, "release")

    @staticmethod
    async def _guarded(operation: Awaitable[None], action: str) -> None:
        """Await *operation*, reporting driver errors as ``QueryError``."""
        try:
            await operation
        except QueryError:
            raise
        except Exception as e:
            raise QueryError(f"{action} failed: {e}") from e

    async def _release_all_scopes(self) -> None:
        """Roll back and release every pinned connection, e.g. on disconnect."""
        for scope in list(self._live_scopes):
            self._end_scope(scope)
            with contextlib.suppress(Exception):
                await self._unpin_connection(scope, "release")

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
