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

TLS: ``sslmode`` (default ``prefer``) and ``sslrootcert`` (a file, or
``system``) follow asyncpg/libpq, from the URL, ``PGSSLMODE``/``PGSSLROOTCERT``
or ``Database(url, ssl=...)`` (``True`` = verify-full against the system trust
store, ``False``, or an sslmode). Other TLS settings (client certificates,
CRLs, protocol bounds) and other keyword options make the registry select
asyncpg, or fail if it is not installed; unknown URL options are rejected.

Session state: pooled single statements are not followed by a session reset
(that would cost a round trip per statement), so session-level state (``SET``,
``SET ROLE``, temporary tables, advisory locks, ``LISTEN``) must only be
changed inside a transaction or pinned connection. Those connections are reset
(``RESET ALL``, ``DISCARD TEMP``, …) in the same round trip as their final
``COMMIT``/``ROLLBACK`` before they return to the pool.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, cast

from auniqorm.drivers import BaseDriver
from auniqorm.drivers.registry import native_options
from auniqorm.exceptions import ConnectionError, QueryError

try:
    import auniqorm_native
except ImportError:
    if TYPE_CHECKING:
        raise
    auniqorm_native = None

_SSL_MODES = frozenset(
    {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
)
# TLS settings asyncpg honours but the native driver does not.
_UNSUPPORTED_TLS = (
    "PGSSLCERT",
    "PGSSLKEY",
    "PGSSLPASSWORD",
    "PGSSLCRL",
    "PGSSLCRLDIR",
    "PGSSLMINPROTOCOLVERSION",
    "PGSSLMAXPROTOCOLVERSION",
)
_UNSUPPORTED_TLS_FILES = ("~/.postgresql/postgresql.crt", "~/.postgresql/root.crl")


def _ssl(value: Any) -> dict[str, Any] | None:
    if value is True:  # asyncpg: ssl.create_default_context()
        return {"sslmode": "verify-full", "sslrootcert": "system"}
    if value is False:
        return {"sslmode": "disable"}
    mode = value.lower().replace("_", "-") if isinstance(value, str) else None
    return {"sslmode": mode} if mode in _SSL_MODES else None


def _options(options: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
    kwargs, unsupported = native_options(options, _ssl)
    if kwargs.get("sslmode") != "disable":
        unsupported += [name for name in _UNSUPPORTED_TLS if os.environ.get(name)]
        unsupported += [
            path
            for path in _UNSUPPORTED_TLS_FILES
            if os.path.exists(os.path.expanduser(path))
        ]
    return kwargs, unsupported


class PostgresNativeDriver(BaseDriver):
    """PostgreSQL driver using the Rust ``auniqorm_native`` engine."""

    #: Sentinel for the ORM's msgpack model-hydration fast path.
    SUPPORTS_MSGPACK = True

    @classmethod
    def unsupported_options(cls, options: Mapping[str, Any]) -> list[str]:
        """Options (and TLS settings) this driver cannot honour faithfully."""
        return _options(options)[1]

    def __init__(self, url: str, **options: Any) -> None:
        super().__init__(url, **options)
        if auniqorm_native is None:
            raise ImportError(
                "auniqorm-native is required for the native PostgreSQL engine. "
                "Build it with: maturin develop --release (in rust/)."
            )
        self._connect_kwargs, unsupported = _options(options)
        if unsupported:
            raise ConnectionError(
                "The native PostgreSQL driver cannot honour "
                f"{', '.join(sorted(unsupported))}; install asyncpg "
                "(pip install auniqorm[postgres]) or set AUNIQORM_PG_NATIVE=0"
            )
        self._engine: Any = None

    @property
    def is_connected(self) -> bool:
        return self._engine is not None

    @property
    def _active(self) -> Any:
        """The current task's pinned connection if it has one, else the pool."""
        scope = self._scope()
        return self._engine if scope is None else scope.conn

    @property
    def in_transaction(self) -> bool:
        return self._scope_in_transaction()

    async def connect(self) -> None:
        if self._engine is None:
            self._engine = await auniqorm_native.connect_pg(
                self._url, **self._connect_kwargs
            )

    async def disconnect(self) -> None:
        if self._engine is not None:
            await self._release_all_scopes()
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
        await self._guarded(self._scoped_begin(), "Begin")

    async def acquire(self) -> None:
        """Pin a connection to the current task (explicit connection() context)."""
        if self._engine is None:
            raise QueryError("Not connected")
        await self._guarded(self._scoped_acquire(), "Acquire")

    async def release(self) -> None:
        await self._guarded(self._scoped_release(), "Release")

    async def commit(self) -> None:
        await self._guarded(self._scoped_end(commit=True), "Commit")

    async def rollback(self) -> None:
        await self._guarded(self._scoped_end(commit=False), "Rollback")
