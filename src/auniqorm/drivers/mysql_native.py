# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Native MySQL driver backed by the Rust mysql_async engine.

Selected by default when the native extension is available. Set
``AUNIQORM_MYSQL_NATIVE=0`` to use aiomysql instead. Supports real
transactions: ``begin()`` pins a connection and issues ``BEGIN``; queries then
route to that connection until ``commit()``/``rollback()``.

The ORM emits ``%s`` placeholders for the MySQL dialect (aiomysql pyformat);
mysql_async uses positional ``?``, so queries are rewritten here before crossing
into Rust.

TLS: as with aiomysql, connections are plaintext unless requested.
``Database(url, ssl=True)`` requires TLS and verifies the chain and host name
against the system trust store (like ``ssl.create_default_context()``);
``ssl="disabled"|"preferred"|"required"|"verify_identity"`` selects that MySQL
mode (``"verify_ca"`` raises: mysql_async cannot skip only the host-name
check). mysql_async's ``require_ssl``/``verify_ca``/``built_in_roots`` URL
parameters also work. Other keyword options (e.g. an ``ssl.SSLContext``) make
the registry select aiomysql, or fail if it is not installed; unknown URL
parameters are rejected.

Session state: pooled single statements are not followed by a session reset
(that would cost a round trip per statement), so session-level state (``SET``,
user variables, temporary tables, ``GET_LOCK``) must only be changed inside a
transaction or pinned connection. Those connections are reset with
``COM_RESET_CONNECTION`` before they return to the pool.
"""

from __future__ import annotations

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


def _translate(query: str) -> str:
    """Rewrite the ORM's ``%s`` placeholders to mysql_async's ``?``."""
    return query.replace("%s", "?")


_SSL_MODES = {
    "disable": "disabled",
    "disabled": "disabled",
    "allow": "preferred",
    "prefer": "preferred",
    "preferred": "preferred",
    "require": "required",
    "required": "required",
    "verify_ca": "verify_ca",
    "verify_full": "verify_identity",
    "verify_identity": "verify_identity",
}


def _ssl(value: Any) -> dict[str, Any] | None:
    if isinstance(value, bool):
        return {"ssl_mode": "verify_identity" if value else "disabled"}
    mode = isinstance(value, str) and _SSL_MODES.get(value.lower().replace("-", "_"))
    return {"ssl_mode": mode} if mode else None


class MySQLNativeDriver(BaseDriver):
    """MySQL driver using the Rust ``auniqorm_native`` engine."""

    #: Sentinel for the ORM's msgpack model-hydration fast path.
    SUPPORTS_MSGPACK = True

    @classmethod
    def unsupported_options(cls, options: Mapping[str, Any]) -> list[str]:
        """Names of keyword options this driver cannot honour faithfully."""
        return native_options(options, _ssl)[1]

    def __init__(self, url: str, **options: Any) -> None:
        super().__init__(url, **options)
        if auniqorm_native is None:
            raise ImportError(
                "auniqorm-native is required for the native MySQL engine. "
                "Build it with: maturin develop --release (in rust/)."
            )
        self._connect_kwargs, unsupported = native_options(options, _ssl)
        if unsupported:
            raise ConnectionError(
                "The native MySQL driver cannot honour option(s) "
                f"{', '.join(sorted(unsupported))}; install aiomysql "
                "(pip install auniqorm[mysql]) or set AUNIQORM_MYSQL_NATIVE=0"
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
            self._engine = await auniqorm_native.connect_mysql(
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
            return await self._active.execute(_translate(query), values)
        except Exception as e:
            raise QueryError(f"Execute failed: {e}") from e

    async def execute_many(self, query: str, values_list: list[list[Any]]) -> None:
        if self._engine is None:
            raise QueryError("Not connected")
        try:
            active = self._active
            sql = _translate(query)
            for values in values_list:
                await active.execute(sql, list(values))
        except Exception as e:
            raise QueryError(f"Execute many failed: {e}") from e

    async def fetch_one(
        self, query: str, values: list[Any] | None = None
    ) -> dict[str, Any] | None:
        if self._engine is None:
            raise QueryError("Not connected")
        try:
            return cast(
                "dict[str, Any] | None",
                await self._active.fetch_one(_translate(query), values),
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
                "list[dict[str, Any]]",
                await self._active.fetch_all(_translate(query), values),
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
            return cast(
                "bytes", await self._active.fetch_all_msgpack(_translate(query), values)
            )
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
                "bytes | None",
                await self._active.fetch_one_msgpack(_translate(query), values),
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
