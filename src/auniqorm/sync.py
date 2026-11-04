# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Synchronous adapter for aUniqORM.

aUniqORM is async-first.  This module lets non-async code use the same models
and connection API without ``await``, mirroring the ``asanypath.sync`` design:
a single background event loop runs on a daemon thread and every coroutine is
submitted to it and awaited synchronously.

Usage::

    from auniqorm import AUniqORMBase, Column, PrimaryKey
    from auniqorm.sync import SyncDatabase, sync_model


    class User(AUniqORMBase, table="users"):
        id: int = PrimaryKey()
        name: str = Column(nullable=False)
        email: str = Column(unique=True)


    with SyncDatabase("sqlite:///app.db") as db:
        Users = sync_model(User)
        user = Users(name="Alice", email="alice@example.com")
        user.insert()                     # blocks, no await
        found = Users.get(email="alice@example.com")
        for u in Users.filter(order_by="-id"):
            print(u.name)

All database work — connect, every query, transactions, and model operations —
runs on one shared background loop, so a pinned connection or an open
transaction stays on the same loop across calls.

Note: model instances returned by the sync API are thin proxies, so
``isinstance(user, User)`` is False.  Use ``.to_dict()`` or attribute access;
call ``._async`` to reach the underlying model instance if needed.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import inspect
import threading
from collections.abc import AsyncIterator, Coroutine, Iterator
from typing import TYPE_CHECKING, Any, TypeVar

from auniqorm.collection import Collection
from auniqorm.connection import Database
from auniqorm.models import AUniqORMBase

if TYPE_CHECKING:
    from types import TracebackType

__all__ = ["SyncDatabase", "run", "sync_model"]

_T = TypeVar("_T")


_thread_state = threading.local()


def _thread_context() -> contextvars.Context:
    """Return the context that the calling thread's coroutines run in."""
    context: contextvars.Context | None = getattr(_thread_state, "context", None)
    if context is None:
        context = _thread_state.context = contextvars.Context()
    return context


class _SyncRunner:
    """Lazy singleton that runs coroutines on a background event loop."""

    _instance: _SyncRunner | None = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None

    @classmethod
    def get(cls) -> _SyncRunner:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def _start(self) -> None:
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
            self._thread = threading.Thread(
                target=self._loop.run_forever, daemon=True, name="auniqorm-sync"
            )
            self._thread.start()

    def run(self, coro: Coroutine[Any, Any, _T]) -> _T:
        """Submit a coroutine and block until it completes.

        Every call from one thread runs in that thread's own context, so
        context-scoped state such as an open transaction carries over between
        the thread's calls while other threads stay isolated.
        """
        self._start()
        loop = self._loop
        assert loop is not None
        context = _thread_context()
        result: concurrent.futures.Future[_T] = concurrent.futures.Future()

        def start() -> None:
            if not result.set_running_or_notify_cancel():
                coro.close()
                return
            task = loop.create_task(coro, context=context)

            def finish(task: asyncio.Task[_T]) -> None:
                if task.cancelled():
                    result.cancel()
                elif (exception := task.exception()) is not None:
                    result.set_exception(exception)
                else:
                    result.set_result(task.result())

            task.add_done_callback(finish)

        loop.call_soon_threadsafe(start)
        return result.result()

    def iter(self, async_iter: AsyncIterator[_T]) -> Iterator[_T]:
        """Drain an async iterator, yielding items synchronously."""
        self._start()
        assert self._loop is not None

        async def _next() -> _T:
            return await async_iter.__anext__()

        while True:
            try:
                yield self.run(_next())
            except StopAsyncIteration:
                return


def run[T](coro: Coroutine[Any, Any, T]) -> T:
    """Run a single coroutine on the shared background loop and return its result.

    Escape hatch for coroutines the proxies do not cover (e.g. an awaitable
    obtained directly from the async API).
    """
    return _SyncRunner.get().run(coro)


def _wrap(obj: Any) -> Any:
    """Wrap ORM instances so their async methods stay callable without await."""
    if isinstance(obj, AUniqORMBase):
        return _SyncInstanceProxy(obj)
    if isinstance(obj, list):
        return [_wrap(item) for item in obj]
    if isinstance(obj, tuple):
        return tuple(_wrap(item) for item in obj)
    return obj


def _bind_sync(attr: Any) -> Any:
    """Turn an async callable into a blocking one; leave sync callables intact."""
    if inspect.iscoroutinefunction(attr):

        def _sync_call(*args: Any, **kwargs: Any) -> Any:
            return _wrap(_SyncRunner.get().run(attr(*args, **kwargs)))

        return _sync_call

    if inspect.isasyncgenfunction(attr):

        def _sync_iter(*args: Any, **kwargs: Any) -> Iterator[Any]:
            for item in _SyncRunner.get().iter(attr(*args, **kwargs)):
                yield _wrap(item)

        return _sync_iter

    if callable(attr):

        def _passthrough(*args: Any, **kwargs: Any) -> Any:
            return _wrap(attr(*args, **kwargs))

        return _passthrough

    return _wrap(attr)


class _SyncInstanceProxy:
    """Sync proxy around a single model instance."""

    __slots__ = ("_async",)

    def __init__(self, instance: AUniqORMBase) -> None:
        object.__setattr__(self, "_async", instance)

    def __getattr__(self, name: str) -> Any:
        return _bind_sync(getattr(self._async, name))

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(self._async, name, value)

    def __repr__(self) -> str:
        return repr(self._async)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _SyncInstanceProxy):
            return bool(self._async == other._async)
        return bool(self._async == other)

    def __hash__(self) -> int:
        return hash(self._async)


class _SyncModelProxy:
    """Sync proxy around a model class: constructs instances and runs classmethods."""

    __slots__ = ("_model",)

    def __init__(self, model: type[AUniqORMBase]) -> None:
        object.__setattr__(self, "_model", model)

    def __call__(self, *args: Any, **kwargs: Any) -> _SyncInstanceProxy:
        return _SyncInstanceProxy(self._model(*args, **kwargs))

    def __getattr__(self, name: str) -> Any:
        return _bind_sync(getattr(self._model, name))

    def __repr__(self) -> str:
        return f"sync_model({self._model.__name__})"


_MODEL_PROXY_CACHE: dict[type[AUniqORMBase], _SyncModelProxy] = {}


def sync_model(model: type[AUniqORMBase]) -> _SyncModelProxy:
    """Return a synchronous proxy for a aUniqORM model class.

    The proxy exposes the model's async classmethods (``get``, ``filter``,
    ``count``, ``insert``, ...) as blocking calls and constructs instance
    proxies whose async methods are likewise blocking.  Proxies are cached per
    model, so repeated calls return the same object.
    """
    cached = _MODEL_PROXY_CACHE.get(model)
    if cached is None:
        cached = _SyncModelProxy(model)
        _MODEL_PROXY_CACHE[model] = cached
    return cached


class _SyncTransaction:
    """Blocking context manager wrapping an async ``Transaction``."""

    __slots__ = ("_database", "_tx")

    def __init__(self, database: Database) -> None:
        self._database = database
        self._tx: Any = None

    def __enter__(self) -> _SyncTransaction:
        runner = _SyncRunner.get()
        self._tx = runner.run(self._database.transaction())
        runner.run(self._tx.__aenter__())
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        _SyncRunner.get().run(self._tx.__aexit__(exc_type, exc_val, exc_tb))


class _SyncPinnedConnection:
    """Blocking context manager wrapping an async ``PinnedConnection``."""

    __slots__ = ("_database", "_conn")

    def __init__(self, database: Database) -> None:
        self._database = database
        self._conn: Any = None

    def __enter__(self) -> _SyncPinnedConnection:
        runner = _SyncRunner.get()
        self._conn = runner.run(self._database.connection())
        runner.run(self._conn.__aenter__())
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        _SyncRunner.get().run(self._conn.__aexit__(exc_type, exc_val, exc_tb))


class _SyncCollection:
    """Blocking proxy around a MongoDB :class:`~auniqorm.Collection`."""

    __slots__ = ("_async",)

    def __init__(self, collection: Collection) -> None:
        self._async = collection

    def __getattr__(self, name: str) -> Any:
        return _bind_sync(getattr(self._async, name))

    def __repr__(self) -> str:
        return repr(self._async)


class SyncDatabase:
    """Synchronous wrapper around :class:`~auniqorm.connection.Database`.

    Runs the async database on a shared background loop and exposes blocking
    versions of its connection, query, and transaction API.  Entering the
    context binds the model registry and creates tables, exactly like the async
    ``async with Database(...)``.
    """

    __slots__ = ("_db",)

    def __init__(self, url: str, **options: Any) -> None:
        self._db = Database(url, **options)

    # ── Lifecycle ──────────────────────────────────────────────────────────

    def connect(self) -> None:
        _SyncRunner.get().run(self._db.connect())

    def disconnect(self) -> None:
        _SyncRunner.get().run(self._db.disconnect())

    def __enter__(self) -> SyncDatabase:
        _SyncRunner.get().run(self._db.__aenter__())
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        _SyncRunner.get().run(self._db.__aexit__(exc_type, exc_val, exc_tb))

    @property
    def is_connected(self) -> bool:
        return self._db.is_connected

    @property
    def protocol(self) -> str:
        return self._db.protocol

    @property
    def async_database(self) -> Database:
        """The wrapped async :class:`Database` (for advanced use)."""
        return self._db

    def collection(self, name: str) -> _SyncCollection:
        """Return a blocking MongoDB collection (see :meth:`Database.collection`)."""
        return _SyncCollection(self._db.collection(name))

    # ── Queries ────────────────────────────────────────────────────────────

    def execute(
        self, query: str, values: Any = None, *, paramstyle: str | None = None
    ) -> Any:
        return _SyncRunner.get().run(
            self._db.execute(query, values, paramstyle=paramstyle)
        )

    def execute_many(
        self, query: str, values_list: Any, *, paramstyle: str | None = None
    ) -> None:
        return _SyncRunner.get().run(
            self._db.execute_many(query, values_list, paramstyle=paramstyle)
        )

    def fetch_one(
        self, query: str, values: Any = None, *, paramstyle: str | None = None
    ) -> dict[str, Any] | None:
        return _SyncRunner.get().run(
            self._db.fetch_one(query, values, paramstyle=paramstyle)
        )

    def fetch_all(
        self, query: str, values: Any = None, *, paramstyle: str | None = None
    ) -> list[dict[str, Any]]:
        return _SyncRunner.get().run(
            self._db.fetch_all(query, values, paramstyle=paramstyle)
        )

    def fetch_val(
        self,
        query: str,
        values: Any = None,
        column: int = 0,
        *,
        paramstyle: str | None = None,
    ) -> Any:
        return _SyncRunner.get().run(
            self._db.fetch_val(query, values, column, paramstyle=paramstyle)
        )

    def iterate(
        self,
        query: str,
        values: Any = None,
        *,
        batch_size: int = 500,
        paramstyle: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        yield from _SyncRunner.get().iter(
            self._db.iterate(
                query, values, batch_size=batch_size, paramstyle=paramstyle
            )
        )

    def iterate_batches(
        self,
        query: str,
        values: Any = None,
        *,
        batch_size: int = 500,
        paramstyle: str | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        yield from _SyncRunner.get().iter(
            self._db.iterate_batches(
                query, values, batch_size=batch_size, paramstyle=paramstyle
            )
        )

    # ── Transactions / pinned connections ──────────────────────────────────

    def transaction(self) -> _SyncTransaction:
        return _SyncTransaction(self._db)

    def connection(self) -> _SyncPinnedConnection:
        return _SyncPinnedConnection(self._db)

    # ── Model access ───────────────────────────────────────────────────────

    def model(self, model: type[AUniqORMBase]) -> _SyncModelProxy:
        """Return a synchronous proxy for ``model`` (see :func:`sync_model`)."""
        return sync_model(model)
