# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Focused asynchronous MongoDB driver.

Most code should use the keyword-based :class:`~auniqorm.Collection` API from
``db.collection(name)``, which keeps native BSON types.  As a raw escape hatch,
operations can also be passed as JSON documents through the generic
``BaseDriver`` interface; they are parsed in Python and run through the same
path as the collection API, limited to JSON-compatible values.  For example::

    await db.execute(
        '{"operation":"insert_one","collection":"users",'
        '"document":{"name":"Ada"}}'
    )
    users = await db.fetch_all(
        '{"operation":"find","collection":"users","filter":{"active":true}}'
    )

The official PyMongo async API is optional and imported lazily.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
import os
from collections.abc import AsyncIterator, Mapping
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, unquote, urlparse, urlsplit, urlunsplit

from auniqorm.drivers import BaseDriver
from auniqorm.exceptions import QueryError

try:
    import pymongo
    from bson import decode, decode_all, encode
    from bson.errors import BSONError
    from pymongo import AsyncMongoClient
    from pymongo.errors import BulkWriteError as PyMongoBulkWriteError
    from pymongo.errors import PyMongoError
except ImportError:
    if TYPE_CHECKING:
        raise
    AsyncMongoClient = None  # type: ignore[assignment,misc]

try:
    import auniqorm_native
except ImportError:
    auniqorm_native = None

logger = logging.getLogger("auniqorm")

# Codec options PyMongo applies while encoding and decoding BSON in Python. The
# Rust client only transports raw BSON, and rejects these URI options.
_PYTHON_CODEC_OPTIONS = frozenset(
    {
        "uuidrepresentation",
        "tz_aware",
        "tzinfo",
        "unicode_decode_error_handler",
        "datetime_conversion",
    }
)


_NATIVE_FIND_KEYS = frozenset(
    {
        "operation",
        "collection",
        "filter",
        "projection",
        "sort",
        "skip",
        "limit",
        "batch_size",
        "hint",
        "collation",
        "max_time_ms",
        "values",
    }
)


_NATIVE_EXECUTE_OPERATIONS = frozenset(
    {
        "insert_one",
        "insert_many",
        "update_one",
        "update_many",
        "replace_one",
        "delete_one",
        "delete_many",
        "count_documents",
        "distinct",
    }
)
_NATIVE_EXECUTE_KEYS = frozenset(
    {
        "operation",
        "collection",
        "document",
        "documents",
        "filter",
        "update",
        "replacement",
        "upsert",
        "array_filters",
        "collation",
        "hint",
        "skip",
        "limit",
        "key",
        "max_time_ms",
        "values",
    }
)
_NATIVE_AGGREGATE_KEYS = frozenset(
    {
        "operation",
        "collection",
        "pipeline",
        "allow_disk_use",
        "batch_size",
        "hint",
        "collation",
        "max_time_ms",
        "values",
    }
)


def _sort_document(sort: Any) -> Any:
    """Convert PyMongo's sort forms into the ordered document MongoDB expects."""
    if isinstance(sort, str):
        return {sort: 1}
    if isinstance(sort, Mapping):
        return sort
    return dict((key, 1) if isinstance(key, str) else tuple(key) for key in sort)


def _native_command(command: dict[str, Any]) -> dict[str, Any]:
    native = {key: value for key, value in command.items() if key != "values"}
    if "sort" in native:
        native["sort"] = _sort_document(native["sort"])
    return native


def _attach_bridge(engine: Any) -> tuple[asyncio.AbstractEventLoop, Any] | None:
    """Resolve the engine's futures through a completion bridge on this loop.

    The bridge wakes only the event loop thread when native operations finish,
    instead of hopping through extra runtime threads.  Loops without
    ``add_reader`` support (Windows' proactor loop) keep the default path.
    """
    bridge_type = getattr(auniqorm_native, "Bridge", None)
    if bridge_type is None or not hasattr(engine, "use_bridge"):
        return None
    event_loop = asyncio.get_running_loop()
    bridge = bridge_type(event_loop)
    try:
        event_loop.add_reader(bridge.fd, bridge.drain)
    except NotImplementedError:
        return None
    engine.use_bridge(bridge)
    return event_loop, bridge


#: PyMongo keyword options the Rust driver also accepts as URI options.
_NATIVE_URI_OPTIONS = frozenset(
    "appname authmechanism authsource connecttimeoutms directconnection "
    "heartbeatfrequencyms journal loadbalanced localthresholdms maxconnecting "
    "maxidletimems maxpoolsize maxstalenessseconds minpoolsize readconcernlevel "
    "readpreference replicaset retryreads retrywrites servermonitoringmode "
    "serverselectiontimeoutms srvmaxhosts srvservicename ssl tls "
    "tlsallowinvalidcertificates tlscafile tlscertificatekeyfile tlsinsecure w "
    "wtimeoutms".split()
)


def _native_url(url: str, options: Mapping[str, Any] | None = None) -> str | None:
    """The connection string for the native client, or None to disable it.

    Python-only codec options are dropped. Keyword *options*, which PyMongo
    applies on top of the URL, are moved into it (credentials percent-encoded
    into the userinfo). If any cannot be expressed there (SSL contexts,
    callables, event listeners, type registries, ...), the native client is
    disabled and PyMongo handles every operation.
    """
    parts = urlsplit(url)
    added: dict[str, str] = {}
    credentials: dict[str, str] = {}
    unsupported: list[str] = []
    for name, value in (options or {}).items():
        key = name.lower()
        if key in _PYTHON_CODEC_OPTIONS or key == "connect":
            continue  # applied by PyMongo's codec in Python, or irrelevant
        if isinstance(value, bool):
            value = "true" if value else "false"
        elif isinstance(value, float) and value.is_integer():
            value = int(value)
        if key in ("username", "password") and isinstance(value, str):
            credentials[key] = quote(value, safe="")
        elif key in _NATIVE_URI_OPTIONS and isinstance(value, int | str):
            added[key] = str(value)
        else:
            unsupported.append(name)
    netloc = parts.netloc
    if credentials:
        if "@" in netloc or "username" not in credentials:
            unsupported.append("username/password")
        else:
            netloc = ":".join(credentials.values()) + "@" + netloc
    if unsupported:
        logger.info(
            "Native MongoDB client disabled (PyMongo handles all operations): "
            "cannot pass option(s) %s to it",
            ", ".join(sorted(unsupported)),
        )
        return None
    query = [
        option
        for option in parts.query.split("&")
        if option
        and (key := unquote(option.split("=", 1)[0]).lower()) not in added
        and key not in _PYTHON_CODEC_OPTIONS
    ]
    query += [f"{key}={quote(value, safe='')}" for key, value in added.items()]
    return urlunsplit(parts._replace(netloc=netloc, query="&".join(query)))


def _pymongo_request(request: Mapping[str, Mapping[str, Any]]) -> Any:
    ((op, statement),) = request.items()
    if op == "insert":
        return pymongo.InsertOne(dict(statement))
    options = {
        name: statement[key]
        for key, name in (("collation", "collation"), ("hint", "hint"))
        if key in statement
    }
    if op == "delete":
        delete = pymongo.DeleteOne if statement["limit"] else pymongo.DeleteMany
        return delete(statement["q"], **options)
    if op == "replace":
        return pymongo.ReplaceOne(
            statement["q"], statement["u"], upsert=statement["upsert"], **options
        )
    if "arrayFilters" in statement:
        options["array_filters"] = statement["arrayFilters"]
    update = pymongo.UpdateMany if statement["multi"] else pymongo.UpdateOne
    return update(statement["q"], statement["u"], upsert=statement["upsert"], **options)


_INDEX_OPTIONS = {
    "name": "name",
    "unique": "unique",
    "sparse": "sparse",
    "background": "background",
    "hidden": "hidden",
    "collation": "collation",
    "expire_after_seconds": "expireAfterSeconds",
    "partial_filter_expression": "partialFilterExpression",
}


def _index_options(command: Mapping[str, Any]) -> dict[str, Any]:
    """Map snake_case index options to the names MongoDB expects."""
    return {
        server: command[option]
        for option, server in _INDEX_OPTIONS.items()
        if command.get(option) is not None
    }


def _max_time(options: dict[str, Any]) -> dict[str, Any]:
    """Rename ``max_time_ms`` to the ``maxTimeMS`` keyword PyMongo expects."""
    if "max_time_ms" in options:
        options["maxTimeMS"] = options.pop("max_time_ms")
    return options


class MongoDriver(BaseDriver):
    """MongoDB driver backed by PyMongo's native async client."""

    SUPPORTS_MODELS = False
    SUPPORTS_COLLECTIONS = True

    def __init__(self, url: str, **options: Any) -> None:
        super().__init__(url, **options)
        if AsyncMongoClient is None:
            raise ImportError(
                "pymongo is required for MongoDB support. "
                "Install it with: pip install auniqorm[mongodb]"
            )
        parsed = urlparse(url)
        database = parsed.path.lstrip("/")
        if not database:
            raise ValueError("MongoDB URL must include a database name")
        self._database_name = unquote(database.split("/", 1)[0])
        # Transactions belong to the asyncio task that began them (and tasks
        # it starts), so concurrent tasks run independent transactions and
        # never join another task's transaction by accident.
        self._session_var: ContextVar[Any] = ContextVar(
            f"auniqorm_mongo_session_{id(self)}", default=None
        )
        self._native_session_var: ContextVar[Any] = ContextVar(
            f"auniqorm_mongo_native_session_{id(self)}", default=None
        )
        self._client: Any = None
        self._db: Any = None
        self._collections: dict[str, Any] = {}
        self._codec_options: Any = None
        self._native: Any = None
        self._bridge: tuple[asyncio.AbstractEventLoop, Any] | None = None
        self._use_native = (
            auniqorm_native is not None
            and hasattr(auniqorm_native, "connect_mongo")
            and os.getenv("AUNIQORM_MONGODB_NATIVE", "1").lower()
            not in {"0", "false", "no", "off"}
        )

    @property
    def is_connected(self) -> bool:
        return self._client is not None and self._db is not None

    @property
    def _session(self) -> Any:
        """The current task's PyMongo transaction session, if any."""
        return self._session_var.get()

    @_session.setter
    def _session(self, session: Any) -> None:
        self._session_var.set(session)

    @property
    def _native_session(self) -> Any:
        """The current task's native transaction, if any."""
        return self._native_session_var.get()

    @_native_session.setter
    def _native_session(self, session: Any) -> None:
        self._native_session_var.set(session)

    @property
    def in_transaction(self) -> bool:
        return self._session is not None or self._native_session is not None

    async def connect(self) -> None:
        if self.is_connected:
            return
        options = dict(self._options)
        self._client = AsyncMongoClient(self._url, **options)
        await self._client.aconnect()
        self._db = self._client[self._database_name]
        self._codec_options = self._client.codec_options
        native_url = _native_url(self._url, options) if self._use_native else None
        if native_url is not None:
            try:
                self._native = await auniqorm_native.connect_mongo(
                    native_url, self._database_name
                )
            except RuntimeError as exc:
                logger.warning(
                    "Native MongoDB driver unavailable, using PyMongo: %s", exc
                )
            else:
                self._bridge = _attach_bridge(self._native)

    async def disconnect(self) -> None:
        if self.in_transaction:
            await self.rollback()
        if self._native is not None:
            await self._native.close()
            self._native = None
        if self._bridge is not None:
            event_loop, bridge = self._bridge
            event_loop.remove_reader(bridge.fd)
            self._bridge = None
        if self._client is not None:
            close_result = self._client.close()
            if inspect.isawaitable(close_result):
                await close_result
        self._client = None
        self._db = None
        self._codec_options = None
        self._collections.clear()

    @staticmethod
    def _command(query: str, values: list[Any] | None) -> dict[str, Any]:
        try:
            command = json.loads(query)
        except (TypeError, json.JSONDecodeError) as exc:
            raise QueryError(
                "MongoDB queries must be JSON operation documents"
            ) from exc
        if not isinstance(command, dict) or "operation" not in command:
            raise QueryError("MongoDB query requires an 'operation' field")
        if values is not None:
            command.setdefault("values", values)
        return command

    def _collection(self, command: dict[str, Any]) -> Any:
        if self._db is None:
            raise QueryError("Not connected")
        collection = command.get("collection")
        if not isinstance(collection, str) or not collection:
            raise QueryError("MongoDB operation requires a collection")
        cached = self._collections.get(collection)
        if cached is None:
            cached = self._db[collection]
            self._collections[collection] = cached
        return cached

    def _kwargs(self, command: dict[str, Any], *allowed: str) -> dict[str, Any]:
        values = command.get("values")
        kwargs = dict(values) if isinstance(values, dict) else {}
        for key in allowed:
            if key in command:
                kwargs.setdefault(key, command[key])
        if kwargs.get("sort") is not None:
            kwargs["sort"] = list(_sort_document(kwargs["sort"]).items())
        return kwargs

    def _session_kwargs(self) -> dict[str, Any]:
        if self._native_session is not None:
            raise QueryError(
                "This MongoDB operation is not supported by the native driver "
                "inside a transaction; set AUNIQORM_MONGODB_NATIVE=0 to run "
                "transactions through PyMongo"
            )
        return {"session": self._session} if self._session is not None else {}

    def _native_execute(self, command: dict[str, Any]) -> bool:
        return (
            self._native is not None
            and self._session is None
            and command["operation"] in _NATIVE_EXECUTE_OPERATIONS
            and not isinstance(command.get("values"), dict)
            and command.keys() <= _NATIVE_EXECUTE_KEYS
        )

    async def execute(self, query: str, values: list[Any] | None = None) -> Any:
        command = self._command(query, values)
        if self._native_execute(command):
            return await self.execute_operation(_native_command(command))
        return await self._execute_command(command)

    async def execute_operation(self, command: dict[str, Any]) -> Any:
        """Execute a write or admin operation built by :class:`~auniqorm.Collection`.

        The operation is encoded once with PyMongo's BSON encoder and the
        client's codec options, so values keep their native BSON types.
        """
        if (
            self._native is not None
            and self._session is None
            and command["operation"] in _NATIVE_EXECUTE_OPERATIONS
        ):
            try:
                result = await self._native.execute_bson(
                    encode(command, codec_options=self._codec_options),
                    self._native_session,
                )
                # Counts come back as plain integers; other results as BSON.
                if type(result) is int:
                    return result
                return decode(result, codec_options=self._codec_options)["result"]
            except (BSONError, OverflowError, RuntimeError, ValueError) as exc:
                raise QueryError(f"MongoDB execute failed: {exc}") from exc
        # PyMongo adds ``_id`` to inserted documents; keep callers' data intact.
        if "document" in command:
            command = {**command, "document": dict(command["document"])}
        if "documents" in command:
            command = {**command, "documents": [dict(d) for d in command["documents"]]}
        return await self._execute_command(command)

    async def bulk_write_operation(self, command: dict[str, Any]) -> dict[str, Any]:
        """Run a bulk write built by :class:`~auniqorm.Collection`.

        Returns PyMongo's ``bulk_api_result`` summary, including any write
        errors, so callers decide how to report them.
        """
        if self._native is not None and self._session is None:
            try:
                encoded = await self._native.bulk_write_bson(
                    encode(command, codec_options=self._codec_options),
                    self._native_session,
                )
                return decode(encoded, codec_options=self._codec_options)["result"]
            except (BSONError, RuntimeError, ValueError) as exc:
                raise QueryError(f"MongoDB bulk write failed: {exc}") from exc
        requests = [_pymongo_request(request) for request in command["requests"]]
        try:
            result = await self._collection(command).bulk_write(
                requests, ordered=command["ordered"], **self._session_kwargs()
            )
        except PyMongoBulkWriteError as exc:
            return dict(exc.details)
        except Exception as exc:
            raise QueryError(f"MongoDB bulk write failed: {exc}") from exc
        return dict(result.bulk_api_result)

    async def fetch_operation(self, command: dict[str, Any]) -> list[dict[str, Any]]:
        """Run a ``find``, ``find_one``, or ``aggregate`` from a collection."""
        if self._native is not None and self._session is None:
            try:
                encoded, rest = await self._native.fetch_bson(
                    encode(command, codec_options=self._codec_options),
                    self._native_session,
                )
                rows = decode_all(encoded, codec_options=self._codec_options)
                if rest is not None:
                    try:
                        while (batch := await rest.next_batch()) is not None:
                            rows.extend(
                                decode_all(batch, codec_options=self._codec_options)
                            )
                    finally:
                        rest.close()
                return rows
            except (BSONError, RuntimeError, ValueError) as exc:
                raise QueryError(f"MongoDB fetch failed: {exc}") from exc
        if command["operation"] == "find_one":
            row = await self._find_one(command)
            return [] if row is None else [row]
        if command["operation"] == "aggregate":
            return await self._aggregate(command)
        return await self._find(command)

    def _native_aggregate(self, command: dict[str, Any]) -> bool:
        return (
            self._native is not None
            and self._session is None
            and not isinstance(command.get("values"), dict)
            and command.keys() <= _NATIVE_AGGREGATE_KEYS
        )

    async def _aggregate(self, command: dict[str, Any]) -> list[dict[str, Any]]:
        options = _max_time(
            self._kwargs(command, "hint", "collation", "max_time_ms", "let")
        )
        if "allow_disk_use" in command:
            options["allowDiskUse"] = command["allow_disk_use"]
        if "batch_size" in command:
            options["batchSize"] = command["batch_size"]
        try:
            cursor = await self._collection(command).aggregate(
                command.get("pipeline", []), **self._session_kwargs(), **options
            )
            return [row if isinstance(row, dict) else dict(row) async for row in cursor]
        except Exception as exc:
            raise QueryError(f"MongoDB aggregate failed: {exc}") from exc

    async def _execute_command(self, command: dict[str, Any]) -> Any:
        collection = self._collection(command)
        operation = command["operation"]
        try:
            if operation == "insert_one":
                result = await collection.insert_one(
                    command["document"], **self._session_kwargs()
                )
                return result.inserted_id
            if operation == "insert_many":
                result = await collection.insert_many(
                    command["documents"], **self._session_kwargs()
                )
                return result.inserted_ids
            if operation in {"update_one", "update_many", "replace_one"}:
                method = getattr(collection, operation)
                result = await method(
                    command.get("filter", {}),
                    command.get("update", command.get("replacement")),
                    **self._session_kwargs(),
                    **self._kwargs(
                        command, "upsert", "array_filters", "collation", "hint"
                    ),
                )
                return result.modified_count
            if operation in {"delete_one", "delete_many"}:
                result = await getattr(collection, operation)(
                    command.get("filter", {}),
                    **self._session_kwargs(),
                    **self._kwargs(command, "collation", "hint"),
                )
                return result.deleted_count
            if operation == "drop":
                return await collection.drop(**self._session_kwargs())
            if operation == "count_documents":
                return await collection.count_documents(
                    command.get("filter", {}),
                    **self._session_kwargs(),
                    **_max_time(
                        self._kwargs(
                            command, "skip", "limit", "hint", "collation", "max_time_ms"
                        )
                    ),
                )
            if operation == "distinct":
                return await collection.distinct(
                    command["key"],
                    command.get("filter", {}),
                    **self._session_kwargs(),
                    **_max_time(self._kwargs(command, "collation", "max_time_ms")),
                )
            if operation == "create_index":
                return await collection.create_index(
                    list(_sort_document(command["keys"]).items()),
                    **self._session_kwargs(),
                    **_index_options(command),
                )
            if operation == "drop_index":
                index = command["index"]
                if not isinstance(index, str):
                    index = list(_sort_document(index).items())
                return await collection.drop_index(index, **self._session_kwargs())
            raise QueryError(f"Unsupported MongoDB execute operation: {operation}")
        except QueryError:
            raise
        except Exception as exc:
            raise QueryError(f"MongoDB execute failed: {exc}") from exc

    async def execute_many(self, query: str, values_list: list[list[Any]]) -> None:
        for values in values_list:
            await self.execute(query, values)

    async def fetch_one(
        self, query: str, values: list[Any] | None = None
    ) -> dict[str, Any] | None:
        command = self._command(query, values)
        if command["operation"] != "find_one":
            raise QueryError("fetch_one requires the find_one operation")
        if self._native_find(command):
            rows = await self.fetch_operation(_native_command(command))
            return rows[0] if rows else None
        return await self._find_one(command)

    async def _find_one(self, command: dict[str, Any]) -> dict[str, Any] | None:
        try:
            result = await self._collection(command).find_one(
                command.get("filter", {}),
                **self._session_kwargs(),
                **self._kwargs(
                    command,
                    "projection",
                    "sort",
                    "skip",
                    "limit",
                    "hint",
                    "collation",
                    "max_time_ms",
                ),
            )
            if result is None or isinstance(result, dict):
                return result
            return dict(result)
        except Exception as exc:
            raise QueryError(f"MongoDB fetch_one failed: {exc}") from exc

    async def fetch_all(
        self, query: str, values: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        command = self._command(query, values)
        if command["operation"] == "aggregate":
            if self._native_aggregate(command):
                return await self.fetch_operation(_native_command(command))
            return await self._aggregate(command)
        if command["operation"] != "find":
            raise QueryError("fetch_all requires the find or aggregate operation")
        if self._native_find(command):
            return await self.fetch_operation(_native_command(command))
        return await self._find(command)

    async def _find(self, command: dict[str, Any]) -> list[dict[str, Any]]:
        try:
            cursor = self._collection(command).find(
                command.get("filter", {}),
                **self._session_kwargs(),
                **self._kwargs(
                    command,
                    "projection",
                    "sort",
                    "skip",
                    "limit",
                    "hint",
                    "collation",
                    "max_time_ms",
                    "batch_size",
                ),
            )
            return [row if isinstance(row, dict) else dict(row) async for row in cursor]
        except Exception as exc:
            raise QueryError(f"MongoDB fetch_all failed: {exc}") from exc

    async def fetch_val(
        self, query: str, values: list[Any] | None = None, column: int = 0
    ) -> Any:
        row = await self.fetch_one(query, values)
        if row is None:
            return None
        return next(iter(row.values())) if column == 0 else list(row.values())[column]

    async def begin(self) -> None:
        if self._client is None:
            raise QueryError("Not connected")
        if self.in_transaction:
            raise QueryError("Transaction already active")
        if self._native is not None:
            # The native session starts with the first operation, so errors
            # such as a standalone server surface there, as with PyMongo.
            self._native_session = self._native.begin_transaction()
            return
        session = self._client.start_session()
        try:
            await session.start_transaction()
        except BaseException:
            await session.end_session()
            raise
        self._session = session

    async def commit(self) -> None:
        native, self._native_session = self._native_session, None
        if native is not None and not native.end_if_unused():
            try:
                await native.commit()
            except RuntimeError as exc:
                raise QueryError(f"MongoDB commit failed: {exc}") from exc
        session, self._session = self._session, None
        if session is not None:
            try:
                await session.commit_transaction()
            finally:
                await session.end_session()

    async def rollback(self) -> None:
        native, self._native_session = self._native_session, None
        if native is not None and not native.end_if_unused():
            # Like PyMongo, aborting is best effort: the server discards the
            # transaction when it times out anyway.
            with contextlib.suppress(RuntimeError):
                await native.abort()
        session, self._session = self._session, None
        if session is not None:
            try:
                await session.abort_transaction()
            finally:
                await session.end_session()

    async def iterate(
        self, query: str, values: list[Any] | None = None, *, batch_size: int = 500
    ) -> AsyncIterator[dict[str, Any]]:
        async for batch in self.iterate_batches(query, values, batch_size=batch_size):
            for row in batch:
                yield row

    async def iterate_batches(
        self, query: str, values: list[Any] | None = None, *, batch_size: int = 500
    ) -> AsyncIterator[list[dict[str, Any]]]:
        command = self._command(query, values)
        if command["operation"] != "find":
            raise QueryError("iterate requires the find operation")
        async for batch in self.iterate_operation(command, batch_size=batch_size):
            yield batch

    def _native_find(self, command: dict[str, Any]) -> bool:
        return (
            self._native is not None
            and self._session is None
            and not isinstance(command.get("values"), dict)
            and command.keys() <= _NATIVE_FIND_KEYS
        )

    async def iterate_operation(
        self, command: dict[str, Any], *, batch_size: int
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Stream a ``find`` in batches of decoded documents.

        The native cursor prefetches the next batches while the caller
        processes the current one.
        """
        if not self._native_find(command):
            async for batch in self._iterate_pymongo(command, batch_size):
                yield batch
            return
        if self._native_session is not None:
            # Session cursors cannot outlive one engine call; read, then chunk.
            rows = await self.fetch_operation(_native_command(command))
            for start in range(0, len(rows), batch_size):
                yield rows[start : start + batch_size]
            return
        native_command = _native_command(command)
        native_command["batch_size"] = batch_size
        try:
            cursor = await self._native.open_cursor_bson(
                encode(native_command, codec_options=self._codec_options), batch_size
            )
        except (BSONError, RuntimeError, ValueError) as exc:
            raise QueryError(f"MongoDB iterate failed: {exc}") from exc
        try:
            while True:
                try:
                    encoded = await cursor.next_batch()
                except RuntimeError as exc:
                    raise QueryError(f"MongoDB iterate failed: {exc}") from exc
                if encoded is None:
                    return
                yield decode_all(encoded, codec_options=self._codec_options)
        finally:
            cursor.close()

    async def _iterate_pymongo(
        self, command: dict[str, Any], batch_size: int
    ) -> AsyncIterator[list[dict[str, Any]]]:
        cursor = self._collection(command).find(
            command.get("filter", {}),
            batch_size=batch_size,
            **self._session_kwargs(),
            **self._kwargs(
                command,
                "projection",
                "sort",
                "skip",
                "limit",
                "hint",
                "collation",
                "max_time_ms",
            ),
        )
        batch: list[dict[str, Any]] = []
        rows = aiter(cursor)
        try:
            while True:
                try:
                    row = await anext(rows)
                except StopAsyncIteration:
                    break
                except PyMongoError as exc:
                    raise QueryError(f"MongoDB iterate failed: {exc}") from exc
                batch.append(row if isinstance(row, dict) else dict(row))
                if len(batch) == batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch
        finally:
            await cursor.close()
