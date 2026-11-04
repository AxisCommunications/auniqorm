# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Keyword-based MongoDB collection API.

Obtain a collection from a connected MongoDB :class:`~auniqorm.Database`::

    users = db.collection("users")
    await users.insert_one({"name": "Ada", "created": datetime.now(UTC)})
    await users.update_one(
        {"_id": user_id},
        set={"profile.name": "Ada"},
        inc={"login_count": 1},
    )
    active = await users.find({"active": True}, sort={"name": 1}, limit=50)

Operations are encoded once with PyMongo's BSON encoder, so values keep their
native BSON types (``datetime`` → Date, ``ObjectId``, ``Decimal128``, …) and
the native Rust driver sends them to MongoDB without re-encoding.  MongoDB
update operators are exposed as keyword arguments (``set`` → ``$set``,
``inc`` → ``$inc``, …); pass ``update=`` for anything else, including
aggregation pipelines.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from auniqorm.exceptions import BulkWriteError

if TYPE_CHECKING:
    from auniqorm.connection import Database

Document = Mapping[str, Any]
Sort = str | Mapping[str, int] | Sequence[tuple[str, int]]
Keys = str | Mapping[str, Any] | Sequence[tuple[str, Any]]
Hint = Keys
Collation = Mapping[str, Any]


def _sort(sort: Keys | None) -> dict[str, Any] | None:
    if sort is None:
        return None
    if isinstance(sort, str):
        return {sort: 1}
    return dict(sort)


def _unset(fields: Document | str | Iterable[str]) -> Document:
    if isinstance(fields, str):
        return {fields: ""}
    if isinstance(fields, Mapping):
        return fields
    return dict.fromkeys(fields, "")


def _update(
    update: Document | Sequence[Document] | None,
    set: Document | None,
    unset: Document | str | Iterable[str] | None,
    inc: Document | None,
    mul: Document | None,
    min: Document | None,
    max: Document | None,
    rename: Mapping[str, str] | None,
    current_date: Document | None,
    set_on_insert: Document | None,
    push: Document | None,
    pull: Document | None,
    add_to_set: Document | None,
) -> Document | list[Document]:
    """Build the update document from operator keywords or ``update=``."""
    named: dict[str, Any] = {}
    if set is not None:
        named["$set"] = set
    if unset is not None:
        named["$unset"] = _unset(unset)
    if inc is not None:
        named["$inc"] = inc
    if mul is not None:
        named["$mul"] = mul
    if min is not None:
        named["$min"] = min
    if max is not None:
        named["$max"] = max
    if rename is not None:
        named["$rename"] = rename
    if current_date is not None:
        named["$currentDate"] = current_date
    if set_on_insert is not None:
        named["$setOnInsert"] = set_on_insert
    if push is not None:
        named["$push"] = push
    if pull is not None:
        named["$pull"] = pull
    if add_to_set is not None:
        named["$addToSet"] = add_to_set
    if update is not None:
        if named:
            raise ValueError("Pass either update= or operator keywords, not both")
        return update if isinstance(update, Mapping) else list(update)
    if not named:
        raise ValueError("An update requires at least one operator")
    return named


def _options(
    command: dict[str, Any],
    hint: Hint | None,
    collation: Collation | None,
    max_time_ms: int | None,
) -> dict[str, Any]:
    if hint is not None:
        command["hint"] = hint
    if collation is not None:
        command["collation"] = collation
    if max_time_ms is not None:
        command["max_time_ms"] = max_time_ms
    return command


class WriteRequest:
    """A single write in :meth:`Collection.bulk_write`."""

    __slots__ = ("_op", "_statement")

    def __init__(self, op: str, statement: Document) -> None:
        self._op = op
        self._statement = statement

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._statement!r})"

    def _request(self) -> dict[str, Document]:
        return {self._op: self._statement}


class InsertOne(WriteRequest):
    """Insert *document*."""

    __slots__ = ()

    def __init__(self, document: Document) -> None:
        super().__init__("insert", document)


class UpdateOne(WriteRequest):
    """Update the first document matching *where*.

    Takes the same operator keywords as :meth:`Collection.update_one`.
    """

    __slots__ = ()
    _multi = False

    def __init__(
        self,
        where: Document | None = None,
        *,
        set: Document | None = None,
        unset: Document | str | Iterable[str] | None = None,
        inc: Document | None = None,
        mul: Document | None = None,
        min: Document | None = None,
        max: Document | None = None,
        rename: Mapping[str, str] | None = None,
        current_date: Document | None = None,
        set_on_insert: Document | None = None,
        push: Document | None = None,
        pull: Document | None = None,
        add_to_set: Document | None = None,
        update: Document | Sequence[Document] | None = None,
        upsert: bool = False,
        array_filters: Sequence[Document] | None = None,
        collation: Collation | None = None,
        hint: Hint | None = None,
    ) -> None:
        statement: dict[str, Any] = {
            "q": where or {},
            "u": _update(
                update,
                set,
                unset,
                inc,
                mul,
                min,
                max,
                rename,
                current_date,
                set_on_insert,
                push,
                pull,
                add_to_set,
            ),
            "multi": self._multi,
            "upsert": upsert,
        }
        if array_filters is not None:
            statement["arrayFilters"] = list(array_filters)
        if collation is not None:
            statement["collation"] = collation
        if hint is not None:
            statement["hint"] = hint
        super().__init__("update", statement)


class UpdateMany(UpdateOne):
    """Update every document matching *where* (see :class:`UpdateOne`)."""

    __slots__ = ()
    _multi = True


class ReplaceOne(WriteRequest):
    """Replace the first document matching *where* with *replacement*."""

    __slots__ = ()

    def __init__(
        self,
        where: Document,
        replacement: Document,
        *,
        upsert: bool = False,
        collation: Collation | None = None,
        hint: Hint | None = None,
    ) -> None:
        if any(key.startswith("$") for key in replacement):
            raise ValueError("A replacement document must not use update operators")
        statement = {"q": where, "u": replacement, "multi": False, "upsert": upsert}
        super().__init__("replace", _options(statement, hint, collation, None))


class DeleteOne(WriteRequest):
    """Delete the first document matching *where*."""

    __slots__ = ()
    _limit = 1

    def __init__(
        self,
        where: Document | None = None,
        *,
        collation: Collation | None = None,
        hint: Hint | None = None,
    ) -> None:
        statement = {"q": where or {}, "limit": self._limit}
        super().__init__("delete", _options(statement, hint, collation, None))


class DeleteMany(DeleteOne):
    """Delete every document matching *where*."""

    __slots__ = ()
    _limit = 0


@dataclass(frozen=True, slots=True)
class BulkWriteResult:
    """Counts from :meth:`Collection.bulk_write`.

    ``upserted_ids`` maps request indexes to the ``_id`` of upserted documents.
    """

    inserted_count: int = 0
    matched_count: int = 0
    modified_count: int = 0
    deleted_count: int = 0
    upserted_count: int = 0
    upserted_ids: dict[int, Any] = field(default_factory=dict)

    @classmethod
    def _from_details(cls, details: Mapping[str, Any]) -> BulkWriteResult:
        if details.get("writeErrors") or details.get("writeConcernErrors"):
            raise BulkWriteError(dict(details))
        return cls(
            inserted_count=details["nInserted"],
            matched_count=details["nMatched"],
            modified_count=details["nModified"],
            deleted_count=details["nRemoved"],
            upserted_count=details["nUpserted"],
            upserted_ids={
                entry["index"]: entry["_id"] for entry in details["upserted"]
            },
        )


class Collection:
    """A MongoDB collection bound to a :class:`~auniqorm.Database`.

    Filters are plain mappings using MongoDB query syntax.  Inside a
    transaction, operations run through PyMongo with the active session.
    """

    __slots__ = ("_db", "_name")

    def __init__(self, db: Database, name: str) -> None:
        if not name:
            raise ValueError("Collection name must not be empty")
        self._db = db
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return f"Collection({self._name!r})"

    # Hot methods build their command dict inline and await the driver once:
    # under concurrency the event loop thread is the bottleneck, so per-call
    # work such as keyword dicts or extra coroutine layers costs throughput.

    async def _execute(self, operation: str, **fields: Any) -> Any:
        command = {"operation": operation, "collection": self._name}
        command.update(
            (key, value) for key, value in fields.items() if value is not None
        )
        return await self._db._collection_driver().execute_operation(command)

    def _find_command(
        self,
        operation: str,
        where: Document | None,
        projection: Document | None,
        sort: Keys | None,
        skip: int | None,
        limit: int | None,
        hint: Hint | None,
        collation: Collation | None,
        max_time_ms: int | None,
    ) -> dict[str, Any]:
        command: dict[str, Any] = {"operation": operation, "collection": self._name}
        if where is not None:
            command["filter"] = where
        if projection is not None:
            command["projection"] = projection
        if sort is not None:
            command["sort"] = _sort(sort)
        if skip is not None:
            command["skip"] = skip
        if limit is not None:
            command["limit"] = limit
        return _options(command, hint, collation, max_time_ms)

    async def insert_one(self, document: Document) -> Any:
        """Insert *document* and return its ``_id``."""
        command = {
            "operation": "insert_one",
            "collection": self._name,
            "document": document,
        }
        return await self._db._collection_driver().execute_operation(command)

    async def insert_many(self, documents: Iterable[Document]) -> list[Any]:
        """Insert *documents* in order and return their ``_id`` values."""
        command = {
            "operation": "insert_many",
            "collection": self._name,
            "documents": list(documents),
        }
        ids: list[Any] = await self._db._collection_driver().execute_operation(command)
        return ids

    async def create_index(
        self,
        keys: Keys,
        *,
        name: str | None = None,
        unique: bool | None = None,
        sparse: bool | None = None,
        hidden: bool | None = None,
        expire_after_seconds: int | None = None,
        partial_filter_expression: Document | None = None,
        collation: Collation | None = None,
    ) -> str:
        """Create an index on *keys* and return its name.

        *keys* takes the same forms as ``sort``: a field name, a mapping, or
        ``(field, direction)`` pairs, where the direction may also be an index
        type such as ``"text"`` or ``"2dsphere"``.
        """
        index: str = await self._execute(
            "create_index",
            keys=_sort(keys),
            name=name,
            unique=unique,
            sparse=sparse,
            hidden=hidden,
            expire_after_seconds=expire_after_seconds,
            partial_filter_expression=partial_filter_expression,
            collation=collation,
        )
        return index

    async def drop_index(self, index: Keys) -> None:
        """Drop an index by name or by its key specification."""
        await self._execute(
            "drop_index", index=index if isinstance(index, str) else _sort(index)
        )

    async def drop(self) -> None:
        """Drop the collection and its indexes."""
        await self._execute("drop")

    async def bulk_write(
        self, requests: Iterable[WriteRequest], *, ordered: bool = True
    ) -> BulkWriteResult:
        """Send many writes in as few round trips as possible.

        Consecutive writes of the same kind are batched into one command.
        With ``ordered=True`` the writes run in order and stop at the first
        error; otherwise they may be reordered and every write is attempted.
        Raises :class:`~auniqorm.exceptions.BulkWriteError` when any write
        fails, with the outcome in ``details``.
        """
        command = {
            "operation": "bulk_write",
            "collection": self._name,
            "ordered": ordered,
            "requests": [request._request() for request in requests],
        }
        if not command["requests"]:
            raise ValueError("bulk_write requires at least one request")
        details = await self._db._collection_driver().bulk_write_operation(command)
        return BulkWriteResult._from_details(details)

    async def find(
        self,
        where: Document | None = None,
        *,
        projection: Document | None = None,
        sort: Sort | None = None,
        skip: int | None = None,
        limit: int | None = None,
        hint: Hint | None = None,
        collation: Collation | None = None,
        max_time_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return all documents matching *where*.

        ``hint`` selects an index (name or key specification), ``collation``
        sets string comparison rules, and ``max_time_ms`` bounds server time.
        """
        command = self._find_command(
            "find",
            where,
            projection,
            sort,
            skip,
            limit,
            hint,
            collation,
            max_time_ms,
        )
        rows: list[
            dict[str, Any]
        ] = await self._db._collection_driver().fetch_operation(command)
        return rows

    async def iterate(
        self,
        where: Document | None = None,
        *,
        projection: Document | None = None,
        sort: Sort | None = None,
        skip: int | None = None,
        limit: int | None = None,
        batch_size: int = 500,
        hint: Hint | None = None,
        collation: Collation | None = None,
        max_time_ms: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Stream documents matching *where* without loading them all at once.

        The native driver fetches the next batches while you process the
        current one.
        """
        batches = self.iterate_batches(
            where,
            projection=projection,
            sort=sort,
            skip=skip,
            limit=limit,
            batch_size=batch_size,
            hint=hint,
            collation=collation,
            max_time_ms=max_time_ms,
        )
        async for batch in batches:
            for row in batch:
                yield row

    async def iterate_batches(
        self,
        where: Document | None = None,
        *,
        projection: Document | None = None,
        sort: Sort | None = None,
        skip: int | None = None,
        limit: int | None = None,
        batch_size: int = 500,
        hint: Hint | None = None,
        collation: Collation | None = None,
        max_time_ms: int | None = None,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """Stream documents matching *where* in lists of up to *batch_size*."""
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        command = self._find_command(
            "find",
            where,
            projection,
            sort,
            skip,
            limit,
            hint,
            collation,
            max_time_ms,
        )
        driver = self._db._collection_driver()
        async for batch in driver.iterate_operation(command, batch_size=batch_size):
            yield batch

    async def aggregate(
        self,
        pipeline: Sequence[Document],
        *,
        allow_disk_use: bool | None = None,
        batch_size: int | None = None,
        hint: Hint | None = None,
        collation: Collation | None = None,
        max_time_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        """Run an aggregation *pipeline* and return the resulting documents."""
        command: dict[str, Any] = {
            "operation": "aggregate",
            "collection": self._name,
            "pipeline": list(pipeline),
        }
        if allow_disk_use is not None:
            command["allow_disk_use"] = allow_disk_use
        if batch_size is not None:
            command["batch_size"] = batch_size
        _options(command, hint, collation, max_time_ms)
        rows: list[
            dict[str, Any]
        ] = await self._db._collection_driver().fetch_operation(command)
        return rows

    async def count_documents(
        self,
        where: Document | None = None,
        *,
        skip: int | None = None,
        limit: int | None = None,
        hint: Hint | None = None,
        collation: Collation | None = None,
        max_time_ms: int | None = None,
    ) -> int:
        """Count documents matching *where*."""
        command: dict[str, Any] = {
            "operation": "count_documents",
            "collection": self._name,
            "filter": where or {},
        }
        if skip is not None:
            command["skip"] = skip
        if limit is not None:
            command["limit"] = limit
        _options(command, hint, collation, max_time_ms)
        count: int = await self._db._collection_driver().execute_operation(command)
        return count

    async def distinct(
        self,
        key: str,
        where: Document | None = None,
        *,
        collation: Collation | None = None,
        max_time_ms: int | None = None,
    ) -> list[Any]:
        """Return the distinct values of *key* among documents matching *where*."""
        command: dict[str, Any] = {
            "operation": "distinct",
            "collection": self._name,
            "key": key,
            "filter": where or {},
        }
        _options(command, None, collation, max_time_ms)
        values: list[Any] = await self._db._collection_driver().execute_operation(
            command
        )
        return values

    async def find_one(
        self,
        where: Document | None = None,
        *,
        projection: Document | None = None,
        sort: Sort | None = None,
        skip: int | None = None,
        hint: Hint | None = None,
        collation: Collation | None = None,
        max_time_ms: int | None = None,
    ) -> dict[str, Any] | None:
        """Return the first document matching *where*, or ``None``."""
        command = self._find_command(
            "find_one",
            where,
            projection,
            sort,
            skip,
            None,
            hint,
            collation,
            max_time_ms,
        )
        rows = await self._db._collection_driver().fetch_operation(command)
        return rows[0] if rows else None

    async def update_one(
        self,
        where: Document | None = None,
        *,
        set: Document | None = None,
        unset: Document | str | Iterable[str] | None = None,
        inc: Document | None = None,
        mul: Document | None = None,
        min: Document | None = None,
        max: Document | None = None,
        rename: Mapping[str, str] | None = None,
        current_date: Document | None = None,
        set_on_insert: Document | None = None,
        push: Document | None = None,
        pull: Document | None = None,
        add_to_set: Document | None = None,
        update: Document | Sequence[Document] | None = None,
        upsert: bool = False,
        array_filters: Sequence[Document] | None = None,
        collation: Collation | None = None,
        hint: Hint | None = None,
    ) -> int:
        """Update the first document matching *where*; return the modified count.

        ``unset`` also accepts a field name or an iterable of field names.
        """
        command: dict[str, Any] = {
            "operation": "update_one",
            "collection": self._name,
            "filter": where or {},
            "update": _update(
                update,
                set,
                unset,
                inc,
                mul,
                min,
                max,
                rename,
                current_date,
                set_on_insert,
                push,
                pull,
                add_to_set,
            ),
            "upsert": upsert,
        }
        if array_filters is not None:
            command["array_filters"] = list(array_filters)
        _options(command, hint, collation, None)
        count: int = await self._db._collection_driver().execute_operation(command)
        return count

    async def update_many(
        self,
        where: Document | None = None,
        *,
        set: Document | None = None,
        unset: Document | str | Iterable[str] | None = None,
        inc: Document | None = None,
        mul: Document | None = None,
        min: Document | None = None,
        max: Document | None = None,
        rename: Mapping[str, str] | None = None,
        current_date: Document | None = None,
        set_on_insert: Document | None = None,
        push: Document | None = None,
        pull: Document | None = None,
        add_to_set: Document | None = None,
        update: Document | Sequence[Document] | None = None,
        upsert: bool = False,
        array_filters: Sequence[Document] | None = None,
        collation: Collation | None = None,
        hint: Hint | None = None,
    ) -> int:
        """Update every document matching *where*; return the modified count.

        Takes the same operator keywords as :meth:`update_one`.
        """
        command: dict[str, Any] = {
            "operation": "update_many",
            "collection": self._name,
            "filter": where or {},
            "update": _update(
                update,
                set,
                unset,
                inc,
                mul,
                min,
                max,
                rename,
                current_date,
                set_on_insert,
                push,
                pull,
                add_to_set,
            ),
            "upsert": upsert,
        }
        if array_filters is not None:
            command["array_filters"] = list(array_filters)
        _options(command, hint, collation, None)
        count: int = await self._db._collection_driver().execute_operation(command)
        return count

    async def replace_one(
        self,
        where: Document,
        replacement: Document,
        *,
        upsert: bool = False,
        collation: Collation | None = None,
        hint: Hint | None = None,
    ) -> int:
        """Replace the first document matching *where*; return the modified count."""
        command: dict[str, Any] = {
            "operation": "replace_one",
            "collection": self._name,
            "filter": where,
            "replacement": replacement,
            "upsert": upsert,
        }
        _options(command, hint, collation, None)
        count: int = await self._db._collection_driver().execute_operation(command)
        return count

    async def delete_one(
        self,
        where: Document | None = None,
        *,
        collation: Collation | None = None,
        hint: Hint | None = None,
    ) -> int:
        """Delete the first document matching *where*; return the deleted count."""
        command: dict[str, Any] = {
            "operation": "delete_one",
            "collection": self._name,
            "filter": where or {},
        }
        _options(command, hint, collation, None)
        count: int = await self._db._collection_driver().execute_operation(command)
        return count

    async def delete_many(
        self,
        where: Document | None = None,
        *,
        collation: Collation | None = None,
        hint: Hint | None = None,
    ) -> int:
        """Delete every document matching *where*; return the deleted count."""
        command: dict[str, Any] = {
            "operation": "delete_many",
            "collection": self._name,
            "filter": where or {},
        }
        _options(command, hint, collation, None)
        count: int = await self._db._collection_driver().execute_operation(command)
        return count
