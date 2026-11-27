# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""aUniqORM model base class using msgspec.Struct."""

from __future__ import annotations

import logging
import math
import re
import sys
import types as _stdlib_types
import weakref
from collections.abc import AsyncIterator
from contextvars import ContextVar
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Self,
    Union,
    cast,
    dataclass_transform,
    get_args,
    get_origin,
    get_type_hints,
)

import msgspec

from auniqorm.columns import Column
from auniqorm.exceptions import QueryError, SchemaMigrationError
from auniqorm.schema import (
    DDLGenerator,
    python_to_db_type,
    unwrap_optional,
    validate_identifier,
)

if TYPE_CHECKING:
    from auniqorm.connection import Database

logger = logging.getLogger("auniqorm")

# Cap for caches whose keys vary at runtime (F literals, order_by lists).
_CACHE_LIMIT = 4096

_COMPARISON_OPS = {"lt": "<", "lte": "<=", "gt": ">", "gte": ">=", "ne": "!="}

# Pattern lookups: (case_insensitive, prefix, suffix).  The value itself is
# escaped, so only these wildcards are active.
_LIKE_LOOKUPS: dict[str, tuple[bool, str, str]] = {
    "contains": (False, "%", "%"),
    "icontains": (True, "%", "%"),
    "startswith": (False, "", "%"),
    "istartswith": (True, "", "%"),
    "endswith": (False, "%", ""),
    "iendswith": (True, "%", ""),
}


class F:
    """Field reference for use in updates and filters.

    Generates SQL expressions referencing database columns, supporting
    arithmetic composition::

        await task.update(priority=F("priority") + 1)
        await Order.update_where(
            {"total": F("price") * F("quantity")}, status="pending"
        )

    Unlike :class:`RawSQL`, F-expressions compose safely: referenced columns
    must be model fields (checked when the query is built) and the other
    operand must be another ``F`` or a finite ``int``/``float``.
    """

    __slots__ = ("_expr", "_refs")

    def __init__(self, column: str) -> None:
        object.__setattr__(self, "_expr", column)
        object.__setattr__(self, "_refs", (column,))

    @classmethod
    def _make(cls, expr: str, refs: tuple[str, ...]) -> F:
        result = cls.__new__(cls)
        object.__setattr__(result, "_expr", expr)
        object.__setattr__(result, "_refs", refs)
        return result

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("F expressions are immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError("F expressions are immutable")

    def to_sql(self) -> str:
        return self._expr

    def __add__(self, other: F | int | float) -> F:
        return self._binop("+", other)

    def __radd__(self, other: int | float) -> F:
        return self._rbinop("+", other)

    def __sub__(self, other: F | int | float) -> F:
        return self._binop("-", other)

    def __rsub__(self, other: int | float) -> F:
        return self._rbinop("-", other)

    def __mul__(self, other: F | int | float) -> F:
        return self._binop("*", other)

    def __rmul__(self, other: int | float) -> F:
        return self._rbinop("*", other)

    def __truediv__(self, other: F | int | float) -> F:
        return self._binop("/", other)

    def __rtruediv__(self, other: int | float) -> F:
        return self._rbinop("/", other)

    def __mod__(self, other: F | int | float) -> F:
        return self._binop("%", other)

    def __neg__(self) -> F:
        return F._make(f"-({self._expr})", self._refs)

    def _binop(self, op: str, other: F | int | float) -> F:
        if isinstance(other, F):
            return F._make(
                f"({self._expr} {op} {other._expr})", self._refs + other._refs
            )
        return F._make(f"({self._expr} {op} {_f_literal(other)})", self._refs)

    def _rbinop(self, op: str, other: int | float) -> F:
        return F._make(f"({_f_literal(other)} {op} {self._expr})", self._refs)

    def __repr__(self) -> str:
        return f"F({self._expr!r})"


def _f_literal(value: object) -> str:
    """Render a finite ``int``/``float`` as a SQL literal; reject anything else."""
    kind = type(value)
    if kind is not int and kind is not float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"F() operands must be F or int/float, got {value!r}")
        # Drop subclass __repr__ overrides (e.g. IntEnum).
        return _f_literal(int(value) if isinstance(value, int) else float(value))
    if kind is float and not math.isfinite(cast("float", value)):
        raise TypeError(f"F() operands must be finite, got {value!r}")
    text = f"{value}" if kind is int else repr(value)
    return f"({text})" if text[0] == "-" else text


class RawSQL:
    """Sentinel wrapping a raw SQL expression for use in ``update()``.

    Example::

        await task.update(updated_at=RawSQL("NOW()"))
        await comment.update(resolved=RawSQL("NOT resolved"))

    Supports optional bound parameters via ``{}`` placeholders::

        await media.update(latitude=RawSQL("COALESCE({}, latitude)", 5.0))

    The ``{}`` tokens are replaced with dialect-appropriate positional
    placeholders (``$N``, ``%s``, ``?``) at SQL-build time and the values
    are merged into the query parameter list.  The expression template
    itself is still trusted input — parameterization only makes the bound
    VALUES safe from injection.
    """

    __slots__ = ("expr", "params")

    def __init__(self, expr: str, *params: Any) -> None:
        expected = expr.count("{}")
        if expected != len(params):
            raise ValueError(
                f"RawSQL placeholder count ({expected}) != param count ({len(params)})"
            )
        self.expr = expr
        self.params: tuple[Any, ...] = params

    def param_count(self) -> int:
        return len(self.params)

    def to_sql(self, start_idx: int, placeholder_style: str) -> tuple[str, int]:
        """Render the expression with dialect placeholders.

        Returns ``(rendered_sql, next_idx)``."""
        if not self.params:
            return self.expr, start_idx
        result = self.expr
        idx = start_idx
        for _ in self.params:
            if placeholder_style == "$":
                result = result.replace("{}", f"${idx}", 1)
            else:
                result = result.replace("{}", placeholder_style, 1)
            idx += 1
        return result, idx

    def __repr__(self) -> str:
        if self.params:
            return f"RawSQL({self.expr!r}, {', '.join(map(repr, self.params))})"
        return f"RawSQL({self.expr!r})"


class Q:
    """Composable filter condition supporting OR and AND.

    Example::

        await Annotation.filter(
            Q(annotation_id='') | Q(annotation_id__isnull=True),
            media_id=1,
            frame_number__isnull=True,
        )

    Combine with ``|`` (OR) and ``&`` (AND).  Nesting is supported::

        Q(Q(a=1) | Q(b=2)) & Q(c=3)  # (a=1 OR b=2) AND c=3
    """

    OR = "OR"
    AND = "AND"

    __slots__ = ("children", "_connector", "kwargs")

    def __init__(self, *args: Q, **kwargs: Any) -> None:
        self.children: list[Q] = list(args)
        self.kwargs: dict[str, Any] = kwargs
        self._connector: str = self.AND

    @property
    def connector(self) -> str:
        return self._connector

    def __or__(self, other: Q) -> Q:
        node = Q()
        node._connector = self.OR
        node.children = [self, other]
        return node

    def __and__(self, other: Q) -> Q:
        node = Q()
        node._connector = self.AND
        node.children = [self, other]
        return node

    def __repr__(self) -> str:
        parts = []
        if self.kwargs:
            parts.append(", ".join(f"{k}={v!r}" for k, v in self.kwargs.items()))
        if self.children:
            parts.append(f" {self.connector} ".join(repr(c) for c in self.children))
        return f"Q({', '.join(parts)})"


# ── Aggregate Expressions ──────────────────────────────────────────────────────


class Aggregate:
    """Base class for SQL aggregate expressions."""

    __slots__ = ("column", "distinct")
    function: str = ""

    def __init__(self, column: str = "*", *, distinct: bool = False) -> None:
        self.column = column
        self.distinct = distinct

    def to_sql(self) -> str:
        col = f"DISTINCT {self.column}" if self.distinct else self.column
        return f"{self.function}({col})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.column!r})"


class Count(Aggregate):
    function = "COUNT"


class Sum(Aggregate):
    function = "SUM"


class Avg(Aggregate):
    function = "AVG"


class Min(Aggregate):
    function = "MIN"


class Max(Aggregate):
    function = "MAX"


# ── Many-to-Many ──────────────────────────────────────────────────────────────


def _singularize(table_name: str) -> str:
    """Naive singularization for FK column derivation.

    Handles: categories→category, posts→post, classes→class, boxes→box.
    """
    if table_name.endswith("ies"):
        return table_name[:-3] + "y"
    if (
        table_name.endswith("sses")
        or table_name.endswith("shes")
        or table_name.endswith("ches")
    ):
        return table_name[:-2]
    if table_name.endswith("xes"):
        return table_name[:-2]
    if table_name.endswith("s"):
        return table_name[:-1]
    return table_name


class ManyToMany:
    """Descriptor for many-to-many relationships via a through (join) table.

    Define on a model class to declare a M2M relation::

        class Article(AUniqORMBase):
            id: int = PrimaryKey()
            title: str = Column(nullable=False)
            tags = ManyToMany("tags", through="article_tags")

    The through table must have ForeignKey columns named ``<source>_id``
    and ``<target>_id`` (derived from the table names of the two models).

    Instance access provides ``.add()``, ``.remove()``, and ``.all()``
    via the returned :class:`ManyToManyManager`.
    """

    __slots__ = (
        "_target_table",
        "_through_table",
        "_attr_name",
        "_source_fk",
        "_target_fk",
    )

    def __init__(
        self,
        target: str,
        *,
        through: str,
        source_fk: str | None = None,
        target_fk: str | None = None,
    ) -> None:
        self._target_table = validate_identifier(target, "target table", 2)
        self._through_table = validate_identifier(through, "through table", 2)
        self._source_fk = source_fk and validate_identifier(source_fk, "source_fk")
        self._target_fk = target_fk and validate_identifier(target_fk, "target_fk")
        self._attr_name: str | None = None

    def __set_name__(self, owner: type, name: str) -> None:
        self._attr_name = name

    def __get__(self, obj: Any, objtype: type | None = None) -> Any:
        if obj is None:
            # Class-level access returns the descriptor itself
            return self
        # Instance-level access returns a manager bound to this instance
        return ManyToManyManager(obj, self)

    @property
    def target_table(self) -> str:
        return self._target_table

    @property
    def through_table(self) -> str:
        return self._through_table

    def _resolve(
        self, source_model: type[AUniqORMBase]
    ) -> tuple[type[AUniqORMBase], str, str]:
        """Resolve to (TargetModel, source_fk_col, target_fk_col)."""
        target_model = ModelRegistry.get_model(self._target_table)
        if target_model is None:
            raise ValueError(
                f"ManyToMany target table '{self._target_table}' not found in registry"
            )

        if self._source_fk:
            source_fk = self._source_fk
        else:
            source_fk = f"{_singularize(source_model.__table_name__)}_id"

        if self._target_fk:
            target_fk = self._target_fk
        else:
            target_fk = f"{_singularize(target_model.__table_name__)}_id"

        return target_model, source_fk, target_fk

    async def prefetch(self, instances: list[AUniqORMBase]) -> None:
        """Batch-load M2M related objects for a list of instances."""
        if not instances:
            return

        source_model = type(instances[0])
        target_model, source_fk, target_fk = self._resolve(source_model)
        db = source_model.__database__
        if db is None:
            raise RuntimeError("Model not bound to a database.")

        # Find source PK field
        src_pk: str | None = None
        for name, meta in zip(
            source_model.__struct_fields__, source_model.__struct_defaults__
        ):
            if isinstance(meta, Column) and meta.primary_key:
                src_pk = name
                break
        if src_pk is None:
            raise ValueError("Source model has no primary key")

        # Collect PKs
        pk_values = [getattr(inst, src_pk) for inst in instances]
        if not pk_values:
            return

        dialect = db.protocol
        attr_name = self._attr_name or self._target_table

        # Query through table
        if dialect == "postgresql":
            through_sql = (
                f"SELECT {source_fk}, {target_fk} "
                f"FROM {self._through_table} "
                f"WHERE {source_fk} = ANY($1)"
            )
            through_rows = await db.fetch_all(through_sql, [pk_values])
        else:
            ph = "%s" if dialect == "mysql" else "?"
            placeholders = ", ".join(ph for _ in pk_values)
            through_sql = (
                f"SELECT {source_fk}, {target_fk} "
                f"FROM {self._through_table} "
                f"WHERE {source_fk} IN ({placeholders})"
            )
            through_rows = await db.fetch_all(through_sql, pk_values)

        if not through_rows:
            for inst in instances:
                inst.__dict__[attr_name] = []
            return

        # Collect target IDs
        target_ids = list({row[target_fk] for row in through_rows})

        # Find target PK
        tgt_pk: str | None = None
        for name, meta in zip(
            target_model.__struct_fields__, target_model.__struct_defaults__
        ):
            if isinstance(meta, Column) and meta.primary_key:
                tgt_pk = name
                break
        if tgt_pk is None:
            raise ValueError("Target model has no primary key")

        # Fetch target objects
        tgt_cond: dict[str, Any] = {f"{tgt_pk}__in": target_ids}
        targets = cast("list[AUniqORMBase]", await target_model.filter(**tgt_cond))
        target_map = {getattr(t, tgt_pk): t for t in targets}

        # Group by source PK
        grouped: dict[Any, list[AUniqORMBase]] = {}
        for row in through_rows:
            src_id = row[source_fk]
            tgt_id = row[target_fk]
            target_obj = target_map.get(tgt_id)
            if target_obj is not None:
                grouped.setdefault(src_id, []).append(target_obj)

        for inst in instances:
            pk_val = getattr(inst, src_pk)
            inst.__dict__[attr_name] = grouped.get(pk_val, [])


class ManyToManyManager:
    """Provides add/remove/all for a M2M relation on a specific instance."""

    __slots__ = ("_instance", "_descriptor")

    def __init__(self, instance: AUniqORMBase, descriptor: ManyToMany) -> None:
        self._instance = instance
        self._descriptor = descriptor

    async def all(self) -> list[AUniqORMBase]:
        """Fetch all related objects through the join table."""
        source_model = type(self._instance)
        target_model, source_fk, target_fk = self._descriptor._resolve(source_model)
        db = source_model.__database__
        if db is None:
            raise RuntimeError("Model not bound to a database.")

        # Get source PK value
        src_pk_val = self._get_source_pk()
        dialect = db.protocol

        # Find target PK
        tgt_pk: str | None = None
        for name, meta in zip(
            target_model.__struct_fields__, target_model.__struct_defaults__
        ):
            if isinstance(meta, Column) and meta.primary_key:
                tgt_pk = name
                break
        if tgt_pk is None:
            raise ValueError("Target model has no primary key")

        # Query through table for target IDs
        if dialect == "postgresql":
            sql = (
                f"SELECT {target_fk} FROM {self._descriptor._through_table} "
                f"WHERE {source_fk} = $1"
            )
            rows = await db.fetch_all(sql, [src_pk_val])
        else:
            ph = "%s" if dialect == "mysql" else "?"
            sql = (
                f"SELECT {target_fk} FROM {self._descriptor._through_table} "
                f"WHERE {source_fk} = {ph}"
            )
            rows = await db.fetch_all(sql, [src_pk_val])

        if not rows:
            return []

        target_ids = [row[target_fk] for row in rows]
        tgt_cond: dict[str, Any] = {f"{tgt_pk}__in": target_ids}
        return cast("list[AUniqORMBase]", await target_model.filter(**tgt_cond))

    async def add(self, *targets: AUniqORMBase) -> None:
        """Add one or more objects to the M2M relation."""
        source_model = type(self._instance)
        _, source_fk, target_fk = self._descriptor._resolve(source_model)
        db = source_model.__database__
        if db is None:
            raise RuntimeError("Model not bound to a database.")

        src_pk_val = self._get_source_pk()
        dialect = db.protocol

        # Find target PK field
        tgt_pk = self._get_target_pk(targets[0])

        for target in targets:
            tgt_pk_val = getattr(target, tgt_pk)
            if dialect == "postgresql":
                sql = (
                    f"INSERT INTO {self._descriptor._through_table} "
                    f"({source_fk}, {target_fk}) VALUES ($1, $2) "
                    f"ON CONFLICT DO NOTHING"
                )
            elif dialect == "mysql":
                sql = (
                    f"INSERT IGNORE INTO {self._descriptor._through_table} "
                    f"({source_fk}, {target_fk}) VALUES (%s, %s)"
                )
            else:
                sql = (
                    f"INSERT OR IGNORE INTO {self._descriptor._through_table} "
                    f"({source_fk}, {target_fk}) VALUES (?, ?)"
                )
            await db.execute(sql, [src_pk_val, tgt_pk_val])

    async def remove(self, *targets: AUniqORMBase) -> None:
        """Remove one or more objects from the M2M relation."""
        source_model = type(self._instance)
        _, source_fk, target_fk = self._descriptor._resolve(source_model)
        db = source_model.__database__
        if db is None:
            raise RuntimeError("Model not bound to a database.")

        src_pk_val = self._get_source_pk()
        dialect = db.protocol
        tgt_pk = self._get_target_pk(targets[0])

        for target in targets:
            tgt_pk_val = getattr(target, tgt_pk)
            if dialect == "postgresql":
                sql = (
                    f"DELETE FROM {self._descriptor._through_table} "
                    f"WHERE {source_fk} = $1 AND {target_fk} = $2"
                )
            else:
                ph = "%s" if dialect == "mysql" else "?"
                sql = (
                    f"DELETE FROM {self._descriptor._through_table} "
                    f"WHERE {source_fk} = {ph} AND {target_fk} = {ph}"
                )
            await db.execute(sql, [src_pk_val, tgt_pk_val])

    async def clear(self) -> None:
        """Remove all M2M associations for this instance."""
        source_model = type(self._instance)
        _, source_fk, _ = self._descriptor._resolve(source_model)
        db = source_model.__database__
        if db is None:
            raise RuntimeError("Model not bound to a database.")

        src_pk_val = self._get_source_pk()
        dialect = db.protocol

        if dialect == "postgresql":
            sql = (
                f"DELETE FROM {self._descriptor._through_table} WHERE {source_fk} = $1"
            )
        else:
            ph = "%s" if dialect == "mysql" else "?"
            sql = (
                f"DELETE FROM {self._descriptor._through_table} "
                f"WHERE {source_fk} = {ph}"
            )
        await db.execute(sql, [src_pk_val])

    def _get_source_pk(self) -> Any:
        source_model = type(self._instance)
        for name, meta in zip(
            source_model.__struct_fields__, source_model.__struct_defaults__
        ):
            if isinstance(meta, Column) and meta.primary_key:
                val = getattr(self._instance, name)
                if val is None or isinstance(val, Column):
                    raise ValueError("Source instance has no PK value")
                return val
        raise ValueError("Source model has no primary key")

    def _get_target_pk(self, target: AUniqORMBase) -> str:
        target_model = type(target)
        for name, meta in zip(
            target_model.__struct_fields__, target_model.__struct_defaults__
        ):
            if isinstance(meta, Column) and meta.primary_key:
                return name
        raise ValueError("Target model has no primary key")


class ModelRegistry:
    """Registry tracking all AUniqORMBase subclasses."""

    _models: ClassVar[dict[str, type[AUniqORMBase]]] = {}
    _database: ClassVar[weakref.ref[Database] | None] = None
    _database_context: ClassVar[ContextVar[tuple[Database, ...]]] = ContextVar(
        "auniqorm_database_context", default=()
    )

    @classmethod
    def register(cls, model: type[AUniqORMBase]) -> None:
        """Register a model class."""
        table_name = model.__table_name__
        if table_name in cls._models:
            owner_name = cls._models[table_name].__name__
            raise ValueError(f"Table '{table_name}' already registered by {owner_name}")
        cls._models[table_name] = model
        # If a database is already bound, attach it to the new model
        db = cls.get_database()
        if db is not None:
            model.__database__ = db

    @classmethod
    def unregister(cls, model: type[AUniqORMBase]) -> None:
        """Unregister a model class."""
        table_name = model.__table_name__
        cls._models.pop(table_name, None)

    @classmethod
    def get_models(cls) -> dict[str, type[AUniqORMBase]]:
        """Get all registered models."""
        return cls._models

    @classmethod
    def get_model(cls, table_name: str) -> type[AUniqORMBase] | None:
        """Get a model by table name."""
        return cls._models.get(table_name)

    @classmethod
    def bind(cls, database: Database) -> None:
        """Bind all models to a database instance."""
        cls._database_context.set((*cls._database_context.get(), database))

    @classmethod
    def unbind(cls, database: Database) -> None:
        """Remove the most recent binding for a database in this context."""
        stack = cls._database_context.get()
        for index in range(len(stack) - 1, -1, -1):
            if stack[index] is database:
                cls._database_context.set(stack[:index] + stack[index + 1 :])
                return

    @classmethod
    async def create_all(cls) -> None:
        """Create tables for all registered models."""
        db = cls.get_database()
        if db is None:
            raise RuntimeError("No database bound. Call bind() first.")

        pending = set(cls.get_models().values())
        max_rounds = len(pending) + 1

        for _ in range(max_rounds):
            if not pending:
                break
            failed: dict[type, Exception] = {}
            for model in list(pending):
                try:
                    await model.create_table()
                    pending.discard(model)
                except Exception as e:
                    failed[model] = e
        else:
            names = ", ".join(m.__name__ for m in pending)
            errors = "; ".join(f"{m.__name__}: {e}" for m, e in failed.items())
            raise RuntimeError(f"Could not create tables: {names}. Errors: {errors}")

    @classmethod
    async def drop_all(cls, if_exists: bool = True) -> None:
        """Drop tables for all registered models."""
        db = cls.get_database()
        if db is None:
            raise RuntimeError("No database bound. Call bind() first.")

        for model in cls.get_models().values():
            await model.drop_table(if_exists)

    @classmethod
    def get_database(cls) -> Database | None:
        """Get the bound database."""
        stack = cls._database_context.get()
        return stack[-1] if stack else None

    @classmethod
    def clear(cls) -> None:
        """Clear all registrations (useful for testing)."""
        cls._models.clear()
        cls._database = None
        cls._database_context.set(())
        _update_where_cache.clear()


# Module-level SQL cache for update_where fast path
_update_where_cache: dict[tuple[Any, ...], tuple[str, list[tuple[str, str]]]] = {}


def _acceptable_types(annotation: Any) -> tuple[type, ...] | None:
    """Runtime types acceptable for an annotation, or ``None`` if unknown.

    Walks unions/optionals; for parameterized generics returns the origin
    (``list[int]`` → ``(list,)``). Returns ``None`` for ``Any`` / unresolved
    annotations so the writeback check is a no-op.
    """
    if annotation is None or annotation is type(None):
        return (type(None),)
    if annotation is Any:
        return None
    origin = get_origin(annotation)
    if origin is Union or origin is _stdlib_types.UnionType:
        acc: list[type] = []
        for arg in get_args(annotation):
            sub = _acceptable_types(arg)
            if sub is None:
                return None  # any-of unknown → don't validate
            acc.extend(sub)
        return tuple(acc)
    if origin is not None:
        return (origin,) if isinstance(origin, type) else None
    if isinstance(annotation, type):
        return (annotation,)
    return None


@dataclass_transform(kw_only_default=True)
class AUniqORMBaseMeta(msgspec.StructMeta):
    """Metaclass for AUniqORMBase.

    Handles model registration and field extraction.

    Models are keyword-only (AUniqORMBase sets ``kw_only=True``), so field order
    carries no meaning; saying so here stops checkers reporting a field without
    a default following one that has a default.
    """

    # Instances of this metaclass are model classes, so these are the
    # attributes __new__ assigns below. Declared for type checkers only.
    if TYPE_CHECKING:
        __table_name__: str
        __abstract__: bool
        __database__: Database | None
        __query_fields__: frozenset[str]

    def __setattr__(self, name: str, value: Any) -> None:
        # Table names are interpolated into SQL, so check them on every write.
        if name == "__table_name__" and value != "":
            validate_identifier(value, "table name", 2)
        super().__setattr__(name, value)

    def __getattribute__(self, name: str) -> Any:
        if name == "__database__":
            database = ModelRegistry.get_database()
            if database is not None:
                return database
        return super().__getattribute__(name)

    def __new__(
        mcls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        table: str | None = None,
        abstract: bool = False,
        **kwargs: Any,
    ) -> AUniqORMBaseMeta:
        # Don't process the base class itself
        is_base = name == "AUniqORMBase" and not any(
            isinstance(b, AUniqORMBaseMeta) for b in bases
        )

        # Require Column defaults for annotated fields and validate explicit
        # defaults against the declared field type.
        #
        # NOTE: ``from __future__ import annotations`` makes field_type a
        # *string* at class-creation time, so we must match on the *value*
        # (what the user wrote as the default) rather than the annotation.
        #
        # Python 3.14+ (PEP 649): __annotations__ is no longer in the
        # namespace at metaclass time.  Use annotationlib to retrieve
        # field names; fall back to the dict key for 3.12/3.13.
        if sys.version_info >= (3, 14):
            import annotationlib

            annotate_fn = annotationlib.get_annotate_from_class_namespace(namespace)
            if annotate_fn is not None:
                annotations = annotationlib.call_annotate_function(
                    annotate_fn, annotationlib.Format.STRING
                )
            else:
                annotations = namespace.get("__annotations__", {})
        else:
            annotations = namespace.get("__annotations__", {})

        for field_name in list(annotations):
            # ClassVars and dunders are not columns; rewriting them would
            # replace a model's own class attributes with Column instances.
            ann_str = str(annotations[field_name])
            if field_name.startswith("__") or "ClassVar" in ann_str:
                continue
            try:
                value = namespace[field_name]
            except KeyError:
                # Bare annotation — auto-add Column() with nullable
                # inferred from the type hint.
                nullable = "None" in ann_str or "Optional" in ann_str
                namespace[field_name] = Column(nullable=nullable)
                continue

            if isinstance(value, Column):
                # Already a Column — validate what we can.
                if value.server_default and not isinstance(value.server_default, str):
                    raise TypeError(
                        f"Field '{field_name}' has an invalid server_default; "
                        "must be a string representing a SQL expression"
                    )
            elif callable(value) and not isinstance(value, type):
                # A bare callable (e.g. ``lambda: []``) → default_factory
                namespace[field_name] = Column(default_factory=value)
            elif isinstance(value, type) and issubclass(value, (list, dict, set)):
                # A mutable container type used as factory (e.g. ``list``)
                namespace[field_name] = Column(default_factory=value)
            else:
                # Plain scalar default
                namespace[field_name] = Column(default=value)

        cls = super().__new__(mcls, name, bases, namespace, **kwargs)

        if is_base:
            return cls

        # Names are interpolated into DDL and queries unquoted.
        for field_name in cls.__struct_fields__:
            validate_identifier(field_name, f"column name on model {name}")
        for meta in cls.__struct_defaults__:
            if isinstance(meta, Column) and meta.foreign_key:
                validate_identifier(meta.foreign_key, "foreign key", 3)
        cls.__query_fields__ = frozenset(cls.__struct_fields__)

        # Abstract models: no table, no registration
        cls.__abstract__ = abstract
        if abstract:
            cls.__table_name__ = ""
            cls.__database__ = None
            return cls

        # Set table name
        s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
        s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
        default_table = s.lower() + "s"
        if table and table == default_table:
            logger.warning(
                f"Table name '{table}' is the same as the default"
                f" generated name. Consider omitting the 'table' argument."
            )
        cls.__table_name__ = table or default_table

        cls.__database__ = None

        # Register the model
        ModelRegistry.register(cls)  # type: ignore[arg-type]

        return cls


class AUniqORMBase(
    msgspec.Struct,
    kw_only=True,
    omit_defaults=True,
    repr_omit_defaults=True,
    dict=True,
    metaclass=AUniqORMBaseMeta,
):
    """Create the AUniqORMBase class with msgspec.Struct integration.

    Combines msgspec.Struct for efficient serialization with SQLAlchemy-style
    declarative model definition.

    Example:
        class User(AUniqORMBase):
            id: int = Field(primary_key=True)
            email: str = Field(unique=True, nullable=False)
            name: str
            created_at: datetime | None = None

        # Bind to database and sync schema
        async with Database("postgresql://...") as db:

        # Create instance
        user = User(email="test@example.com", name="Test")

        # Serialize to dict/JSON
        data = msgspec.to_builtins(user)
        json_bytes = msgspec.json.encode(user)

        # Deserialize
        user = msgspec.json.decode(json_bytes, type=User)
    """

    # Injected by AUniqORMBaseMeta at class creation. Declared for type checkers
    # only: schema generation runs get_type_hints() over every model, which
    # would fail to resolve these at runtime.
    if TYPE_CHECKING:
        __table_name__: ClassVar[str]
        __abstract__: ClassVar[bool]
        __database__: ClassVar[Database | None]
        # Set by msgspec.StructMeta / populated lazily by the cache helpers below.
        __struct_defaults__: ClassVar[tuple[Any, ...]]
        __field_types_cache__: ClassVar[dict[str, tuple[type, ...]]]
        __json_fields_cache__: ClassVar[frozenset[str]]
        __msgpack_decoder__: ClassVar[Any]
        __msgpack_decoder_one__: ClassVar[Any]
        __get_sql_cache__: ClassVar[dict[tuple[tuple[str, ...], str], str]]
        __order_by_cache__: ClassVar[dict[Any, str]]
        __query_fields__: ClassVar[frozenset[str]]

    def __getattribute__(self, name: str) -> Any:
        if name == "__database__":
            database = ModelRegistry.get_database()
            if database is not None:
                return database
        return super().__getattribute__(name)

    def __repr__(self) -> str:
        bases = (allbases := self.__class__.mro())[: allbases.index(AUniqORMBase)][::-1]
        annotations = {}
        for base in bases:
            annotations.update(get_type_hints(base))

        base_name = bases[-1].__name__
        suffix = "" if len(bases) == 1 else f"[{bases[-2].__name__}]"
        field_parts = [f"{base_name}{suffix}:"]
        for (field, annotation), default in zip(
            annotations.items(), self.__struct_defaults__
        ):
            value = getattr(self, field)
            if self.__struct_config__.repr_omit_defaults and value is default:
                continue

            type_name = (
                annotation.__name__
                if isinstance(annotation, type)
                else str(annotation).replace("typing.", "")
            )
            field_parts.append(f"{field}: <{type_name}> ={value!r}")
        return "\n  ".join(field_parts)

    # Class methods for database operations

    @classmethod
    def _field_types(cls) -> dict[str, tuple[type, ...]]:
        """Cache of acceptable runtime types per field, for writeback validation."""
        cached = cls.__dict__.get("__field_types_cache__")
        if cached is not None:
            return cast("dict[str, tuple[type, ...]]", cached)
        try:
            hints = get_type_hints(cls)
        except Exception:
            hints = {}
        cache: dict[str, tuple[type, ...]] = {}
        for field in cls.__struct_fields__:
            ann = hints.get(field)
            if ann is None:
                continue
            types_tuple = _acceptable_types(ann)
            if types_tuple is not None:
                # Always tolerate Column sentinels (server-default placeholders).
                cache[field] = types_tuple + (Column,)
        cls.__field_types_cache__ = cache
        return cache

    @classmethod
    def _json_fields(cls) -> frozenset[str]:
        """Field names whose declared type is a JSON container (``list``/``dict``).

        These need explicit (de)serialisation on backends without a native JSON
        codec (SQLite, MySQL).  PostgreSQL handles them via asyncpg codecs, so
        values there arrive already decoded and the text guards below are no-ops.
        """
        cached = cls.__dict__.get("__json_fields_cache__")
        if cached is not None:
            return cast("frozenset[str]", cached)
        try:
            hints = get_type_hints(cls)
        except Exception:
            hints = {}
        fields: set[str] = set()
        for field in cls.__struct_fields__:
            ann = hints.get(field)
            if ann is None:
                continue
            inner, _ = unwrap_optional(ann)
            origin = get_origin(inner) or inner
            if origin in (list, dict):
                fields.add(field)
        result = frozenset(fields)
        cls.__json_fields_cache__ = result
        return result

    @staticmethod
    def _json_encode(value: Any) -> str | None:
        """Serialise a JSON container to text (``None`` stays ``None``)."""
        return None if value is None else msgspec.json.encode(value).decode()

    @staticmethod
    def _json_decode(value: Any) -> Any:
        """Decode JSON text back to Python objects; pass non-text through."""
        if isinstance(value, (str, bytes, bytearray)):
            return msgspec.json.decode(value)
        return value

    def _writeback(self, field: str, value: Any) -> None:
        """Set a field from a DB-sourced value, enforcing the declared type."""
        if field in type(self)._json_fields():
            value = self._json_decode(value)
        types_tuple = type(self)._field_types().get(field)
        if types_tuple is not None and not isinstance(value, types_tuple):
            expected = " | ".join(t.__name__ for t in types_tuple if t is not Column)
            raise TypeError(
                f"{type(self).__name__}.{field}: database returned "
                f"{type(value).__name__}={value!r} but field is annotated "
                f"as {expected}. Fix the model annotation to match the "
                f"database type, or register a driver-level codec to coerce."
            )
        setattr(self, field, value)

    @classmethod
    def _get_db(cls) -> tuple[Database, str]:
        """Return (db, dialect) without DDLGenerator overhead."""
        if (db := ModelRegistry.get_database()) is None:
            raise RuntimeError(
                "Model not bound to a database. Call ModelRegistry.bind() first."
            )
        return db, db.protocol

    @classmethod
    def preamble(cls) -> tuple[Database, str, DDLGenerator]:
        """Generate preamble all Base methods."""
        db, dialect = cls._get_db()
        return db, dialect, DDLGenerator(dialect)

    @classmethod
    async def create_table(cls, if_not_exists: bool = True) -> None:
        """Create the table for this model."""
        db, _, ddl = cls.preamble()
        sql = ddl.create_table(cls)

        await db.execute(sql)

        # Sync schema: add any columns present in the model but missing in the db
        await cls._sync_columns(db, ddl)

        # Create indexes for fields marked with index=True
        for column_name, column_meta in zip(
            cls.__struct_fields__, cls.__struct_defaults__
        ):
            if (
                column_meta.index
                and not column_meta.primary_key
                and not column_meta.unique
            ):
                index_sql = ddl.create_index(cls, [column_name])
                try:
                    await db.execute(index_sql)
                except QueryError:
                    # Index may already exist (MySQL has no IF NOT EXISTS)
                    if ddl.dialect != "mysql":
                        raise

    @classmethod
    async def _sync_columns(cls, db: Database, ddl: DDLGenerator) -> None:
        """Add columns that exist in the model but not yet in the database table.

        Runs inside a transaction — if any ALTER fails, all changes roll back
        and the exception is re-raised so the caller can take action.
        """
        sql, params = ddl.introspect_columns_sql(cls.__table_name__)
        rows = await db.fetch_all(sql, params)
        existing = ddl.parse_column_names(rows)

        resolved_hints = get_type_hints(cls)
        pending = [
            (name, resolved_hints[name], meta)
            for name, meta in zip(resolved_hints, cls.__struct_defaults__)
            if name not in existing
        ]

        if not pending:
            return

        try:
            async with await db.transaction():
                for column_name, column_type, column_meta in pending:
                    alter_sql = ddl.alter_add_column(
                        cls.__table_name__, column_name, column_type, column_meta
                    )
                    await db.execute(alter_sql)
            logger.info(
                "Migrated table '%s': added columns %s",
                cls.__table_name__,
                [name for name, _, _ in pending],
            )
        except Exception as exc:
            col_names = [name for name, _, _ in pending]
            logger.warning(
                "Migration of table '%s' rolled back: could not add columns %s. "
                "Consider making new columns nullable or adding a server_default.",
                cls.__table_name__,
                col_names,
                exc_info=True,
            )
            raise SchemaMigrationError(cls.__table_name__, col_names, exc) from exc

    @classmethod
    async def drop_table(cls, if_exists: bool = True) -> None:
        """Drop the table for this model."""
        db, _, ddl = cls.preamble()
        sql = ddl.drop_table(cls, if_exists)

        await db.execute(sql)

    # Instance methods for CRUD operations

    async def insert(
        self,
        *,
        on_conflict: str | None = None,
    ) -> Any:
        """Insert this instance into the database.

        Parameters
        ----------
        on_conflict:
            Raw SQL appended after the VALUES clause.
            Examples:

            * ``"DO NOTHING"``
            * ``"(name) DO UPDATE SET name = EXCLUDED.name"``

            When *on_conflict* is set and the dialect is PostgreSQL, ``ON
            CONFLICT {on_conflict}`` is appended.  For SQLite the equivalent
            ``OR IGNORE`` / ``OR REPLACE`` must be expressed differently so
            only PostgreSQL is currently supported.
        """
        db, dialect, ddl = self.preamble()
        json_fields = type(self)._json_fields()

        # Build column list (excluding auto-increment primary keys)
        columns = []
        values = []
        placeholders = []

        idx = 1
        for column, column_meta in zip(
            self.__struct_fields__, self.__struct_defaults__
        ):
            value = getattr(self, column, None)

            # Skip primary keys with None value (auto-increment)
            if column_meta and column_meta.primary_key and value is column_meta:
                continue

            # Skip fields still holding their Column sentinel (use server default)
            if isinstance(value, Column):
                continue

            columns.append(column)
            # PostgreSQL serialises JSON via asyncpg codecs; other backends have
            # no native codec, so encode JSON containers to text here.
            if dialect != "postgresql" and column in json_fields:
                value = self._json_encode(value)
            values.append(value)

            if dialect == "postgresql":
                placeholders.append(f"${idx}")
                idx += 1
            elif dialect == "mysql":
                placeholders.append("%s")
            else:
                placeholders.append("?")

        columns_sql = ", ".join(columns)
        placeholders_sql = ", ".join(placeholders)

        sql = (
            f"INSERT INTO {self.__table_name__} ({columns_sql}) "
            f"VALUES ({placeholders_sql})"
        )

        if on_conflict and dialect == "postgresql":
            sql += f" ON CONFLICT {on_conflict}"

        if dialect == "postgresql":
            sql += " RETURNING *"
            row = await db.fetch_one(sql, values)
            if row is not None:
                for field in self.__struct_fields__:
                    if field in row:
                        self._writeback(field, row[field])
            return self

        # Detect whether we need server-generated values back.
        has_auto_pk = False
        has_server_defaults = False
        pk_field: str | None = None

        for name, meta in zip(self.__struct_fields__, self.__struct_defaults__):
            if meta.primary_key:
                pk_field = name
                if isinstance(getattr(self, name, None), Column):
                    has_auto_pk = True
            elif isinstance(getattr(self, name, None), Column):
                has_server_defaults = True

        if dialect == "sqlite":
            # SQLite 3.35+ supports RETURNING — single round trip.
            if has_auto_pk or has_server_defaults:
                if has_server_defaults:
                    sql += " RETURNING *"
                    row = await db.fetch_one(sql, values)
                    if row is not None:
                        for field in self.__struct_fields__:
                            if field in row:
                                self._writeback(field, row[field])
                elif pk_field is not None:
                    sql += f" RETURNING {pk_field}"
                    pk_val = await db.fetch_val(sql, values)
                    if pk_val is not None:
                        self._writeback(pk_field, pk_val)
            else:
                await db.execute(sql, values)
            return self

        # MySQL: no RETURNING support — fall back to two queries.
        await db.execute(sql, values)

        if has_auto_pk and not has_server_defaults and pk_field is not None:
            rowid = await db.fetch_val("SELECT LAST_INSERT_ID()")
            if rowid is not None:
                self._writeback(pk_field, rowid)
        elif has_auto_pk or has_server_defaults:
            if has_auto_pk and pk_field is not None:
                row = await db.fetch_one(
                    f"SELECT * FROM {self.__table_name__} "
                    f"WHERE {pk_field} = LAST_INSERT_ID()",
                )
            elif pk_field is not None:
                pk_val = getattr(self, pk_field, None)
                row = await db.fetch_one(
                    f"SELECT * FROM {self.__table_name__} WHERE {pk_field} = %s",
                    [pk_val],
                )
            else:
                row = None

            if row is not None:
                for field in self.__struct_fields__:
                    if field in row:
                        self._writeback(field, row[field])

        return self

    @classmethod
    async def bulk_insert(
        cls,
        instances: list[AUniqORMBase],
        *,
        on_conflict: str | None = None,
    ) -> None:
        """Insert multiple rows in a single round trip.

        Parameters
        ----------
        instances:
            Model instances to insert.  All must be the same model class.
        on_conflict:
            Conflict-handling clause (same syntax as ``insert()``).

        PostgreSQL uses ``SELECT unnest(...)`` for a fixed-size query
        regardless of batch length.  MySQL and SQLite fall back to multi-row
        ``VALUES``.
        """
        if not instances:
            return

        db, dialect, ddl = cls.preamble()
        hints = get_type_hints(cls)

        # Determine insertable columns from the first instance
        columns: list[str] = []
        for col_name, col_meta in zip(cls.__struct_fields__, cls.__struct_defaults__):
            val = getattr(instances[0], col_name, None)
            if col_meta and col_meta.primary_key and val is col_meta:
                continue
            if isinstance(val, Column):
                continue
            columns.append(col_name)

        if dialect == "postgresql":
            # Map each column to its PostgreSQL type for the array cast.
            meta_map = dict(zip(cls.__struct_fields__, cls.__struct_defaults__))

            col_arrays: list[list[Any]] = []
            casts: list[str] = []
            select_exprs: list[str] = []
            for col_name in columns:
                col_meta = meta_map.get(col_name)
                py_type = hints[col_name]
                db_type = python_to_db_type(py_type, col_meta, dialect)
                # asyncpg treats a Python list as (nested) array dimensions, so
                # a JSON(B) value whose payload is a list is corrupted when sent
                # inside a ``jsonb[]`` unnest: empty lists collapse to SQL NULL
                # and non-empty lists lose their wrapper.  Serialise JSON(B)
                # columns to text and cast back to JSON(B) so each value is
                # passed as an opaque scalar.
                if db_type.upper().startswith("JSON"):
                    casts.append(f"${len(casts) + 1}::text[]")
                    select_exprs.append(f"{col_name}::{db_type}")
                    col_arrays.append(
                        [
                            cls._json_encode(getattr(inst, col_name))
                            for inst in instances
                        ]
                    )
                else:
                    casts.append(f"${len(casts) + 1}::{db_type}[]")
                    select_exprs.append(col_name)
                    col_arrays.append([getattr(inst, col_name) for inst in instances])

            unnest_expr = ", ".join(casts)
            cols_sql = ", ".join(columns)
            select_sql = ", ".join(select_exprs)
            alias_sql = ", ".join(columns)
            sql = (
                f"INSERT INTO {cls.__table_name__} ({cols_sql}) "
                f"SELECT {select_sql} FROM unnest({unnest_expr}) AS t({alias_sql})"
            )
            if on_conflict:
                sql += f" ON CONFLICT {on_conflict}"
            await db.execute(sql, col_arrays)
        else:
            # MySQL / SQLite: multi-row VALUES, chunked so the server parses and
            # binds several small statements instead of one huge one (faster, and
            # keeps SQLite under its bound-variable limit). Multiple chunks run in
            # one transaction to stay atomic.
            param_ph = "%s" if dialect == "mysql" else "?"
            json_fields = cls._json_fields()
            cols_sql = ", ".join(columns)
            row_ph = "(" + ", ".join(param_ph for _ in columns) + ")"
            suffix = (
                f" ON DUPLICATE KEY {on_conflict}"
                if on_conflict and dialect == "mysql"
                else ""
            )
            rows_per_chunk = max(1, min(500, 20000 // len(columns)))

            async def _emit(batch: list[AUniqORMBase]) -> None:
                values: list[Any] = []
                for inst in batch:
                    for col_name in columns:
                        val = getattr(inst, col_name)
                        if col_name in json_fields:
                            val = cls._json_encode(val)
                        values.append(val)
                rows_sql = ", ".join(row_ph for _ in batch)
                sql = (
                    f"INSERT INTO {cls.__table_name__} ({cols_sql}) "
                    f"VALUES {rows_sql}{suffix}"
                )
                await db.execute(sql, values)

            driver = getattr(db, "_driver", None)
            if len(instances) <= rows_per_chunk:
                await _emit(instances)
            elif driver is not None and driver.in_transaction:
                for start in range(0, len(instances), rows_per_chunk):
                    await _emit(instances[start : start + rows_per_chunk])
            else:
                async with await db.transaction():
                    for start in range(0, len(instances), rows_per_chunk):
                        await _emit(instances[start : start + rows_per_chunk])

    @classmethod
    async def bulk_update(
        cls,
        instances: list[AUniqORMBase],
        fields: list[str],
    ) -> None:
        """Update multiple rows in a single round trip.

        Parameters
        ----------
        instances:
            Model instances with updated values.  All must be the same model
            class and must have their primary key set.
        fields:
            Column names to update.  Only these fields are written to the
            database; other attributes are ignored.

        PostgreSQL uses ``UPDATE ... FROM unnest(...)`` for a fixed-size query.
        MySQL and SQLite use batched ``CASE WHEN`` expressions.
        """
        if not instances or not fields:
            return

        db, dialect, ddl = cls.preamble()

        # Find primary key field
        pk_field: str | None = None
        for name, meta in zip(cls.__struct_fields__, cls.__struct_defaults__):
            if isinstance(meta, Column) and meta.primary_key:
                pk_field = name
                break

        if pk_field is None:
            raise ValueError("Cannot bulk_update: no primary key defined")
        cls._check_fields(fields, "bulk_update field")

        # Collect PK values and validate
        pk_values = []
        for inst in instances:
            pk_val = getattr(inst, pk_field)
            if pk_val is None or isinstance(pk_val, Column):
                raise ValueError(
                    "Cannot bulk_update: instance missing primary key value"
                )
            pk_values.append(pk_val)

        if dialect == "postgresql":
            hints = get_type_hints(cls)
            meta_map = dict(zip(cls.__struct_fields__, cls.__struct_defaults__))

            # Build arrays: one for PK + one per update field
            pk_array = pk_values
            col_arrays: list[list[Any]] = [
                [getattr(inst, f) for inst in instances] for f in fields
            ]

            # Build type casts
            pk_meta = meta_map.get(pk_field)
            pk_type = python_to_db_type(hints[pk_field], pk_meta, dialect)
            idx = 1
            casts = [f"${idx}::{pk_type}[]"]
            idx += 1

            unnest_cols = ["_pk"]
            for f in fields:
                col_meta = meta_map.get(f)
                py_type = hints[f]
                db_type = python_to_db_type(py_type, col_meta, dialect)
                casts.append(f"${idx}::{db_type}[]")
                unnest_cols.append(f)
                idx += 1

            unnest_expr = ", ".join(casts)
            cols_alias = ", ".join(unnest_cols)
            set_parts = ", ".join(f"{f} = _d.{f}" for f in fields)

            sql = (
                f"UPDATE {cls.__table_name__} SET {set_parts} "
                f"FROM (SELECT * FROM unnest({unnest_expr}) "
                f"AS t({cols_alias})) AS _d "
                f"WHERE {cls.__table_name__}.{pk_field} = _d._pk"
            )
            await db.execute(sql, [pk_array, *col_arrays])
        else:
            # SQLite / MySQL: CASE WHEN pk = ? THEN ? ... END
            placeholder = "?" if dialect == "sqlite" else "%s"
            values: list[Any] = []
            set_parts_list: list[str] = []

            for f in fields:
                case_parts: list[str] = []
                for inst, pk_val in zip(instances, pk_values):
                    case_parts.append(
                        f"WHEN {pk_field} = {placeholder} THEN {placeholder}"
                    )
                    values.append(pk_val)
                    values.append(getattr(inst, f))
                set_parts_list.append(f"{f} = CASE {' '.join(case_parts)} ELSE {f} END")

            # WHERE pk IN (?, ?, ...)
            in_placeholders = ", ".join(placeholder for _ in pk_values)
            values.extend(pk_values)

            set_clause = ", ".join(set_parts_list)
            sql = (
                f"UPDATE {cls.__table_name__} SET {set_clause} "
                f"WHERE {pk_field} IN ({in_placeholders})"
            )
            await db.execute(sql, values)

    # ── Identifier validation ──────────────────────────────────────────────────

    @classmethod
    def _unknown_field(cls, name: object, context: str) -> ValueError:
        return ValueError(f"Invalid {context} {name!r}: not a field of {cls.__name__}")

    @classmethod
    def _check_fields(cls, names: Any, context: str) -> None:
        for name in names:
            if name not in cls.__query_fields__:
                raise cls._unknown_field(name, context)

    @classmethod
    def _validate_assignments(cls, assignments: dict[str, Any]) -> None:
        """Validate SET targets and the columns referenced by F() values."""
        cls._check_fields(assignments, "update field")
        for value in assignments.values():
            if isinstance(value, F):
                cls._check_fields(value._refs, "F() column")

    @classmethod
    def _order_by_sql(
        cls, order_by: str | list[str], aliases: frozenset[str] | None = None
    ) -> str:
        """Validated, memoized `` ORDER BY`` clause (fields or *aliases*)."""
        key = (order_by if isinstance(order_by, str) else tuple(order_by), aliases)
        cache: dict[Any, str] = cls.__dict__.get("__order_by_cache__") or {}
        sql = cache.get(key)
        if sql is None:
            allowed = cls.__query_fields__ | (aliases or frozenset())
            parts = []
            for clause in [order_by] if isinstance(order_by, str) else order_by:
                name = clause.removeprefix("-")
                if name not in allowed:
                    raise cls._unknown_field(clause, "order_by field")
                parts.append(f"{name} {'DESC' if name != clause else 'ASC'}")
            sql = " ORDER BY " + ", ".join(parts)
            if len(cache) >= _CACHE_LIMIT:
                cache.clear()
            cache[key] = sql
            cls.__order_by_cache__ = cache
        return sql

    # ── Lookup expression support ──────────────────────────────────────────────

    # Supported suffixes for kwargs: field__in, field__isnull, field__lt, etc.
    _LOOKUPS = frozenset(
        {
            "in",
            "isnull",
            "lt",
            "lte",
            "gt",
            "gte",
            "ne",
            "contains",
            "icontains",
            "iexact",
            "istartswith",
            "iendswith",
            "startswith",
            "endswith",
            "range",
        }
    )

    @classmethod
    def _build_conditions(
        cls,
        kwargs: dict[str, Any],
        dialect: str,
        *,
        idx_start: int = 1,
    ) -> tuple[list[str], list[Any], int]:
        """Parse kwargs into SQL conditions, handling lookup expressions.

        Keys must be model fields (optionally with a ``__lookup`` suffix).
        Returns (conditions, values, next_idx).
        """
        placeholder_style = (
            "$" if dialect == "postgresql" else ("%s" if dialect == "mysql" else "?")
        )
        fields = cls.__query_fields__
        conditions: list[str] = []
        values: list[Any] = []
        idx = idx_start

        for key, value in kwargs.items():
            # Check for lookup suffix
            parts = key.rsplit("__", 1)
            if len(parts) == 2 and parts[1] in cls._LOOKUPS:
                field, lookup = parts
            else:
                field, lookup = key, "exact"
            if field not in fields:
                raise cls._unknown_field(key, "filter")

            ph = f"${idx}" if placeholder_style == "$" else placeholder_style

            if lookup == "exact":
                conditions.append(f"{field} = {ph}")
                values.append(value)

            elif lookup == "in":
                if not isinstance(value, (list, tuple, set)):
                    raise TypeError(
                        f"Lookup '{key}' requires a list/tuple/set, got "
                        f"{type(value).__name__}"
                    )
                if dialect == "postgresql":
                    conditions.append(f"{field} = ANY({ph})")
                    values.append(list(value))
                else:
                    # SQLite/MySQL: field IN (?, ?, ...) or (%s, %s, ...)
                    placeholders = ", ".join(placeholder_style for _ in value)
                    conditions.append(f"{field} IN ({placeholders})")
                    values.extend(value)
                    continue

            elif lookup == "isnull":
                if value:
                    conditions.append(f"{field} IS NULL")
                else:
                    conditions.append(f"{field} IS NOT NULL")
                continue

            elif lookup in _COMPARISON_OPS:
                conditions.append(f"{field} {_COMPARISON_OPS[lookup]} {ph}")
                values.append(value)

            elif lookup == "iexact":  # equality, not ILIKE: no wildcards
                conditions.append(f"LOWER({field}) = LOWER({ph})")
                values.append(value)

            elif lookup in _LIKE_LOOKUPS:
                insensitive, prefix, suffix = _LIKE_LOOKUPS[lookup]
                # Backslash escape char: MySQL string literals need it doubled.
                escape = " ESCAPE '\\\\'" if dialect == "mysql" else " ESCAPE '\\'"
                if not insensitive:
                    conditions.append(f"{field} LIKE {ph}{escape}")
                elif dialect == "postgresql":
                    conditions.append(f"{field} ILIKE {ph}{escape}")
                else:
                    conditions.append(f"LOWER({field}) LIKE LOWER({ph}){escape}")
                text = str(value).replace("\\", "\\\\")
                text = text.replace("%", "\\%").replace("_", "\\_")
                values.append(f"{prefix}{text}{suffix}")

            elif lookup == "range":
                if not isinstance(value, (list, tuple)) or len(value) != 2:
                    raise TypeError(
                        f"Lookup '{key}' requires a 2-element list/tuple, got "
                        f"{type(value).__name__}"
                    )
                if placeholder_style == "$":
                    conditions.append(f"{field} BETWEEN ${idx} AND ${idx + 1}")
                    idx += 1
                else:
                    conditions.append(
                        f"{field} BETWEEN {placeholder_style} AND {placeholder_style}"
                    )
                values.extend(value)

            if placeholder_style == "$":
                idx += 1

        return conditions, values, idx

    @classmethod
    def _compile_q(
        cls,
        q: Q,
        dialect: str,
        idx_start: int = 1,
    ) -> tuple[str, list[Any], int]:
        """Compile a Q object tree into a SQL fragment with parameters.

        Returns (sql_fragment, values, next_idx).
        """
        parts: list[str] = []
        values: list[Any] = []
        idx = idx_start

        # Compile own kwargs as leaf conditions
        if q.kwargs:
            conds, vals, idx = cls._build_conditions(q.kwargs, dialect, idx_start=idx)
            if len(conds) == 1:
                parts.append(conds[0])
            else:
                parts.append("(" + " AND ".join(conds) + ")")
            values.extend(vals)

        # Compile children recursively
        for child in q.children:
            fragment, child_vals, idx = cls._compile_q(child, dialect, idx)
            parts.append(fragment)
            values.extend(child_vals)

        if not parts:
            return "1=1", values, idx

        if len(parts) == 1:
            return parts[0], values, idx

        if q.connector not in (Q.AND, Q.OR):
            raise ValueError(f"Invalid Q connector {q.connector!r}")
        joined = f" {q.connector} ".join(parts)
        return f"({joined})", values, idx

    @classmethod
    def _build_where(
        cls,
        q_args: tuple[Q, ...],
        kwargs: dict[str, Any],
        dialect: str,
        idx_start: int = 1,
    ) -> tuple[str, list[Any], int]:
        """Build a complete WHERE clause from Q args + kwargs.

        Returns (where_clause, values, next_idx).  Returns empty string
        if no conditions.
        """
        parts: list[str] = []
        values: list[Any] = []
        idx = idx_start

        # Process Q objects
        for q in q_args:
            fragment, q_vals, idx = cls._compile_q(q, dialect, idx)
            parts.append(fragment)
            values.extend(q_vals)

        # Process kwargs
        if kwargs:
            conds, kw_vals, idx = cls._build_conditions(kwargs, dialect, idx_start=idx)
            parts.extend(conds)
            values.extend(kw_vals)

        if not parts:
            return "", values, idx

        return " AND ".join(parts), values, idx

    @classmethod
    async def all(cls) -> list[Self]:
        """Get all records."""
        return cast("list[Self]", await cls.filter())

    @classmethod
    async def iterate(
        cls,
        *args: Q,
        order_by: str | list[str] | None = None,
        batch_size: int = 500,
        **kwargs: Any,
    ) -> AsyncIterator[AUniqORMBase]:
        """Yield model instances one at a time using cursor-based streaming.

        Use this for large result sets to avoid loading everything into memory::

            async for user in User.iterate(score__gt=50, order_by="id"):
                process(user)

        Parameters
        ----------
        *args:
            Optional :class:`Q` objects for complex conditions.
        order_by:
            Column(s) to sort by.
        batch_size:
            Number of rows fetched per database round-trip (default 500).
        **kwargs:
            Equality/lookup filters.
        """
        if cls.__database__ is None:
            raise RuntimeError("Model not bound to a database.")

        dialect = cls.__database__.protocol

        if not args and not kwargs:
            sql = f"SELECT * FROM {cls.__table_name__}"
            values: list[Any] = []
        else:
            where_clause, values, _ = cls._build_where(args, kwargs, dialect)
            sql = f"SELECT * FROM {cls.__table_name__} WHERE {where_clause}"

        if order_by is not None:
            sql += cls._order_by_sql(order_by)

        async for row in cls.__database__.iterate(sql, values, batch_size=batch_size):
            yield cls.from_row(row)

    @classmethod
    async def count(cls, *args: Q, **kwargs: Any) -> int:
        """Count records matching the given filters.

        Example::

            total = await Task.count(project_id=1)
        """
        if cls.__database__ is None:
            raise RuntimeError("Model not bound to a database.")

        dialect = cls.__database__.protocol

        if not args and not kwargs:
            sql = f"SELECT COUNT(*) FROM {cls.__table_name__}"
            values: list[Any] = []
        else:
            where_clause, values, _ = cls._build_where(args, kwargs, dialect)
            sql = f"SELECT COUNT(*) FROM {cls.__table_name__} WHERE {where_clause}"

        result = await cls.__database__.fetch_val(sql, values)
        return result or 0

    @classmethod
    async def exists(cls, *args: Q, **kwargs: Any) -> bool:
        """Check whether at least one record matching the filters exists.

        Example::

            if await Task.exists(project_id=1, status="pending"):
                ...
        """
        if cls.__database__ is None:
            raise RuntimeError("Model not bound to a database.")

        dialect = cls.__database__.protocol

        if not args and not kwargs:
            sql = f"SELECT EXISTS (SELECT 1 FROM {cls.__table_name__})"
            values: list[Any] = []
        else:
            where_clause, values, _ = cls._build_where(args, kwargs, dialect)
            sql = (
                f"SELECT EXISTS (SELECT 1 FROM {cls.__table_name__}"
                f" WHERE {where_clause})"
            )

        return bool(await cls.__database__.fetch_val(sql, values))

    @classmethod
    async def distinct(
        cls,
        column: str,
        *,
        order_by: str | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Return unique values of *column*.

        Parameters
        ----------
        column:
            The column to select distinct values from.  Must be a valid
            field on the model.
        order_by:
            Sort direction.  Defaults to ``column`` ascending.  Prefix
            with ``"-"`` for descending.
        **kwargs:
            Equality filters, e.g. ``media_id=1``.

        Returns a flat ``list`` of scalar values.

        Example::

            labels = await Annotation.distinct("label")
            types  = await Annotation.distinct(
                "annotation_type", order_by="annotation_type",
            )
        """
        if cls.__database__ is None:
            raise RuntimeError("Model not bound to a database.")

        if column not in cls.__struct_fields__:
            raise ValueError(f"'{column}' is not a field on {cls.__name__}")

        dialect = cls.__database__.protocol

        sql = f"SELECT DISTINCT {column} FROM {cls.__table_name__}"
        values: list[Any] = []

        if kwargs:
            conditions, values, _ = cls._build_conditions(kwargs, dialect)
            sql += " WHERE " + " AND ".join(conditions)

        # Default ordering: by the selected column ascending
        sql += cls._order_by_sql(order_by if order_by is not None else column)

        rows = await cls.__database__.fetch_all(sql, values)
        return [row[column] for row in rows]

    async def update(self, **kwargs: Any) -> None:
        """Update this instance with new values.

        Accepts plain values (parameterised) or :class:`RawSQL` for server-side
        expressions::

            await task.update(status="done", updated_at=RawSQL("NOW()"))
        """
        if self.__database__ is None:
            raise RuntimeError("Model not bound to a database.")

        # Find primary key
        pk_field = None
        pk_value = None
        for name, meta in zip(self.__struct_fields__, self.__struct_defaults__):
            if isinstance(meta, Column) and meta.primary_key:
                pk_field = name
                pk_value = getattr(self, name)
                break

        if pk_field is None or pk_value is None:
            raise ValueError("Cannot update: no primary key defined or value is None")
        type(self)._validate_assignments(kwargs)

        dialect = self.__database__.protocol
        placeholder_style = (
            "$" if dialect == "postgresql" else ("%s" if dialect == "mysql" else "?")
        )

        set_parts = []
        values: list[Any] = []
        idx = 1
        raw_fields: list[str] = []

        for field, value in kwargs.items():
            if isinstance(value, RawSQL):
                if value.params:
                    rendered, idx = value.to_sql(idx, placeholder_style)
                    set_parts.append(f"{field} = {rendered}")
                    values.extend(value.params)
                else:
                    set_parts.append(f"{field} = {value.expr}")
                raw_fields.append(field)
            elif isinstance(value, F):
                set_parts.append(f"{field} = {value.to_sql()}")
                raw_fields.append(field)
            else:
                if placeholder_style == "$":
                    set_parts.append(f"{field} = ${idx}")
                    idx += 1
                else:
                    set_parts.append(f"{field} = {placeholder_style}")
                values.append(value)
                # Update local instance
                setattr(self, field, value)

        set_clause = ", ".join(set_parts)

        if placeholder_style == "$":
            where_clause = f"{pk_field} = ${idx}"
        else:
            where_clause = f"{pk_field} = {placeholder_style}"
        values.append(pk_value)

        sql = f"UPDATE {self.__table_name__} SET {set_clause} WHERE {where_clause}"
        await self.__database__.execute(sql, values)

        # Re-fetch to sync local state for RawSQL/F fields
        if raw_fields:
            refreshed = await self.get(**{pk_field: pk_value})
            if refreshed is not None:
                for field in raw_fields:
                    self._writeback(field, getattr(refreshed, field))

    async def sync(self) -> None:
        """Synchronise this instance with the database.

        Behaviour depends on primary key state:

        - **No PK value** (or PK still holds the Column sentinel): INSERT the
          instance and populate all server-generated fields (auto PK, defaults).
        - **Has PK value**: UPDATE all mutable fields to the database, then
          re-fetch the row to pick up any server-side changes (triggers,
          computed columns, etc.).

        This is the unified equivalent of ``save()`` + ``refresh_from_db()``.
        """
        if self.__database__ is None:
            raise RuntimeError("Model not bound to a database.")

        # Determine PK field and whether it has a real value
        pk_field: str | None = None
        pk_value: Any = None
        for name, meta in zip(self.__struct_fields__, self.__struct_defaults__):
            if isinstance(meta, Column) and meta.primary_key:
                pk_field = name
                raw = getattr(self, name)
                if not isinstance(raw, Column):
                    pk_value = raw
                break

        if pk_field is None:
            raise ValueError("Cannot sync: no primary key defined")

        if pk_value is None:
            # No PK → INSERT (delegates to .insert() which handles RETURNING)
            await self.insert()
            return

        # Has PK → UPDATE all non-PK fields, then re-fetch
        dialect = self.__database__.protocol
        placeholder_style = (
            "$" if dialect == "postgresql" else ("%s" if dialect == "mysql" else "?")
        )

        set_parts: list[str] = []
        values: list[Any] = []
        idx = 1

        for field_name, meta in zip(self.__struct_fields__, self.__struct_defaults__):
            if isinstance(meta, Column) and meta.primary_key:
                continue
            value = getattr(self, field_name)
            if isinstance(value, Column):
                continue  # field still at sentinel — skip
            if placeholder_style == "$":
                set_parts.append(f"{field_name} = ${idx}")
                idx += 1
            else:
                set_parts.append(f"{field_name} = {placeholder_style}")
            values.append(value)

        if not set_parts:
            # Nothing to update — just re-fetch
            pass
        else:
            if placeholder_style == "$":
                where = f"{pk_field} = ${idx}"
            else:
                where = f"{pk_field} = {placeholder_style}"
            values.append(pk_value)

            sql = (
                f"UPDATE {self.__table_name__} SET {', '.join(set_parts)} WHERE {where}"
            )
            await self.__database__.execute(sql, values)

        # Re-fetch to sync server-side state
        refreshed = await self.get(**{pk_field: pk_value})
        if refreshed is not None:
            for field_name in self.__struct_fields__:
                self._writeback(field_name, getattr(refreshed, field_name))

    async def delete(self) -> None:
        """Delete this instance from the database."""
        if self.__database__ is None:
            raise RuntimeError("Model not bound to a database.")

        # Find primary key
        pk_field = None
        pk_value = None
        for name, meta in zip(self.__struct_fields__, self.__struct_defaults__):
            if isinstance(meta, Column) and meta.primary_key:
                pk_field = name
                pk_value = getattr(self, name)
                break

        if pk_field is None or pk_value is None:
            raise ValueError("Cannot delete: no primary key defined or value is None")

        dialect = self.__database__.protocol
        placeholder_style = (
            "$" if dialect == "postgresql" else ("%s" if dialect == "mysql" else "?")
        )

        if placeholder_style == "$":
            where_clause = f"{pk_field} = $1"
        else:
            where_clause = f"{pk_field} = {placeholder_style}"

        sql = f"DELETE FROM {self.__table_name__} WHERE {where_clause}"
        await self.__database__.execute(sql, [pk_value])

    @classmethod
    async def delete_where(cls, *args: Q, **kwargs: Any) -> None:
        """Delete all rows matching the given filters.

        Example::

            await Task.delete_where(project_id=1, status="cancelled")
            await CollectionItem.delete_where(
                collection_id=5, media_id__in=[1, 2, 3]
            )
            await Annotation.delete_where(
                Q(annotation_id='') | Q(annotation_id__isnull=True),
                media_id=1,
            )
        """
        if not args and not kwargs:
            raise ValueError("delete_where() requires at least one filter condition")

        db, dialect = cls._get_db()
        where_clause, values, _ = cls._build_where(args, kwargs, dialect)
        sql = f"DELETE FROM {cls.__table_name__} WHERE {where_clause}"
        await db.execute(sql, values)

    @classmethod
    async def update_where(
        cls, values: dict[str, Any], *args: Q, **kwargs: Any
    ) -> None:
        """Update all rows matching the given filters.

        Parameters
        ----------
        values:
            Column-value pairs to SET.  Supports :class:`RawSQL` for
            server-side expressions.
        *args:
            Optional :class:`Q` objects for OR / complex conditions.
        **kwargs:
            Filter conditions (same syntax as ``filter()``).

            Special keyword: ``defaults`` — if provided (a dict), and the
            WHERE clause matches zero rows, a new row is created using
            the filter kwargs merged with *defaults* merged with *values*
            (update-or-create semantics).

        Example::

            await CollectionItem.update_where(
                {"has_updates": False},
                collection_id=5, media_id__in=[1, 2, 3],
            )
            await Task.update_where(
                {"updated_at": RawSQL("NOW()")},
                project_id=1,
            )
            # Update-or-create: update if exists, otherwise insert
            await User.update_where(
                {"last_login": now},
                defaults={"name": "Alice"},
                email="alice@example.com",
            )
        """
        defaults: dict[str, Any] | None = kwargs.pop("defaults", None)

        if not args and not kwargs:
            raise ValueError("update_where() requires at least one filter condition")
        if not values:
            raise ValueError("update_where() requires at least one value to set")

        db, dialect = cls._get_db()

        # Fast-path: no Q objects, all values are F/RawSQL or plain, and
        # all kwargs are simple exact-match (no __lookup suffixes).
        # This avoids _build_where / _build_conditions overhead entirely.
        placeholder_style = (
            "$" if dialect == "postgresql" else ("%s" if dialect == "mysql" else "?")
        )
        if not args and defaults is None:
            # Check SQL cache for this value+filter combination.  F/RawSQL text
            # is inlined into the SQL, so it is part of the key.
            value_type_keys = tuple(
                (f, "r", v.expr, v.param_count())
                if isinstance(v, RawSQL)
                else (f, "f", v._expr)
                if isinstance(v, F)
                else (f, "v")
                for f, v in values.items()
            )
            kwarg_keys = tuple(kwargs.keys())
            cache_key = (cls, value_type_keys, kwarg_keys, dialect)
            cached = _update_where_cache.get(cache_key)
            if cached is not None:
                # Fast: reuse cached SQL, just build params
                sql, param_indices = cached
                params: list[Any] = []
                for src, key in param_indices:
                    if src == "v":
                        params.append(values[key])
                    elif src == "r":
                        params.extend(values[key].params)
                    else:
                        params.append(kwargs[key])
                await db.execute(sql, params)
                return

            # Build SQL and cache it
            cls._validate_assignments(values)
            set_parts: list[str] = []
            params = []
            param_indices_build: list[tuple[str, str]] = []
            idx = 1
            for field, value in values.items():
                if isinstance(value, RawSQL):
                    if value.params:
                        rendered, idx = value.to_sql(idx, placeholder_style)
                        set_parts.append(f"{field} = {rendered}")
                        params.extend(value.params)
                        param_indices_build.append(("r", field))
                    else:
                        set_parts.append(f"{field} = {value.expr}")
                elif isinstance(value, F):
                    set_parts.append(f"{field} = {value.to_sql()}")
                else:
                    if placeholder_style == "$":
                        set_parts.append(f"{field} = ${idx}")
                        idx += 1
                    else:
                        set_parts.append(f"{field} = {placeholder_style}")
                    params.append(value)
                    param_indices_build.append(("v", field))

            # Build simple WHERE conditions inline
            where_parts: list[str] = []
            for key, value in kwargs.items():
                if "__" in key:
                    # Has a lookup suffix — fall through to general path
                    break
                cls._check_fields((key,), "filter")
                if placeholder_style == "$":
                    where_parts.append(f"{key} = ${idx}")
                    idx += 1
                else:
                    where_parts.append(f"{key} = {placeholder_style}")
                params.append(value)
                param_indices_build.append(("k", key))
            else:
                # All kwargs were simple — use the fast path
                set_sql = ", ".join(set_parts)
                where_sql = " AND ".join(where_parts)
                sql = f"UPDATE {cls.__table_name__} SET {set_sql} WHERE {where_sql}"
                if len(_update_where_cache) >= _CACHE_LIMIT:
                    _update_where_cache.clear()
                _update_where_cache[cache_key] = (sql, param_indices_build)
                await db.execute(sql, params)
                return

        # General path
        cls._validate_assignments(values)
        placeholder_style = (
            "$" if dialect == "postgresql" else ("%s" if dialect == "mysql" else "?")
        )

        set_parts = []
        params = []
        idx = 1

        for field, value in values.items():
            if isinstance(value, RawSQL):
                if value.params:
                    rendered, idx = value.to_sql(idx, placeholder_style)
                    set_parts.append(f"{field} = {rendered}")
                    params.extend(value.params)
                else:
                    set_parts.append(f"{field} = {value.expr}")
            elif isinstance(value, F):
                set_parts.append(f"{field} = {value.to_sql()}")
            else:
                if placeholder_style == "$":
                    set_parts.append(f"{field} = ${idx}")
                    idx += 1
                else:
                    set_parts.append(f"{field} = {placeholder_style}")
                params.append(value)

        where_clause, cond_params, _ = cls._build_where(
            args, kwargs, dialect, idx_start=idx
        )
        params.extend(cond_params)

        set_clause = ", ".join(set_parts)
        sql = f"UPDATE {cls.__table_name__} SET {set_clause} WHERE {where_clause}"

        if defaults is not None:
            # Check if row exists first; if not, create it
            check_sql = (
                f"SELECT 1 FROM {cls.__table_name__} WHERE {where_clause} LIMIT 1"
            )
            row = await db.fetch_one(check_sql, cond_params)
            if row is None:
                # Merge: kwargs (lookups) + defaults + plain values
                plain_values = {
                    k: v for k, v in values.items() if not isinstance(v, (RawSQL, F))
                }
                create_data = {**kwargs, **defaults, **plain_values}
                await cls(**create_data).insert()
                return

        await db.execute(sql, params)

    def to_dict(self) -> dict[str, Any]:
        """Convert model to dictionary."""
        return cast("dict[str, Any]", msgspec.to_builtins(self))

    @classmethod
    async def batch_update_where(
        cls,
        values: dict[str, Any],
        params_list: list[list[Any]],
        *,
        filter_fields: list[str],
    ) -> None:
        """Execute the same UPDATE statement with many parameter sets.

        Uses ``executemany`` for efficient pipelining (asyncpg prepares once).

        Parameters
        ----------
        values:
            Column assignments.  May contain :class:`F` or :class:`RawSQL`.
            Plain values are bound as the *first* parameters for each row.
        params_list:
            List of parameter tuples — each entry provides the WHERE-clause
            bind values for one execution.
        filter_fields:
            Column names used in the WHERE clause (one ``= ?`` per field).
        """
        db, dialect = cls._get_db()
        cls._validate_assignments(values)
        cls._check_fields(filter_fields, "filter field")
        placeholder_style = (
            "$" if dialect == "postgresql" else ("%s" if dialect == "mysql" else "?")
        )

        set_parts: list[str] = []
        raw_prefix_params: list[Any] = []
        idx = 1
        for field, value in values.items():
            if isinstance(value, RawSQL):
                if value.params:
                    rendered, idx = value.to_sql(idx, placeholder_style)
                    set_parts.append(f"{field} = {rendered}")
                    raw_prefix_params.extend(value.params)
                else:
                    set_parts.append(f"{field} = {value.expr}")
            elif isinstance(value, F):
                set_parts.append(f"{field} = {value.to_sql()}")
            else:
                if placeholder_style == "$":
                    set_parts.append(f"{field} = ${idx}")
                    idx += 1
                else:
                    set_parts.append(f"{field} = {placeholder_style}")

        where_parts: list[str] = []
        for ff in filter_fields:
            if placeholder_style == "$":
                where_parts.append(f"{ff} = ${idx}")
                idx += 1
            else:
                where_parts.append(f"{ff} = {placeholder_style}")

        sql = (
            f"UPDATE {cls.__table_name__} SET {', '.join(set_parts)} "
            f"WHERE {' AND '.join(where_parts)}"
        )

        # Prepend RawSQL params to each row's param list
        if raw_prefix_params:
            params_list = [raw_prefix_params + row_params for row_params in params_list]
        await db.execute_many(sql, params_list)

    def keys(self) -> tuple[str, ...]:
        """Return field names, enabling ``**instance`` dict unpacking."""
        return self.__struct_fields__

    def __getitem__(self, key: str) -> Any:
        """Return field value by name, enabling ``**instance`` dict unpacking."""
        return getattr(self, key)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create model instance from dictionary."""
        return cls(**data)

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Self:
        """Create model instance from database row."""
        json_fields = cls._json_fields()
        if json_fields:
            row = {
                key: (cls._json_decode(val) if key in json_fields else val)
                for key, val in row.items()
            }
        return cls(**row)

    @classmethod
    def _msgpack_decoder(cls) -> Any:
        """Cached msgspec msgpack decoder producing ``list[cls]``."""
        decoder = cls.__dict__.get("__msgpack_decoder__")
        if decoder is None:
            decoder = msgspec.msgpack.Decoder(list[cls])  # type: ignore[valid-type]
            cls.__msgpack_decoder__ = decoder
        return decoder

    @classmethod
    def _msgpack_decoder_one(cls) -> Any:
        """Cached msgspec msgpack decoder producing a single ``cls``."""
        decoder = cls.__dict__.get("__msgpack_decoder_one__")
        if decoder is None:
            decoder = msgspec.msgpack.Decoder(cls)
            cls.__msgpack_decoder_one__ = decoder
        return decoder

    @classmethod
    async def _fetch_instances(cls, sql: str, values: list[Any]) -> list[Self]:
        """Hydrate instances, using the native msgpack fast path when available."""
        db = cls.__database__
        if db is None:
            raise RuntimeError("Model not bound to a database.")
        driver = getattr(db, "_driver", None)
        use_msgpack = getattr(driver, "SUPPORTS_MSGPACK", False) is True
        if use_msgpack and not cls._json_fields():
            assert driver is not None
            buf = await driver.fetch_all_msgpack(sql, values)
            return cast("list[Self]", cls._msgpack_decoder().decode(buf))
        rows = await db.fetch_all(sql, values)
        return [cls.from_row(row) for row in rows]

    @classmethod
    def _get_sql(cls, kwargs: dict[str, Any], dialect: str) -> tuple[str, list[Any]]:
        """SQL + values for a ``LIMIT 1`` lookup, memoized for plain-key filters."""
        keys = tuple(kwargs.keys())
        if all("__" not in key for key in keys):
            cache = cls.__dict__.get("__get_sql_cache__")
            if cache is None:
                cache = {}
                cls.__get_sql_cache__ = cache
            sql = cache.get((keys, dialect))
            if sql is None:
                conditions, _, _ = cls._build_conditions(kwargs, dialect)
                where = " AND ".join(conditions)
                sql = f"SELECT * FROM {cls.__table_name__} WHERE {where} LIMIT 1"
                cache[(keys, dialect)] = sql
            return sql, list(kwargs.values())
        conditions, values, _ = cls._build_conditions(kwargs, dialect)
        where = " AND ".join(conditions)
        return f"SELECT * FROM {cls.__table_name__} WHERE {where} LIMIT 1", values

    @classmethod
    async def _fetch_instance(cls, sql: str, values: list[Any]) -> Self | None:
        """Hydrate one instance via the native msgpack fast path when available."""
        db = cls.__database__
        if db is None:
            raise RuntimeError("Model not bound to a database.")
        driver = getattr(db, "_driver", None)
        use_msgpack = getattr(driver, "SUPPORTS_MSGPACK", False) is True
        if use_msgpack and not cls._json_fields():
            assert driver is not None
            buf = await driver.fetch_one_msgpack(sql, values)
            return (
                None
                if buf is None
                else cast("Self", cls._msgpack_decoder_one().decode(buf))
            )
        row = await db.fetch_one(sql, values)
        return cls.from_row(row) if row is not None else None

    # ── Relationships ──────────────────────────────────────────────────────────

    @classmethod
    def _resolve_relation(cls, name: str) -> tuple[str, str, type[AUniqORMBase]]:
        """Resolve a relation name to (fk_field, target_table, TargetModel).

        Convention: relation name ``"author"`` maps to field ``author_id``
        which must be a ForeignKey pointing at a registered model.

        Raises ValueError if the relation cannot be resolved.
        """
        fk_field = f"{name}_id"

        # Find the Column metadata for the FK field
        for field_name, meta in zip(cls.__struct_fields__, cls.__struct_defaults__):
            if field_name == fk_field and isinstance(meta, Column) and meta.foreign_key:
                # Parse "table.column" reference
                ref_table, ref_col = meta.foreign_key.rsplit(".", 1)
                target_model = ModelRegistry.get_model(ref_table)
                if target_model is None:
                    raise ValueError(
                        f"Cannot resolve relation '{name}': target table "
                        f"'{ref_table}' not found in registry"
                    )
                return fk_field, ref_col, target_model

        raise ValueError(
            f"Cannot resolve relation '{name}' on {cls.__name__}: "
            f"no ForeignKey field '{fk_field}' found"
        )

    async def fetch_related(
        self, name: str
    ) -> AUniqORMBase | list[AUniqORMBase] | None:
        """Fetch related object(s) by relation name (lazy, one query).

        Supports both forward relations (FK on this model) and reverse
        relations (FK on the related model pointing at this model).

        Example::

            post = await Post.get(id=1)
            author = await post.fetch_related("author")  # forward

            author = await Author.get(id=1)
            posts = await author.fetch_related("posts")  # reverse (returns list)

        The result is cached on the instance; subsequent calls return the
        cached value without a query.
        """
        # Return cached if available
        cached = self.__dict__.get("_related")
        if cached and name in cached:
            return cast("AUniqORMBase | list[AUniqORMBase] | None", cached[name])

        # Try forward relation first
        try:
            fk_field, ref_col, target_model = self.__class__._resolve_relation(name)
            fk_value = getattr(self, fk_field)

            if fk_value is None or isinstance(fk_value, Column):
                result: AUniqORMBase | list[AUniqORMBase] | None = None
            else:
                result = await target_model.get(**{ref_col: fk_value})
        except ValueError:
            # Try reverse relation
            reverse = self.__class__._resolve_reverse_relation(name)
            if reverse is None:
                raise ValueError(
                    f"Cannot resolve relation '{name}' on "
                    f"{self.__class__.__name__}: no forward FK '{name}_id' "
                    f"and no reverse relation found"
                )
            related_model, fk_field_on_related, our_pk = reverse
            pk_value = getattr(self, our_pk)
            rev_cond: dict[str, Any] = {fk_field_on_related: pk_value}
            result = cast("list[AUniqORMBase]", await related_model.filter(**rev_cond))

        # Cache
        if "_related" not in self.__dict__:
            self.__dict__["_related"] = {}
        self.__dict__["_related"][name] = result
        return result

    @classmethod
    def _resolve_reverse_relation(
        cls, name: str
    ) -> tuple[type[AUniqORMBase], str, str] | None:
        """Find a model whose FK points at this model's table.

        Returns (RelatedModel, fk_field_name_on_related, our_pk_field) or None.

        Convention: relation name is the target table name (e.g. "posts"
        maps to the model with table "posts" that has a FK to our table).
        """
        # Find our PK field
        our_pk: str | None = None
        for field_name, meta in zip(cls.__struct_fields__, cls.__struct_defaults__):
            if isinstance(meta, Column) and meta.primary_key:
                our_pk = field_name
                break
        if our_pk is None:
            return None

        # Look for a model with table name == relation name
        target_model = ModelRegistry.get_model(name)
        if target_model is None:
            return None

        # Check if that model has a FK pointing at our table
        for field_name, meta in zip(
            target_model.__struct_fields__, target_model.__struct_defaults__
        ):
            if isinstance(meta, Column) and meta.foreign_key:
                ref_table, _ = meta.foreign_key.rsplit(".", 1)
                if ref_table == cls.__table_name__:
                    return target_model, field_name, our_pk

        return None

    @classmethod
    async def get(
        cls,
        *,
        select_related: list[str] | None = None,
        defaults: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Self | None:
        """Get a single record by column values.

        Parameters
        ----------
        select_related:
            List of relation names to eagerly load via LEFT JOIN.
            Example: ``await Post.get(id=1, select_related=["author"])``
        defaults:
            If provided and no matching row exists, create a new row using
            the filter kwargs merged with *defaults*.  Returns the new
            instance (get-or-create semantics).  The return value is never
            ``None`` when *defaults* is given.
        **kwargs:
            Filter conditions.
        """
        if cls.__database__ is None:
            raise RuntimeError("Model not bound to a database.")

        dialect = cls.__database__.protocol

        if select_related:
            conditions, values, _ = cls._build_conditions(kwargs, dialect)
            where_clause = " AND ".join(conditions)
            result = await cls._get_with_relations(
                where_clause, values, select_related, dialect
            )
            if result is None and defaults is not None:
                return cast("Self", await cls(**{**kwargs, **defaults}).insert())
            return result

        sql, values = cls._get_sql(kwargs, dialect)
        instance = await cls._fetch_instance(sql, values)
        if instance is not None:
            return instance

        if defaults is not None:
            return cast("Self", await cls(**{**kwargs, **defaults}).insert())
        return None

    @classmethod
    async def _get_with_relations(
        cls,
        where_clause: str,
        values: list[Any],
        relations: list[str],
        dialect: str,
    ) -> Self | None:
        """Execute a LEFT JOIN query and return the instance with relations."""
        sql, join_info = cls._build_join_sql(where_clause, relations, dialect)
        sql += " LIMIT 1"

        row = await cls.__database__.fetch_one(sql, values)  # type: ignore[union-attr]
        if row is None:
            return None

        return cls._split_row(row, join_info)

    @classmethod
    async def filter(
        cls,
        *args: Q,
        order_by: str | list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        select_related: list[str] | None = None,
        prefetch_related: list[str] | None = None,
        columns: list[str] | None = None,
        flat: bool = False,
        **kwargs: Any,
    ) -> list[Self] | list[tuple[Any, ...]] | list[Any]:
        """Get multiple records by field values.

        Parameters
        ----------
        *args:
            Optional :class:`Q` objects for OR / complex conditions.
        order_by:
            Column(s) to sort by.  Prefix with ``"-"`` for DESC.
            Example: ``["-priority", "created_at"]``
        limit:
            Maximum number of rows to return.
        offset:
            Number of rows to skip (requires *order_by* for deterministic
            results, but this is not enforced).
        select_related:
            List of relation names to eagerly load via LEFT JOIN.
        prefetch_related:
            List of relation names to batch-load after the main query.
            Uses separate ``IN`` queries — prevents N+1 without JOINs.
            Supports forward FKs, reverse relations, and M2M fields.
        columns:
            If provided, select only these columns and return a list of
            tuples instead of model instances.  With ``flat=True`` and a
            single column, returns a flat list of scalar values.
        flat:
            Only valid with *columns* containing a single column.  Returns
            a flat list of values instead of 1-tuples.
        **kwargs:
            Equality filters, e.g. ``project_id=1``.
        """
        if cls.__database__ is None:
            raise RuntimeError("Model not bound to a database.")

        dialect = cls.__database__.protocol

        if not args and not kwargs:
            where_clause = ""
            values: list[Any] = []
        else:
            where_clause, values, _ = cls._build_where(args, kwargs, dialect)

        if select_related and not columns:
            return await cls._filter_with_relations(
                where_clause,
                values,
                select_related,
                dialect,
                order_by=order_by,
                limit=limit,
                offset=offset,
            )

        if columns:
            cls._check_fields(columns, "column")
            cols_sql = ", ".join(columns)
        else:
            cols_sql = "*"

        if where_clause:
            sql = f"SELECT {cols_sql} FROM {cls.__table_name__} WHERE {where_clause}"
        else:
            sql = f"SELECT {cols_sql} FROM {cls.__table_name__}"

        if order_by is not None:
            sql += cls._order_by_sql(order_by)

        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        if offset is not None:
            sql += f" OFFSET {int(offset)}"

        if columns:
            rows = await cls.__database__.fetch_all(sql, values)
            if flat:
                if len(columns) != 1:
                    raise ValueError("flat=True requires exactly one column")
                key = columns[0]
                return [row[key] for row in rows]
            return [tuple(row[c] for c in columns) for row in rows]

        instances = await cls._fetch_instances(sql, values)

        if prefetch_related and instances:
            await cls._prefetch_related(
                cast("list[AUniqORMBase]", instances), prefetch_related
            )

        return instances

    @classmethod
    async def _filter_with_relations(
        cls,
        where_clause: str,
        values: list[Any],
        relations: list[str],
        dialect: str,
        *,
        order_by: str | list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[Self]:
        """Execute a LEFT JOIN query for filter() with select_related."""
        sql, join_info = cls._build_join_sql(where_clause, relations, dialect)

        if order_by is not None:
            sql += cls._order_by_sql(order_by)

        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        if offset is not None:
            sql += f" OFFSET {int(offset)}"

        rows = await cls.__database__.fetch_all(sql, values)  # type: ignore[union-attr]
        return [cls._split_row(row, join_info) for row in rows]

    @classmethod
    async def _prefetch_related(
        cls,
        instances: list[AUniqORMBase],
        relations: list[str],
    ) -> None:
        """Batch-load related objects and attach them to instances.

        For each relation, collects the relevant FK values from all
        instances, runs a single IN query, and distributes results.
        """
        for rel_name in relations:
            # Check for M2M field first
            m2m = cls._get_m2m_field(rel_name)
            if m2m is not None:
                await m2m.prefetch(instances)
                continue

            # Try forward relation (FK on this model)
            try:
                fk_field, ref_col, target_model = cls._resolve_relation(rel_name)
                # Collect unique FK values
                fk_values = list(
                    {
                        getattr(inst, fk_field)
                        for inst in instances
                        if not isinstance(getattr(inst, fk_field), Column)
                        and getattr(inst, fk_field) is not None
                    }
                )
                if not fk_values:
                    for inst in instances:
                        inst.__dict__[rel_name] = None
                    continue

                # Batch query
                fwd_cond: dict[str, Any] = {f"{ref_col}__in": fk_values}
                related = cast(
                    "list[AUniqORMBase]", await target_model.filter(**fwd_cond)
                )
                # Index by the referenced column
                related_map: dict[Any, AUniqORMBase] = {
                    getattr(r, ref_col): r for r in related
                }
                # Attach to instances
                for inst in instances:
                    fk_val = getattr(inst, fk_field)
                    inst.__dict__[rel_name] = related_map.get(fk_val)
                continue
            except ValueError:
                pass

            # Try reverse relation (FK on the related model)
            reverse = cls._resolve_reverse_relation(rel_name)
            if reverse is not None:
                related_model, fk_field_on_related, our_pk = reverse
                pk_values = list(
                    {
                        getattr(inst, our_pk)
                        for inst in instances
                        if not isinstance(getattr(inst, our_pk), Column)
                        and getattr(inst, our_pk) is not None
                    }
                )
                if not pk_values:
                    for inst in instances:
                        inst.__dict__[rel_name] = []
                    continue

                rev_in_cond: dict[str, Any] = {f"{fk_field_on_related}__in": pk_values}
                related = cast(
                    "list[AUniqORMBase]", await related_model.filter(**rev_in_cond)
                )
                # Group by FK value
                grouped: dict[Any, list[AUniqORMBase]] = {}
                for r in related:
                    key = getattr(r, fk_field_on_related)
                    grouped.setdefault(key, []).append(r)
                for inst in instances:
                    pk_val = getattr(inst, our_pk)
                    inst.__dict__[rel_name] = grouped.get(pk_val, [])
                continue

            raise ValueError(
                f"Cannot resolve relation '{rel_name}' on "
                f"{cls.__name__} for prefetch_related"
            )

    @classmethod
    def _get_m2m_field(cls, name: str) -> ManyToMany | None:
        """Return the ManyToMany descriptor for *name*, or None."""
        desc = cls.__dict__.get(name)
        if isinstance(desc, ManyToMany):
            return desc
        # Check parent classes
        for klass in cls.__mro__:
            desc = klass.__dict__.get(name)
            if isinstance(desc, ManyToMany):
                return desc
        return None

    @classmethod
    def _build_join_sql(
        cls,
        where_clause: str,
        relations: list[str],
        dialect: str,
    ) -> tuple[str, list[tuple[str, str, type[AUniqORMBase]]]]:
        """Build a SELECT with LEFT JOINs for the given relations.

        Returns (sql, join_info) where join_info is a list of
        (relation_name, alias, TargetModel) tuples.
        """
        # Alias the main table
        main_alias = cls.__table_name__[0]
        select_parts = [f"{main_alias}.*"]
        join_parts: list[str] = []
        join_info: list[tuple[str, str, type[AUniqORMBase]]] = []

        for rel_name in relations:
            fk_field, ref_col, target_model = cls._resolve_relation(rel_name)
            alias = f"_r_{rel_name}"

            # Select all columns from the joined table with prefixed aliases
            for field in target_model.__struct_fields__:
                select_parts.append(f"{alias}.{field} AS _rel_{rel_name}__{field}")

            join_parts.append(
                f"LEFT JOIN {target_model.__table_name__} {alias} "
                f"ON {main_alias}.{fk_field} = {alias}.{ref_col}"
            )
            join_info.append((rel_name, alias, target_model))

        select_sql = ", ".join(select_parts)
        sql = f"SELECT {select_sql} FROM {cls.__table_name__} {main_alias}"
        sql += " " + " ".join(join_parts)

        if where_clause:
            # Prefix bare column names with main alias to avoid ambiguity
            import re

            def _prefix_columns(clause: str, alias: str) -> str:
                """Prefix unqualified column names in WHERE with table alias."""
                # Match word at start of condition or after AND/OR, before operator
                return re.sub(
                    r"\b([a-zA-Z_]\w*)\s*(=|!=|<>|<=|>=|<|>|LIKE|IN|IS)",
                    rf"{alias}.\1 \2",
                    clause,
                )

            sql += f" WHERE {_prefix_columns(where_clause, main_alias)}"

        return sql, join_info

    @classmethod
    def _split_row(
        cls,
        row: dict[str, Any],
        join_info: list[tuple[str, str, type[AUniqORMBase]]],
    ) -> Self:
        """Split a joined row into main instance + related instances."""
        # Extract main model fields
        main_data = {}
        for field in cls.__struct_fields__:
            if field in row:
                main_data[field] = row[field]

        instance = cls.from_row(main_data)

        # Extract related model fields
        for rel_name, _, target_model in join_info:
            prefix = f"_rel_{rel_name}__"
            related_data: dict[str, Any] = {}
            all_none = True

            for field in target_model.__struct_fields__:
                key = f"{prefix}{field}"
                if key in row:
                    val = row[key]
                    related_data[field] = val
                    if val is not None:
                        all_none = False

            if all_none:
                related_obj = None
            else:
                related_obj = target_model.from_row(related_data)

            # Store on __dict__ for attribute access
            instance.__dict__[rel_name] = related_obj

        return instance

    # ── Aggregations ───────────────────────────────────────────────────────────

    @classmethod
    def _check_aggregates(cls, aggregates: dict[str, Aggregate]) -> None:
        for alias, agg in aggregates.items():
            validate_identifier(alias, "aggregate alias")
            if agg.column != "*":
                cls._check_fields((agg.column,), "aggregate column")

    @classmethod
    async def aggregate(
        cls,
        *args: Q,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Run aggregate functions over the table, optionally filtered.

        Keyword arguments whose values are :class:`Aggregate` instances
        become ``SELECT func(col) AS alias`` expressions.  All other
        keyword arguments are treated as filter conditions.

        Example::

            result = await Order.aggregate(
                status="completed",
                total=Sum("amount"),
                count=Count("id"),
            )
            # result == {"total": 1250.0, "count": 42}
        """
        if cls.__database__ is None:
            raise RuntimeError("Model not bound to a database.")

        dialect = cls.__database__.protocol

        # Separate aggregates from filter conditions
        aggregates: dict[str, Aggregate] = {}
        filters: dict[str, Any] = {}
        for key, val in kwargs.items():
            if isinstance(val, Aggregate):
                aggregates[key] = val
            else:
                filters[key] = val

        if not aggregates:
            raise ValueError("aggregate() requires at least one Aggregate expression")
        cls._check_aggregates(aggregates)

        # Build SELECT
        select_parts = [
            f"{agg.to_sql()} AS {alias}" for alias, agg in aggregates.items()
        ]
        sql = f"SELECT {', '.join(select_parts)} FROM {cls.__table_name__}"

        # Build WHERE
        values: list[Any] = []
        if args or filters:
            where_clause, values, _ = cls._build_where(args, filters, dialect)
            sql += f" WHERE {where_clause}"

        row = await cls.__database__.fetch_one(sql, values)
        if row is None:
            return {alias: None for alias in aggregates}
        return dict(row)

    @classmethod
    async def values(
        cls,
        *fields: str,
        flat: bool = False,
        group_by: str | list[str] | None = None,
        order_by: str | list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]] | list[Any]:
        """Select specific columns, optionally with GROUP BY.

        Parameters
        ----------
        *fields:
            Column names or :class:`Aggregate` aliases to select.
            Pass ``Aggregate`` instances as keyword arguments.
        flat:
            If True and exactly one field is selected, return a flat list
            of values instead of dicts.
        group_by:
            Column(s) to group by.
        order_by:
            Column(s) to sort by.  Prefix with ``"-"`` for DESC.
        limit:
            Maximum number of rows to return.
        offset:
            Number of rows to skip.
        **kwargs:
            Keyword arguments whose values are :class:`Aggregate` instances
            are used as computed columns.  All others are filter conditions.

        Example::

            # Grouped aggregation
            results = await Order.values(
                "status",
                total=Sum("amount"),
                group_by="status",
                order_by="-total",
            )
            # [{"status": "completed", "total": 5000}, ...]

            # Flat partial select
            names = await User.values("name", flat=True)
            # ["Alice", "Bob", ...]
        """
        if cls.__database__ is None:
            raise RuntimeError("Model not bound to a database.")

        dialect = cls.__database__.protocol

        # Separate aggregates from filters
        aggregates: dict[str, Aggregate] = {}
        filters: dict[str, Any] = {}
        for key, val in kwargs.items():
            if isinstance(val, Aggregate):
                aggregates[key] = val
            else:
                filters[key] = val

        cls._check_fields(fields, "values() field")
        cls._check_aggregates(aggregates)

        # Build SELECT clause
        select_parts: list[str] = list(fields)
        for alias, agg in aggregates.items():
            select_parts.append(f"{agg.to_sql()} AS {alias}")

        if not select_parts:
            raise ValueError("values() requires at least one field or aggregate")

        sql = f"SELECT {', '.join(select_parts)} FROM {cls.__table_name__}"

        # WHERE
        filter_values: list[Any] = []
        if filters:
            where_clause, filter_values, _ = cls._build_where((), filters, dialect)
            sql += f" WHERE {where_clause}"

        # GROUP BY
        if group_by is not None:
            groups = group_by if isinstance(group_by, list) else [group_by]
            cls._check_fields(groups, "group_by field")
            sql += f" GROUP BY {', '.join(groups)}"

        # ORDER BY
        if order_by is not None:
            sql += cls._order_by_sql(order_by, frozenset(aggregates))

        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        if offset is not None:
            sql += f" OFFSET {int(offset)}"

        rows = await cls.__database__.fetch_all(sql, filter_values)

        if flat:
            if len(fields) + len(aggregates) != 1:
                raise ValueError("flat=True requires exactly one field/aggregate")
            key = fields[0] if fields else next(iter(aggregates))
            return [row[key] for row in rows]

        return [dict(row) for row in rows]


# Type alias for type checking
ModelMeta = AUniqORMBase
