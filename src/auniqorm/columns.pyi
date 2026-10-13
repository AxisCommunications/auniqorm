# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

# ruff: noqa: N802

"""Type stubs for auniqorm.columns.

A column is declared as ``name: str = Column(...)``, so the call has to be
assignable to the field's own type. Column is a msgspec Struct at runtime,
whose synthesized constructor overrides a declared ``__new__``; describing it
here as a plain class lets ``__new__`` report ``Any`` — the same approach
typeshed uses for ``dataclasses.field``. ``isinstance`` checks and annotations
against ``Column`` keep working because it is still a class.
"""

from collections.abc import Callable
from typing import Any, Literal

DbType = Literal[
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "TEXT",
    "VARCHAR",
    "BOOLEAN",
    "REAL",
    "DOUBLE PRECISION",
    "NUMERIC",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "DATE",
    "TIME",
    "JSON",
    "JSONB",
    "UUID",
    "BYTEA",
    "BLOB",
]
ON_ACTIONS = Literal["CASCADE", "SET NULL", "RESTRICT", "NO ACTION"]

class Column:
    primary_key: bool
    nullable: bool
    unique: bool
    index: bool
    autoincrement: bool
    default: Any
    default_factory: Callable[[], Any] | None
    server_default: str | None
    db_type: DbType | str | None
    max_length: int | None
    foreign_key: str | None
    on_delete: ON_ACTIONS | None
    on_update: ON_ACTIONS | None
    comment: str | None

    def __new__(
        cls,
        *,
        primary_key: bool = ...,
        nullable: bool = ...,
        unique: bool = ...,
        index: bool = ...,
        autoincrement: bool = ...,
        default: Any = ...,
        default_factory: Callable[[], Any] | None = ...,
        server_default: str | None = ...,
        db_type: DbType | str | None = ...,
        max_length: int | None = ...,
        foreign_key: str | None = ...,
        on_delete: ON_ACTIONS | None = ...,
        on_update: ON_ACTIONS | None = ...,
        comment: str | None = ...,
    ) -> Any: ...

class PrimaryKey(Column): ...

def ForeignKey(
    reference: str,
    *,
    on_delete: ON_ACTIONS = ...,
    on_update: ON_ACTIONS | None = ...,
    **kwargs: Any,
) -> Any: ...
