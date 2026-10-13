# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from msgspec import Struct

# Db type mapping
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


class Column(Struct, frozen=True):
    """Metadata for a table column."""

    primary_key: bool = False
    nullable: bool = True
    unique: bool = False
    index: bool = False
    autoincrement: bool = False
    default: Any = None  # explicit default value
    default_factory: Callable[[], Any] | None = None  # factory for default value
    server_default: str | None = None  # db expression for default
    db_type: DbType | str | None = None
    max_length: int | None = None
    foreign_key: str | None = None  # "table.column" format
    on_delete: ON_ACTIONS | None = None
    on_update: ON_ACTIONS | None = None
    comment: str | None = None

    def __repr__(self) -> str:
        parts = []
        if self.primary_key:
            parts.append("primary_key=True")
        if not self.nullable:
            parts.append("nullable=False")
        if self.unique:
            parts.append("unique=True")
        if self.foreign_key:
            parts.append(f"foreign_key={self.foreign_key!r}")
        return f"Column({', '.join(parts)})"


class PrimaryKey(Column):
    primary_key: bool = True
    nullable: bool = False


# Common column shortcuts
# def PrimaryKey(**kwargs: Any) -> Any:
#     """Shortcut for primary key column."""
#     return Column(primary_key=True, nullable=False, **kwargs)


def ForeignKey(  # noqa: N802
    reference: str,
    *,
    on_delete: ON_ACTIONS = "CASCADE",
    on_update: ON_ACTIONS | None = None,
    **kwargs: Any,
) -> Any:
    """Shortcut for foreign key column."""
    return Column(
        foreign_key=reference,
        on_delete=on_delete,
        on_update=on_update,
        **kwargs,
    )
