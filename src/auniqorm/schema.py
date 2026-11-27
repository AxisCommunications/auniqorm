# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Schema introspection and DDL generation."""

from __future__ import annotations

import re
import uuid
from datetime import date, datetime, time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, get_args, get_origin, get_type_hints

import msgspec

from auniqorm.columns import Column

if TYPE_CHECKING:
    from auniqorm.connection import Database
    from auniqorm.models import ModelMeta

_IDENTIFIER_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def validate_identifier(name: object, kind: str = "identifier", parts: int = 1) -> str:
    """Return *name* if it is 1..*parts* dot-separated plain SQL identifiers.

    Identifiers are interpolated unquoted (quoting would change PostgreSQL case
    folding), so anything else raises ``ValueError``.
    """
    if (
        isinstance(name, str)
        and name.count(".") < parts
        and all(_IDENTIFIER_RE.fullmatch(p) for p in name.split("."))
    ):
        return name
    raise ValueError(f"Invalid {kind} {name!r}: must match [A-Za-z_][A-Za-z0-9_]*")


def quote_identifier(name: object, dialect: str) -> str:
    """Quote *name* as pandas does: backticks on MySQL, double quotes elsewhere."""
    q = "`" if dialect == "mysql" else '"'
    return q + str(name).replace(q, q + q) + q


# Python type to db type mapping
TYPE_MAP: dict[str, dict[type, str]] = {
    "postgresql": {
        int: "INTEGER",
        float: "DOUBLE PRECISION",
        str: "TEXT",
        bool: "BOOLEAN",
        bytes: "BYTEA",
        datetime: "TIMESTAMPTZ",
        date: "DATE",
        time: "TIME",
        Decimal: "NUMERIC",
        uuid.UUID: "UUID",
        dict: "JSONB",
        list: "JSONB",
    },
    "sqlite": {
        int: "INTEGER",
        float: "REAL",
        str: "TEXT",
        bool: "INTEGER",
        bytes: "BLOB",
        datetime: "TEXT",
        date: "TEXT",
        time: "TEXT",
        Decimal: "TEXT",
        uuid.UUID: "TEXT",
        dict: "TEXT",
        list: "TEXT",
    },
    "mysql": {
        int: "INTEGER",
        float: "DOUBLE",
        str: "TEXT",
        bool: "BOOLEAN",
        bytes: "BLOB",
        datetime: "DATETIME",
        date: "DATE",
        time: "TIME",
        Decimal: "DECIMAL",
        uuid.UUID: "CHAR(36)",
        dict: "JSON",
        list: "JSON",
    },
}


def unwrap_optional(tp: type) -> tuple[type, bool]:
    """Unwrap Optional[T] to (T, is_optional)."""
    import types

    origin = get_origin(tp)

    # Python 3.10+: X | Y is types.UnionType (includes Optional[T])
    if origin is types.UnionType:
        args = get_args(tp)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return (non_none[0], True)

    return (tp, False)


def python_to_db_type(
    py_type: type,
    column_meta: Column | None,
    dialect: str = "postgresql",
) -> str:
    """Convert a Python type to db type string."""
    type_map = TYPE_MAP[dialect]

    # If explicit SQL type is provided, use it
    if column_meta and column_meta.db_type:
        return column_meta.db_type

    # Unwrap Optional
    inner_type, _ = unwrap_optional(py_type)

    # Handle str with max_length -> VARCHAR
    if inner_type is str and column_meta and column_meta.max_length:
        return f"VARCHAR({column_meta.max_length})"

    # Handle generic types (list[T], dict[K, V])
    origin = get_origin(inner_type)
    if origin is list or origin is dict:
        return type_map.get(
            origin or list, "JSONB" if dialect == "postgresql" else "TEXT"
        )

    # Direct type lookup with fallback for unknown types
    return type_map.get(inner_type, "TEXT")


class DDLGenerator:
    """Generate DDL statements for database schema operations."""

    def __init__(self, dialect: str = "postgresql") -> None:
        self.dialect = dialect

    def create_table(self, model: type[ModelMeta]) -> str:
        """Generate CREATE TABLE statement for a model."""
        table_name = model.__table_name__
        columns: list[str] = []
        constraints: list[str] = []

        # Use get_type_hints() to resolve string annotations from
        # 'from __future__ import annotations'.
        resolved_hints = get_type_hints(model)

        for column_name, column_meta in zip(resolved_hints, model.__struct_defaults__):
            column_type = resolved_hints[column_name]
            col_def = self._column_definition(column_name, column_type, column_meta)
            columns.append(col_def)

            # Collect constraints
            if column_meta:
                if column_meta.unique and not column_meta.primary_key:
                    constraints.append(f"UNIQUE ({column_name})")
                if column_meta.foreign_key:
                    fk_constraint = self._foreign_key_constraint(
                        column_name, column_meta.foreign_key, column_meta
                    )
                    constraints.append(fk_constraint)

        all_parts = columns + constraints
        columns_sql = ",\n    ".join(all_parts)

        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {columns_sql}\n)"

    def _column_definition(
        self,
        name: str,
        py_type: type,
        column_meta: Column | None,
    ) -> str:
        """Generate column definition."""
        _, is_optional = unwrap_optional(py_type)
        sql_type = python_to_db_type(py_type, column_meta, self.dialect)

        parts = [name, sql_type]

        if column_meta:
            if column_meta.primary_key:
                match self.dialect:
                    case "postgresql":
                        # Use SERIAL for auto-increment in PostgreSQL
                        if sql_type == "INTEGER":
                            parts[1] = "SERIAL"
                        elif sql_type == "BIGINT":
                            parts[1] = "BIGSERIAL"
                        parts.append("PRIMARY KEY")
                    case "sqlite":
                        parts.append("PRIMARY KEY")
                        if sql_type == "INTEGER":
                            parts.append("AUTOINCREMENT")
                    case "mysql":
                        parts.append("PRIMARY KEY AUTO_INCREMENT")
            elif not column_meta.nullable and not is_optional:
                parts.append("NOT NULL")

            if column_meta.server_default:
                parts.append(f"DEFAULT {column_meta.server_default}")
        elif not is_optional:
            parts.append("NOT NULL")

        return " ".join(parts)

    def _foreign_key_constraint(
        self, column: str, ref: str, column_meta: Column
    ) -> str:
        """Generate foreign key constraint."""
        # Convert "table.column" to "table(column)"
        if "." in ref:
            ref_table, ref_column = ref.rsplit(".", 1)
            ref = f"{ref_table}({ref_column})"
        constraint = f"FOREIGN KEY ({column}) REFERENCES {ref}"

        if column_meta.on_delete:
            constraint += f" ON DELETE {column_meta.on_delete}"
        if column_meta.on_update:
            constraint += f" ON UPDATE {column_meta.on_update}"

        return constraint

    def drop_table(self, model: type[ModelMeta], if_exists: bool = True) -> str:
        """Generate DROP TABLE statement."""
        table_name = model.__table_name__
        if_exists_clause = "IF EXISTS " if if_exists else ""
        return f"DROP TABLE {if_exists_clause}{table_name}"

    def create_index(
        self,
        model: type[ModelMeta],
        columns: list[str],
        unique: bool = False,
        name: str | None = None,
    ) -> str:
        """Generate CREATE INDEX statement."""
        table_name = model.__table_name__
        for column in columns:
            validate_identifier(column, "index column")
        if name is not None:
            validate_identifier(name, "index name")
        index_name = name or f"idx_{table_name}_{'_'.join(columns)}"
        unique_clause = "UNIQUE " if unique else ""
        columns_sql = ", ".join(columns)

        # MySQL does not support IF NOT EXISTS for CREATE INDEX
        if_not_exists = "" if self.dialect == "mysql" else "IF NOT EXISTS "

        return (
            f"CREATE {unique_clause}INDEX {if_not_exists}{index_name} "
            f"ON {table_name} ({columns_sql})"
        )

    def introspect_columns_sql(self, table_name: str) -> tuple[str, list[Any]]:
        """Return (sql, params) to query existing column names for a table."""
        match self.dialect:
            case "postgresql":
                return (
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = $1",
                    [table_name],
                )
            case "sqlite":
                # PRAGMA doesn't support parameterised queries
                return (f"PRAGMA table_info({table_name})", [])
            case "mysql":
                return (
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s AND table_schema = DATABASE()",
                    [table_name],
                )
            case _:
                raise ValueError(f"Unsupported dialect: {self.dialect}")

    def parse_column_names(self, rows: list[dict[str, Any]]) -> set[str]:
        """Extract column names from introspection query results."""
        if self.dialect == "sqlite":
            return {row["name"] for row in rows}
        # MySQL/PostgreSQL information_schema — key casing may vary
        for key in ("column_name", "COLUMN_NAME"):
            if rows and key in rows[0]:
                return {row[key] for row in rows}
        return {row["column_name"] for row in rows}

    def alter_add_column(
        self,
        table_name: str,
        column_name: str,
        py_type: type,
        column_meta: Column,
    ) -> str:
        """Generate ALTER TABLE ADD COLUMN statement."""
        col_def = self._column_definition(column_name, py_type, column_meta)
        return f"ALTER TABLE {table_name} ADD COLUMN {col_def}"


# ── Schema Diff ────────────────────────────────────────────────────────────────


class ColumnDiff(msgspec.Struct, frozen=True):
    """A single column-level change detected between model and database."""

    table: str
    column: str
    action: str  # "add", "drop", "type_change"
    sql: str
    detail: str = ""


class TableDiff(msgspec.Struct, frozen=True):
    """A table-level change detected between model registry and database."""

    table: str
    action: str  # "create", "drop"
    sql: str


class SchemaDiffResult(msgspec.Struct):
    """Complete diff between registered models and live database schema."""

    table_ops: list[TableDiff]
    column_ops: list[ColumnDiff]

    @property
    def has_changes(self) -> bool:
        return bool(self.table_ops or self.column_ops)

    def sql_statements(self) -> list[str]:
        """Return all migration SQL statements in execution order."""
        stmts: list[str] = []
        # Creates first, then column ops, then drops last
        stmts.extend(t.sql for t in self.table_ops if t.action == "create")
        stmts.extend(c.sql for c in self.column_ops)
        stmts.extend(t.sql for t in self.table_ops if t.action == "drop")
        return stmts

    def summary(self) -> str:
        """Human-readable summary of changes."""
        lines: list[str] = []
        for t in self.table_ops:
            lines.append(f"  {t.action.upper()} table {t.table}")
        for c in self.column_ops:
            detail = f" ({c.detail})" if c.detail else ""
            lines.append(f"  {c.action.upper()} column {c.table}.{c.column}{detail}")
        if not lines:
            return "No schema changes detected."
        return "Schema changes:\n" + "\n".join(lines)


class SchemaDiff:
    """Compare registered models against the live database schema.

    Usage::

        async with Database("postgresql://...") as db:
            diff = await SchemaDiff(db).generate()
            print(diff.summary())
            for stmt in diff.sql_statements():
                print(stmt)

            # Or apply directly:
            await SchemaDiff(db).apply()
    """

    def __init__(self, db: Database) -> None:
        self._db = db
        self._dialect = db.protocol
        self._ddl = DDLGenerator(self._dialect)

    async def _get_live_tables(self) -> set[str]:
        """Query the database for existing table names."""
        match self._dialect:
            case "postgresql":
                rows = await self._db.fetch_all(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
                )
                return {r["tablename"] for r in rows}
            case "sqlite":
                rows = await self._db.fetch_all(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
                return {r["name"] for r in rows}
            case "mysql":
                rows = await self._db.fetch_all(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = DATABASE()"
                )
                # MySQL key casing varies
                key = "table_name" if rows and "table_name" in rows[0] else "TABLE_NAME"
                return {r[key] for r in rows}
            case _:
                raise ValueError(f"Unsupported dialect: {self._dialect}")

    async def _get_live_columns(self, table_name: str) -> dict[str, dict[str, Any]]:
        """Query column metadata for a single table.

        Returns {column_name: {"type": str, "nullable": bool}}.
        """
        match self._dialect:
            case "postgresql":
                rows = await self._db.fetch_all(
                    "SELECT column_name, data_type, is_nullable "
                    "FROM information_schema.columns "
                    "WHERE table_name = $1",
                    [table_name],
                )
                return {
                    r["column_name"]: {
                        "type": r["data_type"].upper(),
                        "nullable": r["is_nullable"] == "YES",
                    }
                    for r in rows
                }
            case "sqlite":
                rows = await self._db.fetch_all(f"PRAGMA table_info({table_name})")
                return {
                    r["name"]: {
                        "type": (r["type"] or "TEXT").upper(),
                        "nullable": r["notnull"] == 0,
                    }
                    for r in rows
                }
            case "mysql":
                rows = await self._db.fetch_all(
                    "SELECT column_name, data_type, is_nullable "
                    "FROM information_schema.columns "
                    "WHERE table_name = %s AND table_schema = DATABASE()",
                    [table_name],
                )
                key = (
                    "column_name"
                    if rows and "column_name" in rows[0]
                    else "COLUMN_NAME"
                )
                type_key = (
                    "data_type" if rows and "data_type" in rows[0] else "DATA_TYPE"
                )
                null_key = (
                    "is_nullable"
                    if rows and "is_nullable" in rows[0]
                    else "IS_NULLABLE"
                )
                return {
                    r[key]: {
                        "type": r[type_key].upper(),
                        "nullable": r[null_key] == "YES",
                    }
                    for r in rows
                }
            case _:
                raise ValueError(f"Unsupported dialect: {self._dialect}")

    async def generate(self) -> SchemaDiffResult:
        """Compare registered models to the live database and return a diff.

        Detects:
        - Tables that need to be created (model exists, table doesn't)
        - Tables that could be dropped (table exists, no model)
        - Columns to add (field on model, not in table)
        - Columns to drop (column in table, not on model)
        - Column type changes (model type differs from live type)
        """
        from auniqorm.models import ModelRegistry

        models = ModelRegistry.get_models()
        live_tables = await self._get_live_tables()
        model_tables = set(models.keys())

        table_ops: list[TableDiff] = []
        column_ops: list[ColumnDiff] = []

        # Tables to create
        for table_name in sorted(model_tables - live_tables):
            model = models[table_name]
            sql = self._ddl.create_table(model)
            table_ops.append(TableDiff(table=table_name, action="create", sql=sql))

        # Tables to drop (exist in DB but no model) — generate but flag
        for table_name in sorted(live_tables - model_tables):
            sql = f"DROP TABLE {quote_identifier(table_name, self._dialect)}"
            table_ops.append(TableDiff(table=table_name, action="drop", sql=sql))

        # Column-level diffs for tables that exist in both
        for table_name in sorted(model_tables & live_tables):
            model = models[table_name]
            live_cols = await self._get_live_columns(table_name)
            hints = get_type_hints(model)

            model_fields = set(hints.keys())
            live_field_names = set(live_cols.keys())

            # Columns to add
            for col_name in sorted(model_fields - live_field_names):
                meta_map = dict(zip(model.__struct_fields__, model.__struct_defaults__))
                col_meta = meta_map.get(col_name, Column())
                py_type = hints[col_name]
                sql = self._ddl.alter_add_column(
                    table_name,
                    col_name,
                    py_type,
                    col_meta,
                )
                column_ops.append(
                    ColumnDiff(
                        table=table_name,
                        column=col_name,
                        action="add",
                        sql=sql,
                    )
                )

            # Columns to drop
            for col_name in sorted(live_field_names - model_fields):
                quoted = quote_identifier(col_name, self._dialect)
                sql = f"ALTER TABLE {table_name} DROP COLUMN {quoted}"
                column_ops.append(
                    ColumnDiff(
                        table=table_name, column=col_name, action="drop", sql=sql
                    )
                )

            # Type changes (best-effort comparison)
            for col_name in sorted(model_fields & live_field_names):
                meta_map = dict(zip(model.__struct_fields__, model.__struct_defaults__))
                col_meta = meta_map.get(col_name)
                py_type = hints[col_name]
                expected_type = python_to_db_type(py_type, col_meta, self._dialect)
                live_type = live_cols[col_name]["type"]

                if not self._types_compatible(expected_type, live_type):
                    # SQLite doesn't support ALTER COLUMN — skip type changes
                    if self._dialect == "sqlite":
                        detail = (
                            f"expected {expected_type}, got {live_type} "
                            f"(SQLite does not support ALTER COLUMN)"
                        )
                        column_ops.append(
                            ColumnDiff(
                                table=table_name,
                                column=col_name,
                                action="type_change",
                                sql=(
                                    f"-- Cannot alter column type"
                                    f" in SQLite: "
                                    f"{table_name}.{col_name}"
                                    f" {live_type} -> "
                                    f"{expected_type}"
                                ),
                                detail=detail,
                            )
                        )
                    else:
                        sql = (
                            f"ALTER TABLE {table_name} ALTER COLUMN {col_name} "
                            f"TYPE {expected_type}"
                        )
                        detail = f"{live_type} -> {expected_type}"
                        column_ops.append(
                            ColumnDiff(
                                table=table_name,
                                column=col_name,
                                action="type_change",
                                sql=sql,
                                detail=detail,
                            )
                        )

        return SchemaDiffResult(table_ops=table_ops, column_ops=column_ops)

    def _types_compatible(self, model_type: str, live_type: str) -> bool:
        """Check whether two SQL type strings are semantically equivalent."""
        # Normalize both
        m = model_type.upper().strip()
        l = live_type.upper().strip()  # noqa: E741

        if m == l:
            return True

        # Common equivalences
        equivalences: list[set[str]] = [
            {"INTEGER", "INT", "INT4"},
            {"BIGINT", "INT8"},
            {"SMALLINT", "INT2"},
            {"DOUBLE PRECISION", "FLOAT8", "DOUBLE", "FLOAT"},
            {"REAL", "FLOAT4"},
            {"BOOLEAN", "BOOL"},
            {"TEXT", "CHARACTER VARYING", "VARCHAR"},
            {"TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"},
            {"TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE"},
            {"BYTEA", "BLOB"},
            {"NUMERIC", "DECIMAL"},
            {"SERIAL", "INTEGER"},
            {"BIGSERIAL", "BIGINT"},
        ]
        for group in equivalences:
            if m in group and l in group:
                return True

        return False

    async def apply(self, *, safe: bool = True) -> SchemaDiffResult:
        """Generate the diff and apply safe operations.

        Parameters
        ----------
        safe:
            If True (default), only apply CREATE TABLE and ADD COLUMN
            operations.  DROP TABLE, DROP COLUMN, and type changes are
            included in the returned diff but not executed.

        Returns the full diff (including skipped operations).
        """
        diff = await self.generate()

        async with await self._db.transaction():
            for op in diff.table_ops:
                if op.action == "create":
                    await self._db.execute(op.sql)
                elif not safe:
                    await self._db.execute(op.sql)

            for col_op in diff.column_ops:
                if col_op.action == "add":
                    await self._db.execute(col_op.sql)
                elif not safe and not col_op.sql.startswith("--"):
                    await self._db.execute(col_op.sql)

        return diff
