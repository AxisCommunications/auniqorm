# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Shared fixtures for aUniqORM tests.

Provides a parametrized ``db`` fixture that runs every ORM-driven test against
three SQL dialects (sqlite, postgresql, mysql).  A ``DialectDriver`` wrapper
translates PG/MySQL SQL back to SQLite so we exercise dialect-specific code
paths in models.py without requiring real PG/MySQL servers.

Tests that use raw SQL or PRAGMA statements should use the ``sqlite_db``
fixture instead.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from typing import Any

import pytest

from auniqorm.connection import Database
from auniqorm.drivers import BaseDriver
from auniqorm.models import ModelRegistry

from ._services import SERVICES, available


def pytest_configure(config: pytest.Config) -> None:
    for name in SERVICES:
        config.addinivalue_line(
            "markers", f"{name}: skipped unless the compose service is reachable"
        )


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # Probed here, not at import, so collection never waits on a missing server.
    for item in items:
        for name in SERVICES:
            if item.get_closest_marker(name) and not available(name):
                item.add_marker(
                    pytest.mark.skip(reason=f"requires the {name[6:]} compose service")
                )


# ---------------------------------------------------------------------------
# DialectDriver — translates PG / MySQL SQL into SQLite SQL
# ---------------------------------------------------------------------------


class DialectDriver(BaseDriver):
    """Wraps a real SQLiteDriver, translating dialect-specific SQL on the fly.

    This lets aUniqORM's model layer generate PostgreSQL or MySQL SQL (with
    ``$1``/``%s`` placeholders, ``ANY()``, ``unnest()``, etc.) and still
    execute against an in-memory SQLite database.
    """

    def __init__(self, dialect: str, real_driver: BaseDriver) -> None:
        # Skip BaseDriver.__init__ — we don't need a URL
        self._dialect = dialect
        self._real = real_driver

    # ── Delegated lifecycle methods ────────────────────────────────────────

    @property
    def is_connected(self) -> bool:
        return self._real.is_connected

    @property
    def in_transaction(self) -> bool:
        return self._real.in_transaction

    async def connect(self) -> None:
        await self._real.connect()

    async def disconnect(self) -> None:
        await self._real.disconnect()

    async def begin(self) -> None:
        await self._real.begin()

    async def commit(self) -> None:
        await self._real.commit()

    async def rollback(self) -> None:
        await self._real.rollback()

    # ── Translated query methods ───────────────────────────────────────────

    async def execute(self, query: str, values: list[Any] | None = None) -> Any:
        q, v = self._translate(query, values)
        return await self._real.execute(q, v)

    async def execute_many(self, query: str, values_list: list[list[Any]]) -> None:
        q, _ = self._translate(query, None)
        return await self._real.execute_many(q, values_list)

    async def fetch_one(
        self, query: str, values: list[Any] | None = None
    ) -> dict[str, Any] | None:
        q, v = self._translate(query, values)
        return await self._real.fetch_one(q, v)

    async def fetch_all(
        self, query: str, values: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        q, v = self._translate(query, values)

        # Intercept information_schema introspection queries and rewrite
        # for SQLite (PRAGMA table_info).
        if "information_schema.columns" in query.lower():
            table_name = (values or [None])[0]
            rows = await self._real.fetch_all(f"PRAGMA table_info({table_name})")
            # Re-shape to match information_schema result format
            return [{"column_name": r["name"]} for r in rows]

        return await self._real.fetch_all(q, v)

    async def fetch_val(
        self, query: str, values: list[Any] | None = None, column: int = 0
    ) -> Any:
        q, v = self._translate(query, values)
        return await self._real.fetch_val(q, v, column=column)

    async def iterate(
        self, query: str, values: list[Any] | None = None, *, batch_size: int = 500
    ) -> AsyncIterator[dict[str, Any]]:
        q, v = self._translate(query, values)
        async for row in self._real.iterate(q, v, batch_size=batch_size):
            yield row

    async def iterate_batches(
        self, query: str, values: list[Any] | None = None, *, batch_size: int = 500
    ) -> AsyncIterator[list[dict[str, Any]]]:
        q, v = self._translate(query, values)
        async for batch in self._real.iterate_batches(q, v, batch_size=batch_size):
            yield batch

    # ── Translation dispatch ───────────────────────────────────────────────

    def _translate(
        self, sql: str, values: list[Any] | None
    ) -> tuple[str, list[Any] | None]:
        if self._dialect == "postgresql":
            return self._translate_pg(sql, values)
        if self._dialect == "mysql":
            return self._translate_mysql(sql, values)
        return sql, values  # sqlite — no translation

    # ── DDL type normalisation (PG / MySQL → SQLite) ───────────────────────

    _DDL_TYPE_MAP: dict[str, str] = {
        "SERIAL": "INTEGER",
        "BIGSERIAL": "INTEGER",
        "SMALLSERIAL": "INTEGER",
        "DOUBLE PRECISION": "REAL",
        "BOOLEAN": "INTEGER",
        "BIGINT": "INTEGER",
        "SMALLINT": "INTEGER",
        "VARCHAR": "TEXT",
        "CHAR": "TEXT",
        "TIMESTAMP": "TEXT",
        "TIMESTAMPTZ": "TEXT",
        "DATE": "TEXT",
        "TIME": "TEXT",
        "NUMERIC": "REAL",
        "DECIMAL": "REAL",
        "FLOAT": "REAL",
        "JSON": "TEXT",
        "JSONB": "TEXT",
        "UUID": "TEXT",
        "BYTEA": "BLOB",
        "BLOB": "BLOB",
        "INT": "INTEGER",
        "TINYINT": "INTEGER",
        "MEDIUMINT": "INTEGER",
        "DOUBLE": "REAL",
    }

    def _normalise_ddl_types(self, sql: str) -> str:
        """Replace PG / MySQL column types with SQLite equivalents in DDL."""
        if not sql.lstrip().upper().startswith(("CREATE ", "ALTER ")):
            return sql
        for pg_type, sqlite_type in self._DDL_TYPE_MAP.items():
            # Case-insensitive word-boundary replacement
            sql = re.sub(
                rf"\b{re.escape(pg_type)}\b",
                sqlite_type,
                sql,
                flags=re.IGNORECASE,
            )
        # MySQL AUTO_INCREMENT → SQLite AUTOINCREMENT
        sql = re.sub(r"\bAUTO_INCREMENT\b", "AUTOINCREMENT", sql, flags=re.IGNORECASE)
        # Clean up double spaces from removed tokens
        sql = re.sub(r"  +", " ", sql)
        return sql

    # ── PostgreSQL → SQLite ────────────────────────────────────────────────

    # Matches  = ANY($N)  with optional ::TYPE[] cast
    _PG_ANY_RE = re.compile(r"=\s*ANY\(\$(\d+)(?:::[A-Z]+(?:\[\])?)?\)", re.IGNORECASE)
    # Matches  SELECT <cols> FROM unnest($1::T[], $2::T[], ...) [AS t(...)]
    _PG_UNNEST_INSERT_RE = re.compile(
        r"SELECT\s+.+?\s+FROM\s+unnest\(([^)]+)\)", re.IGNORECASE
    )
    # Matches  FROM (SELECT * FROM unnest(...) AS t(...)) AS _d
    _PG_UNNEST_UPDATE_RE = re.compile(
        r"FROM\s+\(SELECT\s+\*\s+FROM\s+unnest\(([^)]+)\)\s+"
        r"AS\s+t\(([^)]+)\)\)\s+AS\s+_d\s+"
        r"WHERE\s+(\w+)\.(\w+)\s*=\s*_d\.(_pk)",
        re.IGNORECASE,
    )
    # Matches $N with optional ::TYPE or ::TYPE[] cast
    _PG_PLACEHOLDER_RE = re.compile(r"\$\d+(?:::[A-Za-z_]+(?:\[\])?)?")

    def _translate_pg(
        self, sql: str, values: list[Any] | None
    ) -> tuple[str, list[Any] | None]:
        values = list(values) if values else []

        # 0. Normalise DDL types (SERIAL → INTEGER, etc.)
        sql = self._normalise_ddl_types(sql)

        # 1. Handle  UPDATE ... FROM unnest(...) AS t(...) AS _d  (bulk_update)
        m = self._PG_UNNEST_UPDATE_RE.search(sql)
        if m:
            return self._pg_bulk_update(sql, m, values)

        # 2. Handle  INSERT ... SELECT * FROM unnest(...)  (bulk_insert)
        if "unnest(" in sql.lower():
            m = self._PG_UNNEST_INSERT_RE.search(sql)
            if m and "INSERT" in sql.upper():
                return self._pg_bulk_insert(sql, m, values)

        # 3. Handle  = ANY($N)  (IN-list lookups)
        sql, values = self._pg_expand_any(sql, values)

        # 4. Replace remaining $N placeholders (+ strip casts) with ?
        sql = self._PG_PLACEHOLDER_RE.sub("?", sql)

        # 5. ILIKE → LIKE (SQLite LIKE is case-insensitive for ASCII)
        sql = re.sub(r"\bILIKE\b", "LIKE", sql)

        # 6. MySQL-compat: LAST_INSERT_ID → last_insert_rowid (shouldn't
        #    appear in PG path but handle defensively)
        sql = sql.replace("LAST_INSERT_ID()", "last_insert_rowid()")

        return sql, values or None

    def _pg_expand_any(self, sql: str, values: list[Any]) -> tuple[str, list[Any]]:
        """Expand ``col = ANY($N)`` into ``col IN (?, ?, ...)``."""
        new_values = list(values)
        offset = 0  # track index shifts from expansion

        for m in list(self._PG_ANY_RE.finditer(sql)):
            param_idx = int(m.group(1)) - 1  # $1 → index 0
            actual_idx = param_idx  # values index
            lst = new_values[actual_idx + offset]
            if not isinstance(lst, (list, tuple)):
                lst = [lst]

            placeholders = ", ".join("?" for _ in lst)
            replacement = f"IN ({placeholders})"

            sql = sql[: m.start()] + replacement + sql[m.end() :]

            # Replace the list in values with its expanded elements
            new_values[actual_idx + offset : actual_idx + offset + 1] = lst
            offset += len(lst) - 1

        return sql, new_values

    def _pg_bulk_insert(
        self, sql: str, m: re.Match, values: list[Any]
    ) -> tuple[str, list[Any] | None]:
        """Translate PG unnest-based bulk INSERT to multi-row VALUES."""
        # values = [[col1_val1, col1_val2, ...], [col2_val1, col2_val2, ...], ...]
        # Transpose to row-major order
        col_arrays = values
        if not col_arrays or not col_arrays[0]:
            return sql, values

        n_rows = len(col_arrays[0])
        n_cols = len(col_arrays)

        flat_values: list[Any] = []
        row_phs: list[str] = []
        for row_i in range(n_rows):
            ph = ", ".join("?" for _ in range(n_cols))
            row_phs.append(f"({ph})")
            for col_i in range(n_cols):
                flat_values.append(col_arrays[col_i][row_i])

        values_sql = ", ".join(row_phs)

        # Extract columns part from the INSERT INTO ... (...) SELECT ...
        insert_match = re.match(
            r"(INSERT\s+INTO\s+\w+\s*\([^)]+\))\s+SELECT", sql, re.IGNORECASE
        )
        if insert_match:
            prefix = insert_match.group(1)
        else:
            prefix = sql[: sql.upper().index("SELECT")]

        new_sql = f"{prefix} VALUES {values_sql}"

        # Preserve ON CONFLICT clause if present
        conflict_match = re.search(r"\bON\s+CONFLICT\b.*$", sql, re.IGNORECASE)
        if conflict_match:
            new_sql += " " + conflict_match.group(0)

        return new_sql, flat_values

    def _pg_bulk_update(
        self, sql: str, m: re.Match, values: list[Any]
    ) -> tuple[str, list[Any] | None]:
        """Translate PG unnest-based bulk UPDATE to CASE WHEN."""
        # Extract table name and SET clause from the UPDATE prefix
        update_prefix = sql[: m.start()].strip()
        table_and_pk = m.group(3)  # table name
        pk_field = m.group(4)  # pk column name

        # Parse SET clause from prefix: UPDATE table SET col1=_d.col1, col2=_d.col2
        set_match = re.search(r"SET\s+(.+)", update_prefix, re.IGNORECASE)
        if not set_match:
            # Can't translate — fall through to placeholder replacement
            sql = self._PG_PLACEHOLDER_RE.sub("?", sql)
            return sql, values

        set_clause_raw = set_match.group(1).strip()
        # Parse "col = _d.col, col2 = _d.col2" → field names
        fields = [part.split("=")[0].strip() for part in set_clause_raw.split(",")]

        # values = [pk_array, col1_array, col2_array, ...]
        pk_array = values[0]
        col_arrays = values[1:]
        n_rows = len(pk_array)

        flat_values: list[Any] = []
        set_parts: list[str] = []

        for fi, field in enumerate(fields):
            case_parts: list[str] = []
            for ri in range(n_rows):
                case_parts.append(f"WHEN {pk_field} = ? THEN ?")
                flat_values.append(pk_array[ri])
                flat_values.append(col_arrays[fi][ri])
            set_parts.append(f"{field} = CASE {' '.join(case_parts)} ELSE {field} END")

        # WHERE pk IN (?, ?, ...)
        in_phs = ", ".join("?" for _ in pk_array)
        flat_values.extend(pk_array)

        new_sql = (
            f"UPDATE {table_and_pk} SET {', '.join(set_parts)} "
            f"WHERE {pk_field} IN ({in_phs})"
        )
        return new_sql, flat_values

    # ── MySQL → SQLite ─────────────────────────────────────────────────────

    def _translate_mysql(
        self, sql: str, values: list[Any] | None
    ) -> tuple[str, list[Any] | None]:
        sql = self._normalise_ddl_types(sql)
        sql = sql.replace("%s", "?")
        sql = sql.replace("LAST_INSERT_ID()", "last_insert_rowid()")
        sql = sql.replace("ESCAPE '\\\\'", "ESCAPE '\\'")  # MySQL → SQLite literal
        # MySQL INSERT IGNORE → SQLite INSERT OR IGNORE
        sql = re.sub(
            r"\bINSERT\s+IGNORE\s+INTO\b",
            "INSERT OR IGNORE INTO",
            sql,
            flags=re.IGNORECASE,
        )
        return sql, values


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_registry():
    """Clear model registry before each test to avoid cross-contamination."""
    ModelRegistry.clear()
    yield
    ModelRegistry.clear()


@pytest.fixture(params=["sqlite", "postgresql", "mysql"])
async def db(request):
    """Provide a connected in-memory database for each dialect.

    For non-sqlite dialects, the driver is wrapped with DialectDriver so the
    ORM generates dialect-specific SQL that gets translated back to SQLite.
    """
    dialect = request.param
    database = Database("sqlite://:memory:")
    await database.connect()

    if dialect != "sqlite":
        real_driver = database._driver
        database._driver = DialectDriver(dialect, real_driver)
        database._protocol = dialect

    ModelRegistry.bind(database)
    yield database
    await database.disconnect()


@pytest.fixture
async def sqlite_db():
    """Provide a plain SQLite in-memory database (no dialect translation).

    Use this for tests that issue raw SQL with ``?`` placeholders, ``PRAGMA``
    statements, or ``sqlite_master`` queries.
    """
    database = Database("sqlite://:memory:")
    await database.connect()
    ModelRegistry.bind(database)
    yield database
    await database.disconnect()
