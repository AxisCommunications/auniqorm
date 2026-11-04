# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Pandas DataFrame integration for aUniqORM.

Provides read_sql() and to_sql() helpers that bridge aUniqORM's async
Database connections with pandas DataFrames — addressing the common
migration pain point for callers using pd.read_sql / df.to_sql.

Requires: pandas (optional dependency).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd

    from auniqorm.connection import Database


def quote_identifier(name: object, dialect: str) -> str:
    """Quote *name* as pandas does: backticks on MySQL, double quotes elsewhere."""
    q = "`" if dialect == "mysql" else '"'
    return q + str(name).replace(q, q + q) + q


async def read_sql(
    db: Database,
    query: str,
    values: list[Any] | None = None,
) -> pd.DataFrame:
    """Execute a SQL query and return results as a pandas DataFrame.

    This is the async equivalent of ``pd.read_sql(query, connection)``.

    Parameters
    ----------
    db:
        A connected :class:`~auniqorm.connection.Database` instance.
    query:
        Arbitrary SQL (SELECT, JOINs, subqueries, aggregations, etc.).  Must be
        trusted: pass external data through *values*, never into *query*.
    values:
        Query parameters for placeholder substitution.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns matching the query result set.

    Example
    -------
    ::

        from auniqorm import Database
        from auniqorm.pandas_io import read_sql

        async with Database("postgresql://user:pass@host/db") as db:
            df = await read_sql(db, "SELECT * FROM detections WHERE score > $1", [0.8])
            df = await read_sql(db, '''
                SELECT d.*, m.filename
                FROM detections d
                JOIN media m ON d.media_id = m.id
                WHERE d.score > $1
            ''', [0.5])
    """
    import pandas as pd

    rows = await db.fetch_all(query, values)
    return pd.DataFrame(rows)


async def read_sql_chunked(
    db: Database,
    query: str,
    values: list[Any] | None = None,
    *,
    batch_size: int = 5000,
) -> AsyncIterator[pd.DataFrame]:
    """Yield DataFrames in chunks for large result sets.

    Parameters
    ----------
    db:
        A connected Database instance.
    query:
        SQL query string.
    values:
        Query parameters.
    batch_size:
        Rows per chunk.

    Yields
    ------
    pandas.DataFrame
        One DataFrame per batch.
    """
    import pandas as pd

    async for batch in db.iterate_batches(query, values, batch_size=batch_size):
        yield pd.DataFrame(batch)


async def to_sql(
    db: Database,
    df: pd.DataFrame,
    table: str,
    *,
    if_exists: str = "append",
    batch_size: int = 1000,
) -> int:
    """Write a DataFrame to a database table.

    This is the async equivalent of ``df.to_sql(table, connection)``.  Like
    pandas, table and column names are quoted (so case-sensitive on PostgreSQL).

    Parameters
    ----------
    db:
        A connected :class:`~auniqorm.connection.Database` instance.
    df:
        DataFrame to write.
    table:
        Target table name.
    if_exists:
        What to do if the table has existing data:
        - ``"append"`` (default): Insert rows.
        - ``"replace"``: Drop and recreate the table, then insert.
    batch_size:
        Number of rows per INSERT statement.

    Returns
    -------
    int
        Number of rows written.

    Example
    -------
    ::

        from auniqorm import Database
        from auniqorm.pandas_io import to_sql

        async with Database("postgresql://...") as db:
            await to_sql(db, df, "detections", if_exists="append")
    """
    if df.empty:
        return 0

    dialect = db.protocol
    columns = list(df.columns)
    table = quote_identifier(table, dialect)
    quoted = [quote_identifier(c, dialect) for c in columns]
    cols_sql = ", ".join(quoted)

    if if_exists == "replace":
        await db.execute(f"DROP TABLE IF EXISTS {table}")
        col_defs = ", ".join(f"{c} TEXT" for c in quoted)
        await db.execute(f"CREATE TABLE {table} ({col_defs})")

    total_written = 0

    for start in range(0, len(df), batch_size):
        chunk = df.iloc[start : start + batch_size]
        rows_data = chunk.to_dict("records")

        if dialect == "postgresql":
            placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))
            sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"
            values_list = [[row[c] for c in columns] for row in rows_data]
            await db.execute_many(sql, values_list)
        else:
            ph = "%s" if dialect == "mysql" else "?"
            row_placeholders: list[str] = []
            all_values: list[Any] = []
            for row in rows_data:
                row_placeholders.append("(" + ", ".join(ph for _ in columns) + ")")
                all_values.extend(row[c] for c in columns)
            values_sql = ", ".join(row_placeholders)
            sql = f"INSERT INTO {table} ({cols_sql}) VALUES {values_sql}"
            await db.execute(sql, all_values)

        total_written += len(rows_data)

    return total_written
