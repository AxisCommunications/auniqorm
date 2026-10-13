# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for the migration-friendly features: raw SQL, DataFrame I/O,
dynamic table names, and multi-database connections."""

from __future__ import annotations

import pytest

from auniqorm import Database, ModelRegistry, dynamic_model

# ---------------------------------------------------------------------------
# Raw SQL execution (already supported — verify the API works correctly)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_raw_sql_fetch_all(sqlite_db):
    """Database.fetch_all() supports arbitrary SQL."""
    db = sqlite_db
    await db.execute(
        "CREATE TABLE raw_test (id INTEGER PRIMARY KEY, name TEXT, score REAL)"
    )
    await db.execute(
        "INSERT INTO raw_test (name, score) VALUES (?, ?)",
        ["alice", 0.9],
    )
    await db.execute(
        "INSERT INTO raw_test (name, score) VALUES (?, ?)",
        ["bob", 0.7],
    )

    rows = await db.fetch_all("SELECT * FROM raw_test WHERE score > ?", [0.8])
    assert len(rows) == 1
    assert rows[0]["name"] == "alice"


@pytest.mark.asyncio
async def test_raw_sql_joins(sqlite_db):
    """Arbitrary JOINs work via raw SQL."""
    db = sqlite_db
    await db.execute("CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)")
    await db.execute(
        "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER)"
    )
    await db.execute("INSERT INTO authors (name) VALUES (?)", ["Tolkien"])
    await db.execute(
        "INSERT INTO books (title, author_id) VALUES (?, ?)",
        ["LOTR", 1],
    )

    rows = await db.fetch_all(
        "SELECT b.title, a.name FROM books b JOIN authors a ON b.author_id = a.id"
    )
    assert len(rows) == 1
    assert rows[0]["title"] == "LOTR"
    assert rows[0]["name"] == "Tolkien"


@pytest.mark.asyncio
async def test_raw_sql_subquery(sqlite_db):
    """Subqueries work via raw SQL."""
    db = sqlite_db
    await db.execute("CREATE TABLE scores (id INTEGER PRIMARY KEY, value REAL)")
    await db.execute("INSERT INTO scores (value) VALUES (?)", [10.0])
    await db.execute("INSERT INTO scores (value) VALUES (?)", [20.0])
    await db.execute("INSERT INTO scores (value) VALUES (?)", [30.0])

    result = await db.fetch_val(
        "SELECT AVG(value) FROM scores WHERE value > (SELECT MIN(value) FROM scores)"
    )
    assert result == 25.0


@pytest.mark.asyncio
async def test_raw_sql_aggregation(sqlite_db):
    """Complex aggregations work via raw SQL."""
    db = sqlite_db
    await db.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, category TEXT, count INTEGER)"
    )
    await db.execute("INSERT INTO events (category, count) VALUES (?, ?)", ["a", 5])
    await db.execute("INSERT INTO events (category, count) VALUES (?, ?)", ["a", 3])
    await db.execute("INSERT INTO events (category, count) VALUES (?, ?)", ["b", 7])

    rows = await db.fetch_all(
        "SELECT category, SUM(count) as total"
        " FROM events GROUP BY category ORDER BY total DESC"
    )
    assert rows[0]["category"] == "a"
    assert rows[0]["total"] == 8
    assert rows[1]["category"] == "b"
    assert rows[1]["total"] == 7


@pytest.mark.asyncio
async def test_raw_sql_iterate(sqlite_db):
    """Database.iterate() streams rows for large result sets."""
    db = sqlite_db
    await db.execute("CREATE TABLE big_table (id INTEGER PRIMARY KEY, val INTEGER)")
    for i in range(50):
        await db.execute("INSERT INTO big_table (val) VALUES (?)", [i])

    collected = []
    async for row in db.iterate("SELECT * FROM big_table ORDER BY id", batch_size=10):
        collected.append(row)

    assert len(collected) == 50
    assert collected[0]["val"] == 0
    assert collected[49]["val"] == 49


# ---------------------------------------------------------------------------
# Dynamic table names
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dynamic_model_basic(sqlite_db):
    """dynamic_model() creates a model bound to any table name."""
    db = sqlite_db
    await db.execute(
        "CREATE TABLE tracking_v3 (id INTEGER PRIMARY KEY, label TEXT, score REAL)"
    )
    await db.execute(
        "INSERT INTO tracking_v3 (label, score) VALUES (?, ?)",
        ["cat", 0.95],
    )
    await db.execute(
        "INSERT INTO tracking_v3 (label, score) VALUES (?, ?)",
        ["dog", 0.80],
    )

    tracking = dynamic_model("tracking_v3", {"id": int, "label": str, "score": float})
    tracking.__database__ = db

    results = await tracking.filter(label="cat")
    assert len(results) == 1
    assert results[0].label == "cat"
    assert results[0].score == 0.95


@pytest.mark.asyncio
async def test_dynamic_model_multiple_tables(sqlite_db):
    """Different dynamic models can target different tables."""
    db = sqlite_db
    await db.execute("CREATE TABLE table_a (id INTEGER PRIMARY KEY, x TEXT)")
    await db.execute("CREATE TABLE table_b (id INTEGER PRIMARY KEY, y INTEGER)")
    await db.execute("INSERT INTO table_a (x) VALUES (?)", ["hello"])
    await db.execute("INSERT INTO table_b (y) VALUES (?)", [42])

    model_a = dynamic_model("table_a", {"id": int, "x": str})
    model_a.__database__ = db
    model_b = dynamic_model("table_b", {"id": int, "y": int})
    model_b.__database__ = db

    a_results = await model_a.filter()
    b_results = await model_b.filter()

    assert len(a_results) == 1
    assert a_results[0].x == "hello"
    assert len(b_results) == 1
    assert b_results[0].y == 42


@pytest.mark.asyncio
async def test_dynamic_model_not_registered():
    """dynamic models don't pollute the global ModelRegistry."""
    before = set(ModelRegistry.get_models().keys())
    dynamic_model("ephemeral_table_xyz", {"id": int, "val": str})
    after = set(ModelRegistry.get_models().keys())
    assert before == after


# ---------------------------------------------------------------------------
# Multi-database connections
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multi_database_independent():
    """Multiple Database instances connect to different databases."""
    db1 = Database("sqlite:///:memory:", models=False)
    db2 = Database("sqlite:///:memory:", models=False)

    async with db1:
        async with db2:
            await db1.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
            await db2.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, num INTEGER)")

            await db1.execute("INSERT INTO t1 (val) VALUES (?)", ["from_db1"])
            await db2.execute("INSERT INTO t2 (num) VALUES (?)", [999])

            rows1 = await db1.fetch_all("SELECT * FROM t1")
            rows2 = await db2.fetch_all("SELECT * FROM t2")

            assert rows1[0]["val"] == "from_db1"
            assert rows2[0]["num"] == 999

            # db1 shouldn't have t2
            with pytest.raises(Exception):
                await db1.fetch_all("SELECT * FROM t2")


@pytest.mark.asyncio
async def test_models_false_skips_registry():
    """Database(models=False) doesn't bind or create model tables."""
    db = Database("sqlite:///:memory:", models=False)
    async with db:
        await db.execute("CREATE TABLE manual (id INTEGER PRIMARY KEY, data TEXT)")
        await db.execute("INSERT INTO manual (data) VALUES (?)", ["test"])
        rows = await db.fetch_all("SELECT * FROM manual")
        assert rows[0]["data"] == "test"


@pytest.mark.asyncio
async def test_dynamic_model_with_separate_db():
    """Dynamic model bound to a separate database connection."""
    db = Database("sqlite:///:memory:", models=False)
    async with db:
        await db.execute(
            "CREATE TABLE detections"
            " (id INTEGER PRIMARY KEY, label TEXT,"
            " confidence REAL)"
        )
        await db.execute(
            "INSERT INTO detections (label, confidence) VALUES (?, ?)",
            ["person", 0.92],
        )

        det = dynamic_model(
            "detections",
            {"id": int, "label": str, "confidence": float},
        )
        det.__database__ = db

        results = await det.filter(confidence__gt=0.5)
        assert len(results) == 1
        assert results[0].label == "person"


# ---------------------------------------------------------------------------
# DataFrame I/O (requires pandas)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_read_sql_basic(sqlite_db):
    """read_sql returns a DataFrame from arbitrary SQL."""
    pytest.importorskip("pandas")
    from auniqorm.pandas_io import read_sql

    db = sqlite_db
    await db.execute(
        "CREATE TABLE measurements (id INTEGER PRIMARY KEY, temp REAL, sensor TEXT)"
    )
    await db.execute(
        "INSERT INTO measurements (temp, sensor) VALUES (?, ?)",
        [22.5, "A"],
    )
    await db.execute(
        "INSERT INTO measurements (temp, sensor) VALUES (?, ?)",
        [23.1, "B"],
    )

    df = await read_sql(db, "SELECT * FROM measurements ORDER BY temp")
    assert len(df) == 2
    assert list(df.columns) == ["id", "temp", "sensor"]
    assert df.iloc[0]["temp"] == 22.5


@pytest.mark.asyncio
async def test_read_sql_with_join(sqlite_db):
    """read_sql handles complex JOINs."""
    pytest.importorskip("pandas")
    from auniqorm.pandas_io import read_sql

    db = sqlite_db
    await db.execute("CREATE TABLE sensors (id INTEGER PRIMARY KEY, name TEXT)")
    await db.execute(
        "CREATE TABLE readings (id INTEGER PRIMARY KEY, sensor_id INTEGER, value REAL)"
    )
    await db.execute("INSERT INTO sensors (name) VALUES (?)", ["temp_1"])
    await db.execute(
        "INSERT INTO readings (sensor_id, value) VALUES (?, ?)",
        [1, 42.0],
    )

    df = await read_sql(
        db,
        "SELECT r.value, s.name FROM readings r JOIN sensors s ON r.sensor_id = s.id",
    )
    assert len(df) == 1
    assert df.iloc[0]["name"] == "temp_1"
    assert df.iloc[0]["value"] == 42.0


@pytest.mark.asyncio
async def test_to_sql_basic(sqlite_db):
    """to_sql writes a DataFrame to a table."""
    pd = pytest.importorskip("pandas")
    from auniqorm.pandas_io import to_sql

    db = sqlite_db
    await db.execute("CREATE TABLE output_table (name TEXT, score REAL)")

    df = pd.DataFrame({"name": ["x", "y", "z"], "score": [1.0, 2.0, 3.0]})
    written = await to_sql(db, df, "output_table")

    assert written == 3
    rows = await db.fetch_all("SELECT * FROM output_table ORDER BY score")
    assert len(rows) == 3
    assert rows[0]["name"] == "x"


@pytest.mark.asyncio
async def test_to_sql_replace(sqlite_db):
    """to_sql with if_exists='replace' recreates the table."""
    pd = pytest.importorskip("pandas")
    from auniqorm.pandas_io import to_sql

    db = sqlite_db
    await db.execute("CREATE TABLE replace_test (col1 TEXT)")
    await db.execute("INSERT INTO replace_test VALUES (?)", ["old"])

    df = pd.DataFrame({"col1": ["new1", "new2"]})
    await to_sql(db, df, "replace_test", if_exists="replace")

    rows = await db.fetch_all("SELECT * FROM replace_test")
    assert len(rows) == 2
    assert rows[0]["col1"] == "new1"


@pytest.mark.asyncio
async def test_read_sql_chunked(sqlite_db):
    """read_sql_chunked yields DataFrames in batches."""
    pytest.importorskip("pandas")
    from auniqorm.pandas_io import read_sql_chunked

    db = sqlite_db
    await db.execute("CREATE TABLE chunked (id INTEGER PRIMARY KEY, val INTEGER)")
    for i in range(25):
        await db.execute("INSERT INTO chunked (val) VALUES (?)", [i])

    chunks = []
    async for chunk in read_sql_chunked(db, "SELECT * FROM chunked", batch_size=10):
        chunks.append(chunk)

    assert len(chunks) == 3  # 10 + 10 + 5
    assert len(chunks[0]) == 10
    assert len(chunks[2]) == 5
