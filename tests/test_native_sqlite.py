# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Integration tests for the native SQLite engine.

Exercises the Rust ``auniqorm_native`` SQLite engine: the synchronous
GIL-released query path, transactions, RETURNING (bundled SQLite 3.35+), and the
msgpack fast path. Skipped only when the native extension is not built (SQLite
itself needs no server).
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

pytest.importorskip("auniqorm_native")

from auniqorm.drivers.sqlite_native import SQLiteNativeDriver  # noqa: E402


@pytest.fixture
async def driver():
    tmp = Path(tempfile.mkdtemp()) / "native.db"
    drv = SQLiteNativeDriver(f"sqlite:///{tmp}")
    await drv.connect()
    await drv.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT, n INTEGER)"
    )
    try:
        yield drv
    finally:
        await drv.disconnect()


async def test_insert_and_lastrowid(driver: SQLiteNativeDriver) -> None:
    rid = await driver.execute("INSERT INTO t (v, n) VALUES (?, ?)", ["a", 10])
    assert rid == 1
    row = await driver.fetch_one("SELECT id, v, n FROM t WHERE v=?", ["a"])
    assert row == {"id": 1, "v": "a", "n": 10}


async def test_rollback_discards_changes(driver: SQLiteNativeDriver) -> None:
    await driver.begin()
    await driver.execute("INSERT INTO t (v, n) VALUES (?, ?)", ["b", 20])
    assert await driver.fetch_one("SELECT n FROM t WHERE v=?", ["b"]) == {"n": 20}
    await driver.rollback()
    assert await driver.fetch_one("SELECT id FROM t WHERE v=?", ["b"]) is None


async def test_commit_persists_changes(driver: SQLiteNativeDriver) -> None:
    await driver.begin()
    await driver.execute("INSERT INTO t (v, n) VALUES (?, ?)", ["c", 30])
    await driver.commit()
    assert await driver.fetch_one("SELECT n FROM t WHERE v=?", ["c"]) == {"n": 30}


async def test_commit_rollback_without_begin_are_noops(
    driver: SQLiteNativeDriver,
) -> None:
    await driver.commit()
    await driver.rollback()


async def test_returning_clause(driver: SQLiteNativeDriver) -> None:
    row = await driver.fetch_one(
        "INSERT INTO t (v, n) VALUES (?, ?) RETURNING id, v", ["d", 40]
    )
    assert row == {"id": 1, "v": "d"}


async def test_null_and_types(driver: SQLiteNativeDriver) -> None:
    await driver.execute("INSERT INTO t (v, n) VALUES (?, ?)", [None, None])
    row = await driver.fetch_one("SELECT v, n FROM t WHERE id=1")
    assert row == {"v": None, "n": None}


async def test_msgpack_fetch(driver: SQLiteNativeDriver) -> None:
    await driver.execute("INSERT INTO t (v, n) VALUES (?, ?)", ["e", 50])
    buf = await driver.fetch_all_msgpack("SELECT id, v, n FROM t")
    assert isinstance(buf, bytes) and buf
    one = await driver.fetch_one_msgpack("SELECT v, n FROM t WHERE v=?", ["e"])
    assert isinstance(one, bytes) and one
    missing = await driver.fetch_one_msgpack("SELECT v FROM t WHERE v=?", ["zzz"])
    assert missing is None
