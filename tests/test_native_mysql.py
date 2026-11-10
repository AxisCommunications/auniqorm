# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Integration tests for the native MySQL engine's real transactions.

These exercise the Rust ``auniqorm_native`` MySQL engine's pinned-connection
``begin``/``commit``/``rollback`` path and the ``%s`` -> ``?`` placeholder
rewrite. They are skipped unless the native extension is built and a MySQL
server is reachable (the benchmark compose stack:
``mysql://bench:bench@localhost:3306/bench``).
"""

from __future__ import annotations

import pytest

pytest.importorskip("auniqorm_native")

from auniqorm.drivers.mysql_native import MySQLNativeDriver  # noqa: E402

from ._services import MYSQL_URL  # noqa: E402

pytestmark = pytest.mark.needs_mysql


@pytest.fixture
async def driver():
    drv = MySQLNativeDriver(MYSQL_URL)
    await drv.connect()
    await drv.execute("DROP TABLE IF EXISTS _mysql_it")
    await drv.execute(
        "CREATE TABLE _mysql_it "
        "(id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(32), n INT)"
    )
    try:
        yield drv
    finally:
        await drv.execute("DROP TABLE IF EXISTS _mysql_it")
        await drv.disconnect()


async def test_placeholder_rewrite_and_roundtrip(driver: MySQLNativeDriver) -> None:
    # ORM-style %s placeholders must be rewritten to ? for mysql_async.
    await driver.execute("INSERT INTO _mysql_it (v, n) VALUES (%s, %s)", ["a", 10])
    row = await driver.fetch_one("SELECT v, n FROM _mysql_it WHERE v=%s", ["a"])
    assert row == {"v": "a", "n": 10}


async def test_rollback_discards_changes(driver: MySQLNativeDriver) -> None:
    await driver.begin()
    await driver.execute("INSERT INTO _mysql_it (v, n) VALUES (%s, %s)", ["b", 20])
    row = await driver.fetch_one("SELECT n FROM _mysql_it WHERE v=%s", ["b"])
    assert row == {"n": 20}
    await driver.rollback()
    row = await driver.fetch_one("SELECT n FROM _mysql_it WHERE v=%s", ["b"])
    assert row is None


async def test_commit_persists_and_last_insert_id(driver: MySQLNativeDriver) -> None:
    await driver.begin()
    await driver.execute("INSERT INTO _mysql_it (v, n) VALUES (%s, %s)", ["c", 30])
    # LAST_INSERT_ID must resolve on the same pinned connection.
    lid = await driver.fetch_val("SELECT LAST_INSERT_ID()")
    await driver.commit()
    assert lid == 1
    row = await driver.fetch_one("SELECT n FROM _mysql_it WHERE v=%s", ["c"])
    assert row == {"n": 30}


async def test_commit_rollback_without_begin_are_noops(
    driver: MySQLNativeDriver,
) -> None:
    await driver.commit()
    await driver.rollback()


async def test_msgpack_fetch(driver: MySQLNativeDriver) -> None:
    await driver.execute("INSERT INTO _mysql_it (v, n) VALUES (%s, %s)", ["d", 40])
    buf = await driver.fetch_all_msgpack("SELECT id, v, n FROM _mysql_it")
    assert isinstance(buf, bytes) and buf
    one = await driver.fetch_one_msgpack("SELECT v, n FROM _mysql_it WHERE v=%s", ["d"])
    assert isinstance(one, bytes) and one
