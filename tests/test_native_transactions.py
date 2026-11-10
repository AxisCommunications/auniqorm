# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Integration tests for the native PostgreSQL engine's real transactions.

These exercise the Rust ``auniqorm_native`` engine's pinned-connection
``begin``/``commit``/``rollback`` path. They are skipped unless the native
extension is built and a Postgres server is reachable (the benchmark compose
stack: ``postgresql://bench:bench@localhost:5432/bench``).
"""

from __future__ import annotations

import pytest

pytest.importorskip("auniqorm_native")

from auniqorm.drivers.postgresql_native import PostgresNativeDriver  # noqa: E402

from ._services import PG_URL  # noqa: E402

pytestmark = pytest.mark.needs_pg


@pytest.fixture
async def driver():
    drv = PostgresNativeDriver(PG_URL)
    await drv.connect()
    await drv.execute("DROP TABLE IF EXISTS _txn_it")
    await drv.execute("CREATE TABLE _txn_it (id int primary key, v text)")
    try:
        yield drv
    finally:
        await drv.execute("DROP TABLE IF EXISTS _txn_it")
        await drv.disconnect()


async def test_rollback_discards_changes(driver: PostgresNativeDriver) -> None:
    await driver.begin()
    await driver.execute("INSERT INTO _txn_it (id, v) VALUES ($1, $2)", [1, "a"])
    # Visible inside the transaction.
    row = await driver.fetch_one("SELECT v FROM _txn_it WHERE id=$1", [1])
    assert row == {"v": "a"}
    await driver.rollback()
    # Gone after rollback.
    row = await driver.fetch_one("SELECT v FROM _txn_it WHERE id=$1", [1])
    assert row is None


async def test_commit_persists_changes(driver: PostgresNativeDriver) -> None:
    await driver.begin()
    await driver.execute("INSERT INTO _txn_it (id, v) VALUES ($1, $2)", [2, "b"])
    await driver.commit()
    row = await driver.fetch_one("SELECT v FROM _txn_it WHERE id=$1", [2])
    assert row == {"v": "b"}


async def test_commit_rollback_without_begin_are_noops(
    driver: PostgresNativeDriver,
) -> None:
    # No active transaction: these must not raise.
    await driver.commit()
    await driver.rollback()


async def test_queries_route_to_pool_after_commit(
    driver: PostgresNativeDriver,
) -> None:
    await driver.begin()
    await driver.execute("INSERT INTO _txn_it (id, v) VALUES ($1, $2)", [3, "c"])
    await driver.commit()
    # After commit the pinned connection is released; subsequent reads go to
    # the pool and still see the committed row.
    rows = await driver.fetch_all("SELECT id, v FROM _txn_it ORDER BY id")
    assert rows == [{"id": 3, "v": "c"}]


async def test_msgpack_fetch_inside_transaction(
    driver: PostgresNativeDriver,
) -> None:
    await driver.begin()
    await driver.execute("INSERT INTO _txn_it (id, v) VALUES ($1, $2)", [4, "d"])
    buf = await driver.fetch_one_msgpack("SELECT v FROM _txn_it WHERE id=$1", [4])
    assert isinstance(buf, bytes) and buf
    await driver.rollback()
