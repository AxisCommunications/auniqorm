# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""SQLite single-row reads stop at the first row."""

from __future__ import annotations

import time

import pytest


class TestSQLiteFetchOneStopsEarly:
    async def test_fetch_one_does_not_materialise_the_result(self, tmp_path):
        pytest.importorskip("auniqorm_native")
        from auniqorm.drivers.sqlite_native import SQLiteNativeDriver

        driver = SQLiteNativeDriver(f"sqlite:///{tmp_path / 'one.db'}")
        await driver.connect()
        endless = (
            "WITH RECURSIVE n(x) AS "
            "(SELECT 1 UNION ALL SELECT x + 1 FROM n LIMIT 5000000) "
            "SELECT x FROM n"
        )
        try:
            started = time.perf_counter()
            assert await driver.fetch_one(endless) == {"x": 1}
            assert await driver.fetch_one_msgpack(endless)
            elapsed = time.perf_counter() - started
        finally:
            await driver.disconnect()

        assert elapsed < 0.25, f"fetch_one read the whole result ({elapsed:.2f}s)"

    async def test_returning_still_applies_every_row(self, tmp_path):
        pytest.importorskip("auniqorm_native")
        from auniqorm.drivers.sqlite_native import SQLiteNativeDriver

        driver = SQLiteNativeDriver(f"sqlite:///{tmp_path / 'ret.db'}")
        await driver.connect()
        try:
            await driver.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
            await driver.execute("INSERT INTO t (n) VALUES (1), (2), (3)")
            row = await driver.fetch_one("UPDATE t SET n = n + 10 RETURNING n")
            assert row is not None
            assert await driver.fetch_all("SELECT n FROM t ORDER BY id") == [
                {"n": 11},
                {"n": 12},
                {"n": 13},
            ]
        finally:
            await driver.disconnect()
