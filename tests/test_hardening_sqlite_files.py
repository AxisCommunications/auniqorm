# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""New SQLite database files are owner-only."""

from __future__ import annotations

import os
import stat
import sys

import pytest

from auniqorm.drivers.sqlite import SQLiteDriver


def _drivers():
    yield pytest.param(SQLiteDriver, id="aiosqlite")
    try:
        from auniqorm.drivers.sqlite_native import SQLiteNativeDriver
    except ImportError:
        return
    yield pytest.param(SQLiteNativeDriver, id="native")


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX file modes")
class TestSQLiteFileMode:
    @pytest.fixture
    def permissive_umask(self):
        previous = os.umask(0o022)
        yield
        os.umask(previous)

    @pytest.mark.parametrize("driver_cls", _drivers())
    async def test_new_database_files_are_owner_only(
        self, driver_cls, tmp_path, permissive_umask
    ):
        path = tmp_path / "private.db"
        driver = driver_cls(f"sqlite:///{path}")
        await driver.connect()
        try:
            await driver.execute("CREATE TABLE t (id INTEGER)")
            await driver.execute("INSERT INTO t VALUES (1)")
            files = [p for p in tmp_path.iterdir() if p.name.startswith("private.db")]
            assert path in files
            modes = {p.name: stat.S_IMODE(p.stat().st_mode) for p in files}
        finally:
            await driver.disconnect()

        assert all(mode & 0o077 == 0 for mode in modes.values()), modes

    @pytest.mark.parametrize("driver_cls", _drivers())
    async def test_existing_files_keep_their_mode(
        self, driver_cls, tmp_path, permissive_umask
    ):
        path = tmp_path / "shared.db"
        path.touch(mode=0o644)
        path.chmod(0o644)
        driver = driver_cls(f"sqlite:///{path}")
        await driver.connect()
        await driver.disconnect()

        assert stat.S_IMODE(path.stat().st_mode) == 0o644

    @pytest.mark.parametrize("driver_cls", _drivers())
    async def test_memory_databases_touch_no_files(
        self, driver_cls, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)
        driver = driver_cls("sqlite://:memory:")
        await driver.connect()
        await driver.disconnect()

        assert list(tmp_path.iterdir()) == []
