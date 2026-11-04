# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for auniqorm.drivers.registry."""

import pytest

from auniqorm.drivers.registry import DriverRegistry
from auniqorm.exceptions import DriverNotFoundError


class TestDriverRegistry:
    def test_available_protocols(self):
        protocols = DriverRegistry.available_protocols()
        assert "postgresql" in protocols
        assert "sqlite" in protocols
        assert "mysql" in protocols

    @pytest.mark.parametrize(
        ("protocol", "env", "driver"),
        [
            ("postgresql", "AUNIQORM_PG_NATIVE", "PostgresAsyncpgDriver"),
            ("sqlite", "AUNIQORM_SQLITE_NATIVE", "SQLiteDriver"),
            ("mysql", "AUNIQORM_MYSQL_NATIVE", "MySQLDriver"),
        ],
    )
    def test_native_driver_can_be_disabled(self, monkeypatch, protocol, env, driver):
        monkeypatch.setenv(env, "0")
        assert DriverRegistry.get(protocol).__name__ == driver

    def test_native_driver_is_default_when_available(self, monkeypatch):
        pytest.importorskip("auniqorm_native")
        monkeypatch.delenv("AUNIQORM_SQLITE_NATIVE", raising=False)
        cls = DriverRegistry.get("sqlite")
        assert cls.__name__ == "SQLiteNativeDriver"

    def test_unknown_protocol_raises(self):
        with pytest.raises(DriverNotFoundError):
            DriverRegistry.get("oracle")

    def test_register_custom_protocol(self):
        from auniqorm.drivers import BaseDriver

        class FakeDriver(BaseDriver):
            @property
            def is_connected(self):
                return False

            async def connect(self):
                pass

            async def disconnect(self):
                pass

            async def execute(self, query, values=None):
                pass

            async def execute_many(self, query, values_list):
                pass

            async def fetch_one(self, query, values=None):
                pass

            async def fetch_all(self, query, values=None):
                return []

            async def fetch_val(self, query, values=None, column=0):
                pass

            async def begin(self):
                pass

            async def commit(self):
                pass

            async def rollback(self):
                pass

        DriverRegistry.register("fakedb", "tests.test_registry", "FakeDriver")
        assert "fakedb" in DriverRegistry.available_protocols()
        # Clean up
        DriverRegistry._drivers.pop("fakedb", None)
