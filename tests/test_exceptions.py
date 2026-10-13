# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for auniqorm.exceptions."""

import pytest

from auniqorm.exceptions import (
    AUniqORMError,
    ConnectionError,
    DriverNotFoundError,
    QueryError,
    SchemaMigrationError,
)


@pytest.mark.parametrize(
    "exc",
    [ConnectionError, DriverNotFoundError, QueryError, SchemaMigrationError],
)
def test_exception_hierarchy(exc):
    assert issubclass(exc, AUniqORMError)


@pytest.mark.parametrize(
    "exc,args",
    [
        (AUniqORMError, ("boom",)),
        (ConnectionError, ("cannot connect",)),
        (QueryError, ("bad sql",)),
    ],
)
def test_raise_roundtrip(exc, args):
    with pytest.raises(exc):
        raise exc(*args)


def test_driver_not_found_error_message():
    err = DriverNotFoundError("redis")
    assert "redis" in str(err)
    assert "pip install" in str(err)
    assert err.protocol == "redis"


def test_driver_not_found_error_with_extra_msg():
    err = DriverNotFoundError("redis", " -- custom note")
    assert "custom note" in str(err)


def test_schema_migration_error():
    cause = RuntimeError("NOT NULL constraint")
    err = SchemaMigrationError("users", ["email", "age"], cause)
    assert err.table == "users"
    assert err.columns == ["email", "age"]
    assert err.cause is cause
    assert "users" in str(err)
    assert "email" in str(err)
    assert "automatic migration" in str(err)
