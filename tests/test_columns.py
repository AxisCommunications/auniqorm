# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for auniqorm.columns."""

import pytest

from auniqorm.columns import Column, ForeignKey, PrimaryKey


class TestColumn:
    @pytest.mark.parametrize(
        "attr,expected",
        [
            ("primary_key", False),
            ("nullable", True),
            ("unique", False),
            ("index", False),
            ("autoincrement", False),
            ("default", None),
            ("default_factory", None),
            ("server_default", None),
            ("db_type", None),
            ("max_length", None),
            ("foreign_key", None),
            ("on_delete", None),
            ("on_update", None),
            ("comment", None),
        ],
    )
    def test_defaults(self, attr, expected):
        assert getattr(Column(), attr) == expected

    def test_frozen(self):
        c = Column(primary_key=True)
        with pytest.raises(AttributeError):
            c.primary_key = False  # type: ignore[misc]

    def test_repr_minimal(self):
        c = Column()
        assert repr(c) == "Column()"

    def test_repr_with_flags(self):
        c = Column(primary_key=True, unique=True, foreign_key="users.id")
        r = repr(c)
        assert "primary_key=True" in r
        assert "unique=True" in r
        assert "foreign_key='users.id'" in r

    @pytest.mark.parametrize(
        "kwargs,attr,expected",
        [
            ({"db_type": "JSONB"}, "db_type", "JSONB"),
            ({"max_length": 255}, "max_length", 255),
        ],
    )
    def test_explicit_attrs(self, kwargs, attr, expected):
        assert getattr(Column(**kwargs), attr) == expected


class TestPrimaryKey:
    @pytest.mark.parametrize(
        "attr,expected",
        [("primary_key", True), ("nullable", False)],
    )
    def test_defaults(self, attr, expected):
        assert getattr(PrimaryKey(), attr) == expected

    def test_is_column(self):
        assert isinstance(PrimaryKey(), Column)


class TestForeignKey:
    def test_creates_column(self):
        fk = ForeignKey("users.id")
        assert isinstance(fk, Column)
        assert fk.foreign_key == "users.id"
        assert fk.on_delete == "CASCADE"

    @pytest.mark.parametrize(
        "kwargs,attr,expected",
        [
            ({"on_delete": "SET NULL"}, "on_delete", "SET NULL"),
            ({"on_update": "RESTRICT"}, "on_update", "RESTRICT"),
            ({"nullable": False}, "nullable", False),
            ({"index": True}, "index", True),
        ],
    )
    def test_overrides(self, kwargs, attr, expected):
        fk = ForeignKey("users.id", **kwargs)
        assert getattr(fk, attr) == expected
