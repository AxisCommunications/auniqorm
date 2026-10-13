# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for auniqorm.models — metaclass, registry, and CRUD via SQLite."""

import pytest

from auniqorm.columns import Column, PrimaryKey
from auniqorm.models import AUniqORMBase, ModelRegistry


class TestAUniqORMBaseMeta:
    def test_table_name_explicit(self):
        class Widget(AUniqORMBase, table="custom_widgets"):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)

        assert Widget.__table_name__ == "custom_widgets"

    def test_table_name_default(self):
        class Gadget(AUniqORMBase):
            id: int = PrimaryKey()

        assert Gadget.__table_name__ == "gadgets"

    def test_model_registered(self):
        class Foo(AUniqORMBase):
            id: int = PrimaryKey()

        assert ModelRegistry.get_model("foos") is Foo

    def test_duplicate_table_raises(self):
        class A(AUniqORMBase, table="dup_table"):
            id: int = PrimaryKey()

        with pytest.raises(ValueError, match="already registered"):

            class B(AUniqORMBase, table="dup_table"):
                id: int = PrimaryKey()

    def test_plain_default_preserved(self):
        class Bar(AUniqORMBase):
            id: int = PrimaryKey()
            status: str = "active"

        # Metaclass wraps the plain default in a Column
        meta = Bar.__struct_defaults__[1]
        assert isinstance(meta, Column)
        assert meta.default == "active"

    def test_explicit_column_default(self):
        class Ev(AUniqORMBase, table="events_meta"):
            id: int = PrimaryKey()
            ts: str = Column(server_default="now()")

        ts_col = Ev.__struct_defaults__[1]
        assert isinstance(ts_col, Column)
        assert ts_col.server_default == "now()"


class TestModelRegistry:
    def test_clear(self):
        class Temp(AUniqORMBase):
            id: int = PrimaryKey()

        assert ModelRegistry.get_model("temps") is not None
        ModelRegistry.clear()
        assert ModelRegistry.get_model("temps") is None

    def test_get_models(self):
        class M1(AUniqORMBase):
            id: int = PrimaryKey()

        class M2(AUniqORMBase):
            id: int = PrimaryKey()

        models = ModelRegistry.get_models()
        assert "m1s" in models
        assert "m2s" in models

    def test_unregister(self):
        class Rm(AUniqORMBase):
            id: int = PrimaryKey()

        ModelRegistry.unregister(Rm)
        assert ModelRegistry.get_model("rms") is None

    def test_get_database_none(self):
        assert ModelRegistry.get_database() is None


class TestAUniqORMBaseSerialization:
    def test_to_dict(self):
        class Item(AUniqORMBase, table="items_ser"):
            id: int = PrimaryKey()
            label: str = Column(nullable=False)

        item = Item(id=1, label="test")
        d = item.to_dict()
        assert d["id"] == 1
        assert d["label"] == "test"

    def test_from_dict(self):
        class Item2(AUniqORMBase, table="items_ser2"):
            id: int = PrimaryKey()
            label: str = Column(nullable=False)

        item = Item2.from_dict({"id": 2, "label": "hello"})
        assert item.id == 2
        assert item.label == "hello"

    def test_from_row(self):
        class Item3(AUniqORMBase, table="items_ser3"):
            id: int = PrimaryKey()
            label: str = Column(nullable=False)

        item = Item3.from_row({"id": 3, "label": "row"})
        assert item.id == 3
        assert item.label == "row"
