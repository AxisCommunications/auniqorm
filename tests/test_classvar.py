# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Class-level attributes must survive the metaclass's column rewriting."""

from __future__ import annotations

from typing import ClassVar

from auniqorm import AUniqORMBase, Column, PrimaryKey


def test_classvar_keeps_its_value() -> None:
    class Cached(AUniqORMBase):
        id: int = PrimaryKey()
        name: str = Column()
        CACHE: ClassVar[dict] = {}
        LIMIT: ClassVar[int] = 10

    assert Cached.CACHE == {}
    assert Cached.LIMIT == 10


def test_classvar_is_not_a_column() -> None:
    class Tuned(AUniqORMBase):
        id: int = PrimaryKey()
        RETRIES: ClassVar[int] = 3

    assert Tuned.__struct_fields__ == ("id",)
    assert not isinstance(Tuned.RETRIES, Column)


def test_bare_classvar_annotation_is_left_alone() -> None:
    class Declared(AUniqORMBase):
        id: int = PrimaryKey()
        REGISTRY: ClassVar[dict]

    assert "REGISTRY" not in Declared.__struct_fields__
    assert not hasattr(Declared, "REGISTRY")


def test_regular_fields_still_become_columns() -> None:
    class Plain(AUniqORMBase):
        id: int = PrimaryKey()
        title: str = Column(nullable=False)
        count: int = 0
        maybe: str | None = None

    assert Plain.__struct_fields__ == ("id", "title", "count", "maybe")


def test_model_dunders_are_reported_on_the_class() -> None:
    class Thing(AUniqORMBase):
        id: int = PrimaryKey()

    assert Thing.__table_name__ == "things"
    assert Thing.__abstract__ is False
    assert Thing.__database__ is None
