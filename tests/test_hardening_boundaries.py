# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""SQL-bearing metadata is validated where it is used, not only when defined."""

from __future__ import annotations

import pytest

from auniqorm.columns import Column, PrimaryKey
from auniqorm.models import AUniqORMBase, F, ManyToMany, ModelRegistry, Q


class Item(AUniqORMBase):
    id: int = PrimaryKey()
    qty: int = Column(default=0)


async def _ready(db) -> None:
    ModelRegistry.register(Item)
    ModelRegistry.bind(db)
    await Item.create_table()


class TestQConnector:
    def test_connector_is_read_only(self):
        q = Q(Q(id=1), Q(id=2))

        with pytest.raises(AttributeError):
            q.connector = "OR TRUE OR"  # type: ignore[misc]

    def test_combinators_still_set_connectors(self):
        assert (Q(a=1) | Q(b=2)).connector == Q.OR
        assert (Q(a=1) & Q(b=2)).connector == Q.AND

    async def test_forged_connector_is_rejected_when_compiled(self, sqlite_db):
        await _ready(sqlite_db)
        await Item(qty=1).insert()
        await Item(qty=2).insert()
        q = Q(Q(id=1), Q(id=2))
        q._connector = "OR TRUE OR"

        with pytest.raises(ValueError, match="connector"):
            await Item.filter(q)


class TestFImmutability:
    @pytest.mark.parametrize("attribute", ["_expr", "_refs"])
    def test_attributes_cannot_be_reassigned(self, attribute):
        f = F("qty")

        with pytest.raises(AttributeError):
            setattr(f, attribute, "0 WHERE 1=1 --")

    def test_arithmetic_still_composes(self):
        expr = -(F("qty") + 1) * F("id") % 3
        assert expr.to_sql() == "((-((qty + 1)) * id) % 3)"

    async def test_update_where_uses_the_validated_expression(self, sqlite_db):
        await _ready(sqlite_db)
        await Item(qty=1).insert()
        await Item(qty=2).insert()

        await Item.update_where({"qty": F("qty") + 10}, id=1)

        assert [row.qty for row in await Item.filter(order_by="id")] == [11, 2]


class TestTableNameAssignment:
    def test_unsafe_table_names_are_rejected(self):
        class Widget(AUniqORMBase, table="widgets_hardening"):
            id: int = PrimaryKey()

        with pytest.raises(ValueError, match="table name"):
            Widget.__table_name__ = "widgets; DROP TABLE secrets;--"

        assert Widget.__table_name__ == "widgets_hardening"

    def test_safe_renames_are_allowed(self):
        class Gadget(AUniqORMBase, table="gadgets_hardening"):
            id: int = PrimaryKey()

        Gadget.__table_name__ = "gadgets_renamed"

        assert Gadget.__table_name__ == "gadgets_renamed"


class TestManyToManyTarget:
    @pytest.mark.parametrize("target", ["tags; DROP TABLE x", "a b", "", "x.y.z"])
    def test_unsafe_target_tables_are_rejected(self, target):
        with pytest.raises(ValueError, match="target table"):
            ManyToMany(target, through="article_tags")

    def test_valid_target_is_accepted(self):
        assert ManyToMany("tags", through="article_tags").target_table == "tags"
