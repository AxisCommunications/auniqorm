# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for F-expressions, streaming, reverse relations, abstract models."""

import pytest

from auniqorm.columns import Column, ForeignKey, PrimaryKey
from auniqorm.models import AUniqORMBase, F, ModelRegistry

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class Team(AUniqORMBase):
    id: int = PrimaryKey()
    name: str


class Player(AUniqORMBase):
    id: int = PrimaryKey()
    name: str
    score: int = Column(default=0)
    bonus: int = Column(default=0)
    team_id: int = ForeignKey("teams.id")


@pytest.fixture
async def seeded(db):
    ModelRegistry.register(Team)
    ModelRegistry.register(Player)
    ModelRegistry.bind(db)
    await Team.create_table()
    await Player.create_table()

    t1 = await Team(name="Alpha").insert()
    t2 = await Team(name="Beta").insert()

    p1 = await Player(name="Alice", score=10, bonus=5, team_id=t1.id).insert()
    p2 = await Player(name="Bob", score=20, bonus=3, team_id=t1.id).insert()
    p3 = await Player(name="Carol", score=15, bonus=7, team_id=t2.id).insert()

    return {"t1": t1, "t2": t2, "p1": p1, "p2": p2, "p3": p3}


# ---------------------------------------------------------------------------
# F-expressions
# ---------------------------------------------------------------------------


class TestFExpressions:
    def test_simple_column_ref(self):
        assert F("score").to_sql() == "score"

    @pytest.mark.parametrize(
        "expr,expected",
        [
            pytest.param(F("score") + 1, "(score + 1)", id="add"),
            pytest.param(F("score") - 5, "(score - 5)", id="sub"),
            pytest.param(F("price") * 2, "(price * 2)", id="mul"),
            pytest.param(F("total") / 10, "(total / 10)", id="div"),
            pytest.param(F("value") % 3, "(value % 3)", id="mod"),
            pytest.param(1 + F("score"), "(1 + score)", id="radd"),
            pytest.param(100 - F("score"), "(100 - score)", id="rsub"),
            pytest.param(2 * F("score"), "(2 * score)", id="rmul"),
            pytest.param(100 / F("score"), "(100 / score)", id="rtruediv"),
        ],
    )
    def test_arithmetic(self, expr, expected):
        assert expr.to_sql() == expected

    def test_neg(self):
        assert (-F("score")).to_sql() == "-(score)"

    def test_add_two_fields(self):
        assert (F("score") + F("bonus")).to_sql() == "(score + bonus)"

    def test_complex_expression(self):
        expr = (F("price") * F("quantity")) - F("discount")
        assert expr.to_sql() == "((price * quantity) - discount)"

    def test_repr(self):
        assert repr(F("score")) == "F('score')"
        assert repr(F("score") + 1) == "F('(score + 1)')"

    async def test_update_with_f(self, seeded):
        player = await Player.get(id=seeded["p1"].id)
        await player.update(score=F("score") + 10)
        # After update, the instance is refreshed
        assert player.score == 20

    async def test_update_f_with_two_fields(self, seeded):
        player = await Player.get(id=seeded["p1"].id)
        await player.update(score=F("score") + F("bonus"))
        assert player.score == 15  # 10 + 5

    async def test_update_where_with_f(self, seeded):
        await Player.update_where(
            {"score": F("score") * 2},
            team_id=seeded["t1"].id,
        )
        alice = await Player.get(id=seeded["p1"].id)
        bob = await Player.get(id=seeded["p2"].id)
        carol = await Player.get(id=seeded["p3"].id)
        assert alice.score == 20  # 10 * 2
        assert bob.score == 40  # 20 * 2
        assert carol.score == 15  # unchanged


# ---------------------------------------------------------------------------
# Streaming / iterate()
# ---------------------------------------------------------------------------


class TestIterate:
    async def test_iterate_all(self, seeded):
        results = []
        async for player in Player.iterate(order_by="id"):
            results.append(player)
        assert len(results) == 3
        assert results[0].name == "Alice"

    async def test_iterate_filtered(self, seeded):
        results = []
        async for player in Player.iterate(team_id=seeded["t1"].id, order_by="id"):
            results.append(player)
        assert len(results) == 2
        assert results[0].name == "Alice"
        assert results[1].name == "Bob"

    async def test_iterate_with_lookup(self, seeded):
        results = []
        async for player in Player.iterate(score__gte=15, order_by="-score"):
            results.append(player)
        assert len(results) == 2
        assert results[0].name == "Bob"
        assert results[1].name == "Carol"

    async def test_iterate_empty(self, seeded):
        results = []
        async for player in Player.iterate(score__gt=9999):
            results.append(player)
        assert results == []

    async def test_iterate_custom_batch_size(self, seeded):
        results = []
        async for player in Player.iterate(order_by="id", batch_size=1):
            results.append(player)
        assert len(results) == 3

    async def test_iterate_no_db_raises(self):
        class Unbound(AUniqORMBase, table="unbound_iter"):
            id: int = PrimaryKey()

        with pytest.raises(RuntimeError, match="not bound"):
            async for _ in Unbound.iterate():
                pass


# ---------------------------------------------------------------------------
# Reverse relations
# ---------------------------------------------------------------------------


class TestReverseRelations:
    async def test_reverse_relation(self, seeded):
        team = await Team.get(id=seeded["t1"].id)
        players = await team.fetch_related("players")
        assert isinstance(players, list)
        assert len(players) == 2
        names = {p.name for p in players}
        assert names == {"Alice", "Bob"}

    async def test_reverse_relation_cached(self, seeded):
        team = await Team.get(id=seeded["t1"].id)
        p1 = await team.fetch_related("players")
        p2 = await team.fetch_related("players")
        assert p1 is p2

    async def test_reverse_relation_empty(self, seeded):
        # Create a team with no players
        team3 = await Team(name="Gamma").insert()
        players = await team3.fetch_related("players")
        assert players == []

    async def test_reverse_not_found_raises(self, seeded):
        team = await Team.get(id=seeded["t1"].id)
        with pytest.raises(ValueError, match="no forward FK"):
            await team.fetch_related("nonexistent")


# ---------------------------------------------------------------------------
# Abstract model inheritance
# ---------------------------------------------------------------------------


class TestAbstractModels:
    def test_abstract_not_registered(self):
        class TimestampMixin(AUniqORMBase, abstract=True):
            created_at: str = Column(server_default="CURRENT_TIMESTAMP")
            updated_at: str = Column(server_default="CURRENT_TIMESTAMP")

        # Abstract model should NOT be in registry
        assert ModelRegistry.get_model("timestamp_mixins") is None
        assert ModelRegistry.get_model("") is None

    def test_concrete_inherits_fields(self):
        class BaseMixin(AUniqORMBase, abstract=True):
            created_at: str = Column(server_default="CURRENT_TIMESTAMP")

        class Article(BaseMixin, table="articles"):
            id: int = PrimaryKey()
            title: str

        # Article should have both its own fields and inherited ones
        assert "id" in Article.__struct_fields__
        assert "title" in Article.__struct_fields__
        assert "created_at" in Article.__struct_fields__

        # Article is registered
        assert ModelRegistry.get_model("articles") is Article

    async def test_concrete_child_crud(self, db):
        class AuditBase(AUniqORMBase, abstract=True):
            created_at: str = Column(server_default="CURRENT_TIMESTAMP")

        class Event(AuditBase, table="events"):
            id: int = PrimaryKey()
            name: str

        ModelRegistry.bind(db)
        await Event.create_table()

        evt = await Event(name="deploy").insert()
        assert evt.id is not None

        fetched = await Event.get(id=evt.id)
        assert fetched.name == "deploy"
        assert fetched.created_at is not None
