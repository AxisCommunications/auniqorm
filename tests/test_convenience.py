# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for sync(), get(defaults=), update_where(defaults=), filter(columns=)."""

import pytest

from auniqorm import AUniqORMBase, Column, PrimaryKey, RawSQL

# ── sync() ───────────────────────────────────────────────────────────────────


class TestSync:
    async def test_sync_insert_new(self, db):
        class Item(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)
            score: int = Column(default=0)

        await Item.create_table()

        item = Item(name="alpha", score=10)
        await item.sync()

        # PK should now be populated
        assert item.id is not None
        assert item.id >= 1

        # Verify in DB
        found = await Item.get(id=item.id)
        assert found is not None
        assert found.name == "alpha"
        assert found.score == 10

    async def test_sync_update_existing(self, db):
        class Task(AUniqORMBase):
            id: int = PrimaryKey()
            title: str = Column(nullable=False)
            status: str = Column(default="open")

        await Task.create_table()

        task = Task(title="Write tests", status="open")
        await task.insert()
        assert task.id >= 1

        # Mutate and sync back
        task.status = "done"
        await task.sync()

        # Verify updated in DB
        refreshed = await Task.get(id=task.id)
        assert refreshed is not None
        assert refreshed.status == "done"

    async def test_sync_refreshes_server_state(self, db):
        """sync() re-fetches after update, picking up server-side effects."""

        class Counter(AUniqORMBase):
            id: int = PrimaryKey()
            val: int = Column(default=0)

        await Counter.create_table()

        c = Counter(val=5)
        await c.insert()

        # Use update() with RawSQL to trigger server-side change,
        # then sync to re-fetch
        await c.update(val=RawSQL("val + 94"))
        # After update with RawSQL, local state is re-fetched
        assert c.val == 99

        # Also test that sync() on a clean instance re-fetches properly:
        # Modify DB directly, then create fresh reference and sync
        await db.execute("UPDATE counters SET val = 200 WHERE id = ?", [c.id])
        c2 = Counter(id=c.id, val=0)
        # c2 has stale local val=0; sync pushes 0 then re-fetches
        await c2.sync()
        # sync pushed val=0, so DB should be 0 now
        assert c2.val == 0

    async def test_sync_no_pk_raises(self, db):
        class NoPK(AUniqORMBase, table="no_pk"):
            name: str = Column(nullable=False)

        await NoPK.create_table()
        item = NoPK(name="oops")
        with pytest.raises(ValueError, match="no primary key"):
            await item.sync()


# ── get(defaults=) ───────────────────────────────────────────────────────────


class TestGetDefaults:
    async def test_get_creates_when_missing(self, db):
        class User(AUniqORMBase):
            id: int = PrimaryKey()
            email: str = Column(nullable=False, unique=True)
            name: str = Column(nullable=False)
            role: str = Column(default="viewer")

        await User.create_table()

        user = await User.get(
            email="alice@example.com",
            defaults={"name": "Alice", "role": "admin"},
        )
        assert user is not None
        assert user.email == "alice@example.com"
        assert user.name == "Alice"
        assert user.role == "admin"
        assert user.id >= 1

    async def test_get_returns_existing_with_defaults(self, db):
        class User(AUniqORMBase, table="users2"):
            id: int = PrimaryKey()
            email: str = Column(nullable=False, unique=True)
            name: str = Column(nullable=False)

        await User.create_table()

        # Pre-insert
        existing = User(email="bob@example.com", name="Bob")
        await existing.insert()

        # get with defaults should return existing, not create
        found = await User.get(email="bob@example.com", defaults={"name": "Robert"})
        assert found is not None
        assert found.id == existing.id
        assert found.name == "Bob"  # NOT "Robert"

    async def test_get_without_defaults_returns_none(self, db):
        class Entry(AUniqORMBase, table="entries"):
            id: int = PrimaryKey()
            key: str = Column(nullable=False)

        await Entry.create_table()

        result = await Entry.get(key="nonexistent")
        assert result is None

    async def test_get_defaults_never_returns_none(self, db):
        class Tag(AUniqORMBase):
            id: int = PrimaryKey()
            label: str = Column(nullable=False)

        await Tag.create_table()

        # Even if nothing exists, defaults ensures creation
        tag = await Tag.get(label="new-tag", defaults={})
        assert tag is not None
        assert tag.label == "new-tag"


# ── update_where(defaults=) ─────────────────────────────────────────────────


class TestUpdateWhereDefaults:
    async def test_updates_existing_row(self, db):
        class Profile(AUniqORMBase):
            id: int = PrimaryKey()
            email: str = Column(nullable=False, unique=True)
            name: str = Column(nullable=False)
            visits: int = Column(default=0)

        await Profile.create_table()
        await Profile(email="eve@example.com", name="Eve", visits=5).insert()

        await Profile.update_where(
            {"visits": 10},
            defaults={"name": "Eve Default"},
            email="eve@example.com",
        )

        found = await Profile.get(email="eve@example.com")
        assert found is not None
        assert found.visits == 10
        assert found.name == "Eve"  # NOT replaced

    async def test_creates_when_no_match(self, db):
        class Profile(AUniqORMBase, table="profiles2"):
            id: int = PrimaryKey()
            email: str = Column(nullable=False, unique=True)
            name: str = Column(nullable=False)
            visits: int = Column(default=0)

        await Profile.create_table()

        await Profile.update_where(
            {"visits": 1},
            defaults={"name": "New User"},
            email="new@example.com",
        )

        found = await Profile.get(email="new@example.com")
        assert found is not None
        assert found.name == "New User"
        assert found.visits == 1

    async def test_without_defaults_no_create(self, db):
        class Record(AUniqORMBase):
            id: int = PrimaryKey()
            key: str = Column(nullable=False)
            val: int = Column(default=0)

        await Record.create_table()

        # No defaults → normal update_where (no-op if no match)
        await Record.update_where({"val": 99}, key="ghost")

        all_records = await Record.filter()
        assert len(all_records) == 0


# ── filter(columns=) ────────────────────────────────────────────────────────


class TestFilterColumns:
    async def test_returns_tuples(self, db):
        class Product(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)
            price: int = Column(default=0)

        await Product.create_table()
        await Product(name="Widget", price=100).insert()
        await Product(name="Gadget", price=200).insert()

        results = await Product.filter(columns=["name", "price"], order_by="name")
        assert results == [("Gadget", 200), ("Widget", 100)]

    async def test_flat_single_column(self, db):
        class Color(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)

        await Color.create_table()
        await Color(name="red").insert()
        await Color(name="blue").insert()
        await Color(name="green").insert()

        names = await Color.filter(columns=["name"], flat=True, order_by="name")
        assert names == ["blue", "green", "red"]

    async def test_columns_with_filter_kwargs(self, db):
        class Score(AUniqORMBase):
            id: int = PrimaryKey()
            player: str = Column(nullable=False)
            points: int = Column(default=0)

        await Score.create_table()
        await Score(player="alice", points=10).insert()
        await Score(player="bob", points=20).insert()
        await Score(player="alice", points=30).insert()

        results = await Score.filter(
            columns=["points"], flat=True, player="alice", order_by="points"
        )
        assert results == [10, 30]

    async def test_flat_multiple_columns_raises(self, db):
        class Dummy(AUniqORMBase, table="dummies"):
            id: int = PrimaryKey()
            a: str = Column(nullable=False)
            b: str = Column(nullable=False)

        await Dummy.create_table()
        await Dummy(a="x", b="y").insert()

        with pytest.raises(ValueError, match="flat=True requires exactly one"):
            await Dummy.filter(columns=["a", "b"], flat=True)

    async def test_columns_with_limit(self, db):
        class Num(AUniqORMBase):
            id: int = PrimaryKey()
            val: int = Column(default=0)

        await Num.create_table()
        for i in range(5):
            await Num(val=i).insert()

        results = await Num.filter(columns=["val"], flat=True, order_by="val", limit=3)
        assert results == [0, 1, 2]
