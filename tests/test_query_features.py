# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for filter(order_by/limit/offset), count(), exists(),
RawSQL update, on_conflict insert.
"""

import re

import pytest

from auniqorm.columns import Column, PrimaryKey
from auniqorm.models import AUniqORMBase, Q, RawSQL

from ._helpers import ready


def _safe_table(prefix, request):
    """Build a SQL-safe table name from a pytest parametrize id."""
    return f"{prefix}_{re.sub(r'[^0-9a-zA-Z]+', '_', request.node.callspec.id)}"


# ── order_by ─────────────────────────────────────────────────────────────────


class TestFilterOrderBy:
    async def test_order_by_asc(self, db):
        class TaskA(AUniqORMBase, table="tasks_a"):
            id: int = PrimaryKey()
            priority: int

        await ready(db, TaskA)
        await TaskA(priority=3).insert()
        await TaskA(priority=1).insert()
        await TaskA(priority=2).insert()

        results = await TaskA.filter(order_by="priority")
        assert [r.priority for r in results] == [1, 2, 3]

    async def test_order_by_desc(self, db):
        class TaskB(AUniqORMBase, table="tasks_b"):
            id: int = PrimaryKey()
            priority: int

        await ready(db, TaskB)
        await TaskB(priority=3).insert()
        await TaskB(priority=1).insert()
        await TaskB(priority=2).insert()

        results = await TaskB.filter(order_by="-priority")
        assert [r.priority for r in results] == [3, 2, 1]

    async def test_order_by_multi_column(self, db):
        class TaskC(AUniqORMBase, table="tasks_c"):
            id: int = PrimaryKey()
            priority: int
            name: str

        await ready(db, TaskC)
        await TaskC(priority=1, name="b").insert()
        await TaskC(priority=2, name="a").insert()
        await TaskC(priority=1, name="a").insert()

        results = await TaskC.filter(order_by=["priority", "name"])
        assert [(r.priority, r.name) for r in results] == [
            (1, "a"),
            (1, "b"),
            (2, "a"),
        ]

    async def test_order_by_with_filter(self, db):
        class TaskD(AUniqORMBase, table="tasks_d"):
            id: int = PrimaryKey()
            status: str
            priority: int

        await ready(db, TaskD)
        await TaskD(status="open", priority=3).insert()
        await TaskD(status="open", priority=1).insert()
        await TaskD(status="closed", priority=5).insert()

        results = await TaskD.filter(status="open", order_by="-priority")
        assert len(results) == 2
        assert [r.priority for r in results] == [3, 1]

    async def test_order_by_string_shorthand(self, db):
        class TaskE(AUniqORMBase, table="tasks_e"):
            id: int = PrimaryKey()
            name: str

        await ready(db, TaskE)
        await TaskE(name="c").insert()
        await TaskE(name="a").insert()
        await TaskE(name="b").insert()

        results = await TaskE.filter(order_by="name")
        assert [r.name for r in results] == ["a", "b", "c"]


# ── limit / offset ──────────────────────────────────────────────────────────


class TestFilterLimitOffset:
    async def test_limit(self, db):
        class NumA(AUniqORMBase, table="nums_a"):
            id: int = PrimaryKey()
            val: int

        await ready(db, NumA)
        for i in range(10):
            await NumA(val=i).insert()

        results = await NumA.filter(order_by="val", limit=3)
        assert len(results) == 3
        assert [r.val for r in results] == [0, 1, 2]

    async def test_offset(self, db):
        class NumB(AUniqORMBase, table="nums_b"):
            id: int = PrimaryKey()
            val: int

        await ready(db, NumB)
        for i in range(10):
            await NumB(val=i).insert()

        results = await NumB.filter(order_by="val", limit=3, offset=5)
        assert len(results) == 3
        assert [r.val for r in results] == [5, 6, 7]

    async def test_limit_with_filter(self, db):
        class NumC(AUniqORMBase, table="nums_c"):
            id: int = PrimaryKey()
            val: int
            grp: str

        await ready(db, NumC)
        for i in range(6):
            await NumC(val=i, grp="a" if i % 2 == 0 else "b").insert()

        results = await NumC.filter(grp="a", order_by="val", limit=2)
        assert len(results) == 2
        assert [r.val for r in results] == [0, 2]


# ── count() ──────────────────────────────────────────────────────────────────


class TestCount:
    async def test_count_all(self, db):
        class ItemA(AUniqORMBase, table="items_a"):
            id: int = PrimaryKey()
            name: str

        await ready(db, ItemA)
        await ItemA(name="a").insert()
        await ItemA(name="b").insert()
        await ItemA(name="c").insert()

        assert await ItemA.count() == 3

    async def test_count_filtered(self, db):
        class ItemB(AUniqORMBase, table="items_b"):
            id: int = PrimaryKey()
            status: str

        await ready(db, ItemB)
        await ItemB(status="open").insert()
        await ItemB(status="open").insert()
        await ItemB(status="closed").insert()

        assert await ItemB.count(status="open") == 2
        assert await ItemB.count(status="closed") == 1

    async def test_count_empty(self, db):
        class ItemC(AUniqORMBase, table="items_c"):
            id: int = PrimaryKey()

        await ready(db, ItemC)
        assert await ItemC.count() == 0


# ── exists() ─────────────────────────────────────────────────────────────────


class TestExists:
    async def test_exists_true(self, db):
        class ThingA(AUniqORMBase, table="things_a"):
            id: int = PrimaryKey()
            name: str

        await ready(db, ThingA)
        await ThingA(name="x").insert()

        assert await ThingA.exists(name="x") is True

    async def test_exists_false(self, db):
        class ThingB(AUniqORMBase, table="things_b"):
            id: int = PrimaryKey()
            name: str

        await ready(db, ThingB)
        assert await ThingB.exists(name="nope") is False

    async def test_exists_no_filter(self, db):
        class ThingC(AUniqORMBase, table="things_c"):
            id: int = PrimaryKey()

        await ready(db, ThingC)
        assert await ThingC.exists() is False
        await ThingC(id=1).insert()
        assert await ThingC.exists() is True


# ── RawSQL update ────────────────────────────────────────────────────────────


class TestRawSQLUpdate:
    async def test_raw_sql_expression(self, db):
        class Counter(AUniqORMBase):
            id: int = PrimaryKey()
            val: int

        await ready(db, Counter)
        c = Counter(id=1, val=10)
        await c.insert()

        await c.update(val=RawSQL("val + 1"))
        assert c.val == 11

    async def test_raw_sql_mixed_with_plain(self, db):
        class Record(AUniqORMBase):
            id: int = PrimaryKey()
            name: str
            score: int

        await ready(db, Record)
        r = Record(id=1, name="old", score=5)
        await r.insert()

        await r.update(name="new", score=RawSQL("score * 2"))
        assert r.name == "new"
        assert r.score == 10

    async def test_raw_sql_repr(self):
        r = RawSQL("NOW()")
        assert repr(r) == "RawSQL('NOW()')"


# ── Parameterized RawSQL ─────────────────────────────────────────────────────


class TestParameterizedRawSQL:
    async def test_update_coalesce_single_param(self, db):
        class Loc(AUniqORMBase):
            id: int = PrimaryKey()
            latitude: float

        await ready(db, Loc)
        loc = Loc(id=1, latitude=1.0)
        await loc.insert()

        await loc.update(latitude=RawSQL("COALESCE({}, latitude)", 5.0))
        assert loc.latitude == 5.0

    async def test_update_coalesce_null_preserves(self, db):
        class Loc2(AUniqORMBase):
            id: int = PrimaryKey()
            latitude: float

        await ready(db, Loc2)
        loc = Loc2(id=1, latitude=3.0)
        await loc.insert()

        await loc.update(latitude=RawSQL("COALESCE({}, latitude)", None))
        assert loc.latitude == 3.0

    async def test_update_multiple_params(self, db):
        class Calc(AUniqORMBase):
            id: int = PrimaryKey()
            result: int

        await ready(db, Calc)
        c = Calc(id=1, result=0)
        await c.insert()

        await c.update(result=RawSQL("{} + {}", 10, 20))
        assert c.result == 30

    async def test_update_where_parameterized(self, db):
        class Item(AUniqORMBase):
            id: int = PrimaryKey()
            val: int
            grp: str

        await ready(db, Item)
        await Item(id=1, val=100, grp="a").insert()
        await Item(id=2, val=200, grp="a").insert()

        await Item.update_where({"val": RawSQL("COALESCE({}, val)", 50)}, grp="a")

        items = await Item.filter(grp="a", order_by="id")
        assert items[0].val == 50
        assert items[1].val == 50

    async def test_batch_update_where_parameterized(self, db):
        class Score(AUniqORMBase):
            id: int = PrimaryKey()
            pts: int

        await ready(db, Score)
        await Score(id=1, pts=10).insert()
        await Score(id=2, pts=20).insert()

        await Score.batch_update_where(
            values={"pts": RawSQL("COALESCE({}, pts)", 99)},
            params_list=[[1], [2]],
            filter_fields=["id"],
        )

        s1 = await Score.get(id=1)
        s2 = await Score.get(id=2)
        assert s1.pts == 99
        assert s2.pts == 99

    async def test_mixed_raw_and_plain_params(self, db):
        class Mix(AUniqORMBase):
            id: int = PrimaryKey()
            name: str
            score: int

        await ready(db, Mix)
        m = Mix(id=1, name="old", score=5)
        await m.insert()

        await m.update(name="new", score=RawSQL("COALESCE({}, score)", 42))
        assert m.name == "new"
        assert m.score == 42

    async def test_backward_compat_no_params(self, db):
        class Counter2(AUniqORMBase):
            id: int = PrimaryKey()
            val: int

        await ready(db, Counter2)
        c = Counter2(id=1, val=10)
        await c.insert()

        await c.update(val=RawSQL("val + 1"))
        assert c.val == 11

    def test_param_count_mismatch_raises(self):
        import pytest

        with pytest.raises(ValueError, match="placeholder count"):
            RawSQL("COALESCE({}, col)", 1, 2)

    def test_repr_with_params(self):
        r = RawSQL("{} + 1", 5)
        assert repr(r) == "RawSQL('{} + 1', 5)"


# ── String / equality lookups (contains/startswith/endswith + i-variants + ne)


STRING_LOOKUP_CASES = [
    pytest.param(
        "contains",
        ["hello world", "goodbye", "worldly"],
        "world",
        {1, 3},
        id="contains",
    ),
    pytest.param(
        "startswith",
        ["prefix_one", "prefix_two", "other"],
        "prefix",
        {1, 2},
        id="startswith",
    ),
    pytest.param(
        "endswith",
        ["photo.jpg", "image.png", "banner.jpg"],
        ".jpg",
        {1, 3},
        id="endswith",
    ),
    pytest.param("iexact", ["Hello", "HELLO", "world"], "hello", {1, 2}, id="iexact"),
    pytest.param(
        "icontains",
        ["Report.PDF", "data.csv", "report_final.pdf"],
        "report",
        {1, 3},
        id="icontains",
    ),
    pytest.param(
        "istartswith",
        ["Python", "PYTHON3", "javascript"],
        "python",
        {1, 2},
        id="istartswith",
    ),
    pytest.param(
        "iendswith",
        ["image.PNG", "photo.png", "doc.pdf"],
        ".png",
        {1, 2},
        id="iendswith",
    ),
    pytest.param("ne", ["open", "closed", "open"], "open", {2}, id="ne"),
]


@pytest.mark.parametrize("lookup,rows,value,expected_ids", STRING_LOOKUP_CASES)
async def test_string_lookup(db, lookup, rows, value, expected_ids, request):
    table = _safe_table("lk", request)

    class M(AUniqORMBase, table=table):
        id: int = PrimaryKey()
        name: str

    await ready(db, M)
    for i, n in enumerate(rows, 1):
        await M(id=i, name=n).insert()

    results = await M.filter(**{f"name__{lookup}": value}, order_by="id")
    assert {r.id for r in results} == expected_ids


@pytest.mark.parametrize(
    "lookup,value",
    [("contains", "xyz"), ("startswith", "z"), ("endswith", ".zzz")],
)
async def test_string_lookup_no_match(db, lookup, value, request):
    table = _safe_table("lk_nomatch", request)

    class M(AUniqORMBase, table=table):
        id: int = PrimaryKey()
        name: str

    await ready(db, M)
    await M(name="abc").insert()
    assert await M.filter(**{f"name__{lookup}": value}) == []


async def test_iexact_with_other_filters(db):
    class Entry(AUniqORMBase):
        id: int = PrimaryKey()
        name: str
        active: int

    await ready(db, Entry)
    await Entry(id=1, name="Alice", active=1).insert()
    await Entry(id=2, name="ALICE", active=0).insert()
    await Entry(id=3, name="alice", active=1).insert()

    results = await Entry.filter(name__iexact="alice", active=1, order_by="id")
    assert len(results) == 2
    assert {r.id for r in results} == {1, 3}


# ── on_conflict insert ──────────────────────────────────────────────────────

# Note: on_conflict uses PostgreSQL syntax (ON CONFLICT ...) which is not
# supported by SQLite. We test that the parameter is accepted without error
# when on_conflict=None (default behaviour).


class TestOnConflictInsert:
    async def test_insert_without_on_conflict(self, db):
        class Widget(AUniqORMBase):
            id: int = PrimaryKey()
            label: str

        await ready(db, Widget)
        w = Widget(label="test")
        await w.insert()

        found = await Widget.get(label="test")
        assert found is not None

    async def test_on_conflict_none_default(self, db):
        """Ensure on_conflict=None (default) works normally."""

        class Gizmo(AUniqORMBase):
            id: int = PrimaryKey()
            name: str

        await ready(db, Gizmo)
        g = Gizmo(name="g1")
        await g.insert(on_conflict=None)

        assert await Gizmo.count() == 1


# ── bulk_insert ──────────────────────────────────────────────────────────────


class TestBulkInsert:
    async def test_bulk_insert_basic(self, db):
        class ItemBI(AUniqORMBase, table="items_bi"):
            id: int = PrimaryKey()
            project_id: int
            media_id: int

        await ready(db, ItemBI)
        instances = [ItemBI(project_id=1, media_id=m) for m in [10, 20, 30]]
        await ItemBI.bulk_insert(instances)

        all_rows = await ItemBI.filter(order_by="media_id")
        assert [r.media_id for r in all_rows] == [10, 20, 30]
        assert all(r.project_id == 1 for r in all_rows)

    async def test_bulk_insert_empty_list(self, db):
        class ItemBI2(AUniqORMBase, table="items_bi2"):
            id: int = PrimaryKey()
            val: int

        await ready(db, ItemBI2)
        await ItemBI2.bulk_insert([])  # should be a no-op
        assert await ItemBI2.count() == 0

    async def test_bulk_insert_single_item(self, db):
        class ItemBI3(AUniqORMBase, table="items_bi3"):
            id: int = PrimaryKey()
            name: str

        await ready(db, ItemBI3)
        await ItemBI3.bulk_insert([ItemBI3(name="solo")])
        assert await ItemBI3.count() == 1

    async def test_bulk_insert_with_strings(self, db):
        class TagBI(AUniqORMBase, table="tags_bi"):
            id: int = PrimaryKey()
            label: str

        await ready(db, TagBI)
        await TagBI.bulk_insert([TagBI(label="a"), TagBI(label="b"), TagBI(label="c")])

        rows = await TagBI.filter(order_by="label")
        assert [r.label for r in rows] == ["a", "b", "c"]

    async def test_bulk_insert_mixed_types(self, db):
        class RecBI(AUniqORMBase, table="recs_bi"):
            id: int = PrimaryKey()
            name: str
            score: float

        await ready(db, RecBI)
        await RecBI.bulk_insert(
            [
                RecBI(name="x", score=1.5),
                RecBI(name="y", score=2.7),
            ]
        )

        rows = await RecBI.filter(order_by="name")
        assert len(rows) == 2
        assert rows[0].name == "x" and rows[0].score == 1.5
        assert rows[1].name == "y" and rows[1].score == 2.7

    async def test_bulk_insert_chunked(self, db):
        # >500 rows spans multiple chunks (auto-wrapped in one transaction).
        class BigBI(AUniqORMBase, table="big_bi"):
            id: int = PrimaryKey()
            n: int

        await ready(db, BigBI)
        await BigBI.bulk_insert([BigBI(n=i) for i in range(1250)])
        assert await BigBI.count() == 1250
        rows = await BigBI.filter(order_by="n")
        assert rows[0].n == 0 and rows[-1].n == 1249

    async def test_bulk_insert_chunked_within_transaction(self, db):
        # Multi-chunk insert must route into an already-open transaction.
        class TxnBI(AUniqORMBase, table="txn_bi"):
            id: int = PrimaryKey()
            n: int

        await ready(db, TxnBI)
        async with await db.transaction():
            await TxnBI.bulk_insert([TxnBI(n=i) for i in range(1100)])
        assert await TxnBI.count() == 1100


# ── distinct ─────────────────────────────────────────────────────────────────


class TestDistinct:
    @pytest.mark.parametrize(
        "rows,kwargs,expected",
        [
            pytest.param(
                [{"label": x} for x in ["car", "person", "car", "bike"]],
                {},
                ["bike", "car", "person"],
                id="unique_sorted",
            ),
            pytest.param(
                [{"label": x} for x in ["a", "c", "b"]],
                {"order_by": "-label"},
                ["c", "b", "a"],
                id="order_desc",
            ),
            pytest.param([], {}, [], id="empty_table"),
        ],
    )
    async def test_distinct(self, db, rows, kwargs, expected, request):
        table = _safe_table("anno_dist", request)

        class AnnoDist(AUniqORMBase, table=table):
            id: int = PrimaryKey()
            label: str

        await ready(db, AnnoDist)
        for r in rows:
            await AnnoDist(**r).insert()

        assert await AnnoDist.distinct("label", **kwargs) == expected

    async def test_distinct_with_filter(self, db):
        class AnnoDist3(AUniqORMBase, table="anno_dist3"):
            id: int = PrimaryKey()
            media_id: int
            label: str

        await ready(db, AnnoDist3)
        await AnnoDist3(media_id=1, label="car").insert()
        await AnnoDist3(media_id=1, label="person").insert()
        await AnnoDist3(media_id=2, label="car").insert()
        await AnnoDist3(media_id=2, label="tree").insert()

        result = await AnnoDist3.distinct("label", media_id=1)
        assert result == ["car", "person"]

    async def test_distinct_invalid_column(self, db):
        class AnnoDist5(AUniqORMBase, table="anno_dist5"):
            id: int = PrimaryKey()
            label: str

        await ready(db, AnnoDist5)

        with pytest.raises(ValueError, match="not a field"):
            await AnnoDist5.distinct("nonexistent")


# ── Lookup expressions (__in, __isnull, __lt, __lte, __gt, __gte) ────────────


class TestLookupIn:
    @pytest.mark.parametrize(
        "values,expected",
        [
            pytest.param([2, 5, 7], [2, 5, 7], id="subset"),
            pytest.param([], [], id="empty_list"),
        ],
    )
    async def test_filter_in(self, db, values, expected, request):
        table = _safe_table("items_in", request)

        class Item(AUniqORMBase, table=table):
            id: int = PrimaryKey()
            val: int

        await ready(db, Item)
        for i in range(10):
            await Item(val=i).insert()

        results = await Item.filter(val__in=values, order_by="val")
        assert [r.val for r in results] == expected

    async def test_filter_in_with_other_conditions(self, db):
        class Item3(AUniqORMBase, table="items_in3"):
            id: int = PrimaryKey()
            grp: str
            val: int

        await ready(db, Item3)
        for i in range(6):
            await Item3(grp="a" if i % 2 == 0 else "b", val=i).insert()

        results = await Item3.filter(grp="a", val__in=[0, 2, 4], order_by="val")
        assert [r.val for r in results] == [0, 2, 4]

    async def test_count_with_in(self, db):
        class Item4(AUniqORMBase, table="items_in4"):
            id: int = PrimaryKey()
            val: int

        await ready(db, Item4)
        for i in range(10):
            await Item4(val=i).insert()

        assert await Item4.count(val__in=[1, 3, 5]) == 3

    async def test_exists_with_in(self, db):
        class Item5(AUniqORMBase, table="items_in5"):
            id: int = PrimaryKey()
            val: int

        await ready(db, Item5)
        await Item5(val=42).insert()

        assert await Item5.exists(val__in=[42, 99]) is True
        assert await Item5.exists(val__in=[99, 100]) is False

    async def test_in_type_error(self, db):
        class Item6(AUniqORMBase, table="items_in6"):
            id: int = PrimaryKey()
            val: int

        await ready(db, Item6)

        with pytest.raises(TypeError, match="requires a list/tuple/set"):
            await Item6.filter(val__in=42)


class TestLookupIsNull:
    @pytest.mark.parametrize(
        "isnull,expected_name",
        [
            pytest.param(True, None, id="true"),
            pytest.param(False, "hello", id="false"),
        ],
    )
    async def test_filter_isnull(self, db, isnull, expected_name, request):
        table = _safe_table("recs_null", request)

        class Rec(AUniqORMBase, table=table):
            id: int = PrimaryKey()
            name: str | None = Column(default=None)

        await ready(db, Rec)
        await Rec(name="hello").insert()
        await Rec(name=None).insert()

        results = await Rec.filter(name__isnull=isnull)
        assert len(results) == 1
        assert results[0].name == expected_name


class TestLookupComparisons:
    @pytest.mark.parametrize(
        "lookup,value,expected",
        [
            pytest.param("lt", 3, [0, 1, 2], id="lt"),
            pytest.param("lte", 3, [0, 1, 2, 3], id="lte"),
            pytest.param("gt", 2, [3, 4], id="gt"),
            pytest.param("gte", 2, [2, 3, 4], id="gte"),
        ],
    )
    async def test_filter(self, db, lookup, value, expected, request):
        table = _safe_table("cmp", request)

        class Num(AUniqORMBase, table=table):
            id: int = PrimaryKey()
            val: int

        await ready(db, Num)
        for i in range(5):
            await Num(val=i).insert()

        results = await Num.filter(**{f"val__{lookup}": value}, order_by="val")
        assert [r.val for r in results] == expected

    async def test_combined_lookups(self, db):
        class Num5(AUniqORMBase, table="nums_comb"):
            id: int = PrimaryKey()
            val: int

        await ready(db, Num5)
        for i in range(10):
            await Num5(val=i).insert()

        results = await Num5.filter(val__gte=3, val__lt=7, order_by="val")
        assert [r.val for r in results] == [3, 4, 5, 6]


# ── delete_where ─────────────────────────────────────────────────────────────


class TestDeleteWhere:
    async def test_delete_where_basic(self, db):
        class Task(AUniqORMBase, table="tasks_dw"):
            id: int = PrimaryKey()
            status: str

        await ready(db, Task)
        await Task(status="open").insert()
        await Task(status="open").insert()
        await Task(status="closed").insert()

        await Task.delete_where(status="open")
        assert await Task.count() == 1
        assert await Task.count(status="closed") == 1

    async def test_delete_where_with_in(self, db):
        class Item(AUniqORMBase, table="items_dw"):
            id: int = PrimaryKey()
            media_id: int
            collection_id: int

        await ready(db, Item)
        for mid in [1, 2, 3, 4, 5]:
            await Item(media_id=mid, collection_id=10).insert()

        await Item.delete_where(collection_id=10, media_id__in=[2, 4])
        remaining = await Item.filter(order_by="media_id")
        assert [r.media_id for r in remaining] == [1, 3, 5]

    async def test_delete_where_no_match(self, db):
        class Rec(AUniqORMBase, table="recs_dw"):
            id: int = PrimaryKey()
            val: int

        await ready(db, Rec)
        await Rec(val=1).insert()

        await Rec.delete_where(val=99)
        assert await Rec.count() == 1

    async def test_delete_where_no_kwargs_raises(self, db):
        class Rec2(AUniqORMBase, table="recs_dw2"):
            id: int = PrimaryKey()

        await ready(db, Rec2)

        with pytest.raises(ValueError, match="requires at least one filter"):
            await Rec2.delete_where()


# ── update_where ─────────────────────────────────────────────────────────────


class TestUpdateWhere:
    async def test_update_where_basic(self, db):
        class Task(AUniqORMBase, table="tasks_uw"):
            id: int = PrimaryKey()
            status: str

        await ready(db, Task)
        await Task(status="open").insert()
        await Task(status="open").insert()
        await Task(status="closed").insert()

        await Task.update_where({"status": "done"}, status="open")
        assert await Task.count(status="done") == 2
        assert await Task.count(status="closed") == 1

    async def test_update_where_with_in(self, db):
        class Item(AUniqORMBase, table="items_uw"):
            id: int = PrimaryKey()
            media_id: int
            has_updates: int

        await ready(db, Item)
        for mid in [1, 2, 3, 4, 5]:
            await Item(media_id=mid, has_updates=1).insert()

        await Item.update_where({"has_updates": 0}, media_id__in=[1, 3, 5])
        results = await Item.filter(has_updates=0, order_by="media_id")
        assert [r.media_id for r in results] == [1, 3, 5]

    async def test_update_where_with_raw_sql(self, db):
        class Counter(AUniqORMBase, table="counters_uw"):
            id: int = PrimaryKey()
            grp: str
            val: int

        await ready(db, Counter)
        await Counter(grp="a", val=10).insert()
        await Counter(grp="a", val=20).insert()
        await Counter(grp="b", val=30).insert()

        await Counter.update_where({"val": RawSQL("val + 1")}, grp="a")
        results = await Counter.filter(grp="a", order_by="val")
        assert [r.val for r in results] == [11, 21]

    async def test_update_where_no_kwargs_raises(self, db):
        class Rec(AUniqORMBase, table="recs_uw"):
            id: int = PrimaryKey()
            val: int

        await ready(db, Rec)

        with pytest.raises(ValueError, match="requires at least one filter"):
            await Rec.update_where({"val": 0})

    async def test_update_where_no_values_raises(self, db):
        class Rec2(AUniqORMBase, table="recs_uw2"):
            id: int = PrimaryKey()
            val: int

        await ready(db, Rec2)

        with pytest.raises(ValueError, match="requires at least one value"):
            await Rec2.update_where({}, val=1)


# ── Q objects ────────────────────────────────────────────────────────────────


class TestQ:
    """Tests for Q object OR / AND / nested queries."""

    async def test_filter_q_or(self, db):
        """Q with OR returns matching rows from either condition."""

        class Item(AUniqORMBase, table="q_items"):
            id: int = PrimaryKey()
            status: str
            priority: int

        await ready(db, Item)
        await Item(id=1, status="open", priority=1).insert()
        await Item(id=2, status="closed", priority=2).insert()
        await Item(id=3, status="pending", priority=3).insert()

        results = await Item.filter(
            Q(status="open") | Q(status="closed"), order_by="id"
        )
        assert len(results) == 2
        assert results[0].id == 1
        assert results[1].id == 2

    async def test_filter_q_and(self, db):
        """Q with AND (explicit) requires both conditions."""

        class Item(AUniqORMBase, table="q_items_and"):
            id: int = PrimaryKey()
            status: str
            priority: int

        await ready(db, Item)
        await Item(id=1, status="open", priority=1).insert()
        await Item(id=2, status="open", priority=2).insert()
        await Item(id=3, status="closed", priority=1).insert()

        results = await Item.filter(Q(status="open") & Q(priority=1))
        assert len(results) == 1
        assert results[0].id == 1

    async def test_filter_q_with_kwargs(self, db):
        """Q args combined with kwargs (AND-ed together)."""

        class Item(AUniqORMBase, table="q_items_kw"):
            id: int = PrimaryKey()
            status: str
            priority: int

        await ready(db, Item)
        await Item(id=1, status="open", priority=1).insert()
        await Item(id=2, status="closed", priority=1).insert()
        await Item(id=3, status="open", priority=2).insert()

        # OR on status, AND with priority=1
        results = await Item.filter(Q(status="open") | Q(status="closed"), priority=1)
        assert len(results) == 2
        ids = {r.id for r in results}
        assert ids == {1, 2}

    async def test_filter_q_nested(self, db):
        """Nested Q: (A | B) & (C | D)."""

        class Item(AUniqORMBase, table="q_items_nested"):
            id: int = PrimaryKey()
            status: str
            priority: int

        await ready(db, Item)
        await Item(id=1, status="open", priority=1).insert()
        await Item(id=2, status="open", priority=2).insert()
        await Item(id=3, status="closed", priority=1).insert()
        await Item(id=4, status="closed", priority=2).insert()

        results = await Item.filter(
            (Q(status="open") | Q(status="closed")) & (Q(priority=1) | Q(priority=2)),
            order_by="id",
        )
        assert len(results) == 4

        # More selective nesting
        results = await Item.filter(
            (Q(status="open") | Q(status="closed")) & Q(priority=1),
            order_by="id",
        )
        assert len(results) == 2
        assert results[0].id == 1
        assert results[1].id == 3

    async def test_filter_q_isnull_or(self, db):
        """Q with __isnull lookup in OR (the motivating use case)."""

        class Anno(AUniqORMBase, table="q_annos"):
            id: int = PrimaryKey()
            annotation_id: str | None = Column(default=None)
            media_id: int

        await ready(db, Anno)
        await Anno(id=1, annotation_id="", media_id=10).insert()
        await Anno(id=2, annotation_id=None, media_id=10).insert()
        await Anno(id=3, annotation_id="abc", media_id=10).insert()
        await Anno(id=4, annotation_id="", media_id=20).insert()

        results = await Anno.filter(
            Q(annotation_id="") | Q(annotation_id__isnull=True),
            media_id=10,
            order_by="id",
        )
        assert len(results) == 2
        assert results[0].id == 1
        assert results[1].id == 2

    async def test_filter_q_in_lookup(self, db):
        """Q with __in lookup."""

        class Item(AUniqORMBase, table="q_items_in"):
            id: int = PrimaryKey()
            status: str

        await ready(db, Item)
        await Item(id=1, status="open").insert()
        await Item(id=2, status="closed").insert()
        await Item(id=3, status="pending").insert()

        results = await Item.filter(Q(status__in=["open", "pending"]))
        assert len(results) == 2
        ids = {r.id for r in results}
        assert ids == {1, 3}

    async def test_count_with_q(self, db):
        """count() accepts Q args."""

        class Item(AUniqORMBase, table="q_count"):
            id: int = PrimaryKey()
            status: str

        await ready(db, Item)
        await Item(id=1, status="open").insert()
        await Item(id=2, status="closed").insert()
        await Item(id=3, status="pending").insert()

        n = await Item.count(Q(status="open") | Q(status="pending"))
        assert n == 2

    async def test_exists_with_q(self, db):
        """exists() accepts Q args."""

        class Item(AUniqORMBase, table="q_exists"):
            id: int = PrimaryKey()
            status: str

        await ready(db, Item)
        await Item(id=1, status="open").insert()

        assert await Item.exists(Q(status="open") | Q(status="closed"))
        assert not await Item.exists(Q(status="closed") | Q(status="pending"))

    async def test_delete_where_with_q(self, db):
        """delete_where() accepts Q args."""

        class Item(AUniqORMBase, table="q_del"):
            id: int = PrimaryKey()
            status: str

        await ready(db, Item)
        await Item(id=1, status="open").insert()
        await Item(id=2, status="closed").insert()
        await Item(id=3, status="pending").insert()

        await Item.delete_where(Q(status="open") | Q(status="closed"))
        remaining = await Item.all()
        assert len(remaining) == 1
        assert remaining[0].status == "pending"

    async def test_update_where_with_q(self, db):
        """update_where() accepts Q args."""

        class Item(AUniqORMBase, table="q_upd"):
            id: int = PrimaryKey()
            status: str
            priority: int

        await ready(db, Item)
        await Item(id=1, status="open", priority=1).insert()
        await Item(id=2, status="closed", priority=2).insert()
        await Item(id=3, status="pending", priority=3).insert()

        await Item.update_where({"priority": 99}, Q(status="open") | Q(status="closed"))
        items = await Item.filter(order_by="id")
        assert items[0].priority == 99
        assert items[1].priority == 99
        assert items[2].priority == 3

    async def test_q_no_conditions_raises(self, db):
        """delete_where / update_where with no args raises ValueError."""

        class Item(AUniqORMBase, table="q_noargs"):
            id: int = PrimaryKey()
            val: int

        await ready(db, Item)

        with pytest.raises(ValueError, match="requires at least one filter"):
            await Item.delete_where()

        with pytest.raises(ValueError, match="requires at least one filter"):
            await Item.update_where({"val": 0})

    async def test_q_repr(self, db):
        """Q __repr__ covers both kwargs and children paths."""
        q1 = Q(status="open")
        assert "status" in repr(q1)

        q2 = Q(status="open") | Q(priority=1)
        assert "OR" in repr(q2)

    async def test_q_multi_kwargs_in_single_q(self, db):
        """Single Q with multiple kwargs produces AND-ed conditions (line 842)."""

        class Item(AUniqORMBase, table="q_multi_kw"):
            id: int = PrimaryKey()
            status: str
            priority: int

        await ready(db, Item)
        await Item(id=1, status="open", priority=1).insert()
        await Item(id=2, status="open", priority=2).insert()
        await Item(id=3, status="closed", priority=1).insert()

        results = await Item.filter(Q(status="open", priority=1))
        assert len(results) == 1
        assert results[0].id == 1

    async def test_q_empty_q(self, db):
        """Empty Q() produces 1=1 (line 852), effectively no filter."""

        class Item(AUniqORMBase, table="q_empty"):
            id: int = PrimaryKey()
            val: int

        await ready(db, Item)
        await Item(id=1, val=10).insert()
        await Item(id=2, val=20).insert()

        # Q() with no kwargs — should match all
        results = await Item.filter(Q())
        assert len(results) == 2


# ── __range lookup ───────────────────────────────────────────────────────────


class TestLookupRange:
    async def test_filter_range(self, db):
        class ItemRng(AUniqORMBase, table="items_rng"):
            id: int = PrimaryKey()
            val: int

        await ready(db, ItemRng)
        for i in range(10):
            await ItemRng(val=i).insert()

        results = await ItemRng.filter(val__range=[3, 7], order_by="val")
        assert [r.val for r in results] == [3, 4, 5, 6, 7]

    @pytest.mark.parametrize(
        "bad_value",
        [
            pytest.param(5, id="not_sequence"),
            pytest.param([1, 2, 3], id="wrong_length"),
        ],
    )
    async def test_range_invalid(self, db, bad_value, request):
        table = _safe_table("items_rng_err", request)

        class M(AUniqORMBase, table=table):
            id: int = PrimaryKey()
            val: int

        await ready(db, M)

        with pytest.raises(TypeError, match="2-element"):
            await M.filter(val__range=bad_value)
