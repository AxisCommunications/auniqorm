# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for CRUD operations, bulk_insert, count, exists, distinct, RawSQL, repr."""

import pytest

from auniqorm.columns import Column, PrimaryKey
from auniqorm.connection import Database
from auniqorm.exceptions import ConnectionError
from auniqorm.models import AUniqORMBase, ModelRegistry, RawSQL
from auniqorm.schema import DDLGenerator

# ---------------------------------------------------------------------------
# RawSQL
# ---------------------------------------------------------------------------


class TestRawSQL:
    def test_repr(self):
        r = RawSQL("NOW()")
        assert repr(r) == "RawSQL('NOW()')"

    def test_stores_expr(self):
        r = RawSQL("1 + 1")
        assert r.expr == "1 + 1"


# ---------------------------------------------------------------------------
# __repr__
# ---------------------------------------------------------------------------


class TestModelRepr:
    def test_repr_omits_defaults(self):
        class R(AUniqORMBase, table="reprs"):
            id: int = PrimaryKey()
            name: str

        r = R(id=1, name="hi")
        text = repr(r)
        assert "R:" in text
        assert "id" in text
        assert "name" in text

    def test_repr_with_only_pk(self):
        class R2(AUniqORMBase, table="repr2s"):
            id: int = PrimaryKey()
            opt: str | None

        r = R2(id=5)
        text = repr(r)
        assert "R2:" in text
        assert "id" in text

    def test_repr_renders_pep604_union_annotation(self):
        class R3(AUniqORMBase, table="repr3s"):
            id: int = PrimaryKey()
            name: str | None = None
            tags: list[int] | None = None

        r = R3(id=1, name="hi", tags=[1, 2])
        text = repr(r)
        # Union annotations must render as their actual type spelling, not
        # the generic "Union" wrapper name.
        assert "Union" not in text, text
        assert "str | None" in text, text
        assert "list[int] | None" in text, text

    def test_repr_renders_generic_alias(self):
        class R4(AUniqORMBase, table="repr4s"):
            id: int = PrimaryKey()
            mapping: dict[str, int] = {}

        r = R4(id=1, mapping={"a": 1})
        text = repr(r)
        assert "dict[str, int]" in text, text


# ---------------------------------------------------------------------------
# insert + get + filter + all (SQLite)
# ---------------------------------------------------------------------------


class TestInsertAndQuery:
    async def test_insert_and_get(self, db):
        class Note(AUniqORMBase):
            id: int = PrimaryKey()
            body: str

        await Note.create_table()
        Note.__database__ = db
        n = Note(body="hello")
        await n.insert()
        found = await Note.get(body="hello")
        assert found is not None
        assert found.body == "hello"

    async def test_filter_no_kwargs_returns_all(self, db):
        class Num(AUniqORMBase):
            id: int = PrimaryKey()
            val: int

        await Num.create_table()
        Num.__database__ = db
        await Num(val=1).insert()
        await Num(val=2).insert()
        result = await Num.filter()
        assert len(result) == 2

    async def test_filter_with_order_by(self, db):
        class Ord(AUniqORMBase):
            id: int = PrimaryKey()
            priority: int

        await Ord.create_table()
        Ord.__database__ = db
        await Ord(priority=3).insert()
        await Ord(priority=1).insert()
        await Ord(priority=2).insert()

        asc = await Ord.filter(order_by="priority")
        assert [o.priority for o in asc] == [1, 2, 3]

        desc = await Ord.filter(order_by="-priority")
        assert [o.priority for o in desc] == [3, 2, 1]

    async def test_filter_with_limit_offset(self, db):
        class Pg(AUniqORMBase):
            id: int = PrimaryKey()
            n: int

        await Pg.create_table()
        Pg.__database__ = db
        for i in range(5):
            await Pg(n=i).insert()

        page = await Pg.filter(order_by="n", limit=2, offset=1)
        assert len(page) == 2
        assert [p.n for p in page] == [1, 2]

    async def test_filter_with_multiple_order_by(self, db):
        class Mo(AUniqORMBase):
            id: int = PrimaryKey()
            a: int
            b: int

        await Mo.create_table()
        Mo.__database__ = db
        await Mo(a=1, b=2).insert()
        await Mo(a=1, b=1).insert()
        await Mo(a=2, b=1).insert()

        result = await Mo.filter(order_by=["a", "-b"])
        assert [(r.a, r.b) for r in result] == [(1, 2), (1, 1), (2, 1)]


# ---------------------------------------------------------------------------
# bulk_insert
# ---------------------------------------------------------------------------


class TestBulkInsert:
    async def test_bulk_insert_sqlite(self, db):
        class Row(AUniqORMBase):
            id: int = PrimaryKey()
            val: str

        await Row.create_table()
        Row.__database__ = db

        items = [Row(val="a"), Row(val="b"), Row(val="c")]
        await Row.bulk_insert(items)

        all_rows = await Row.all()
        assert len(all_rows) == 3

    async def test_bulk_insert_empty_list(self, db):
        class Empty(AUniqORMBase, table="empties"):
            id: int = PrimaryKey()

        await Empty.create_table()
        # Should be a no-op
        await Empty.bulk_insert([])

    async def test_bulk_insert_json_list_columns(self, monkeypatch):
        # Regression: asyncpg treats a Python list inside a ``jsonb[]`` unnest as
        # nested array dimensions, which corrupts JSON list payloads (empty
        # lists collapse to SQL NULL, non-empty lists lose their wrapper). JSON
        # columns must therefore be serialised to text and cast back to JSON(B)
        # so each value is passed to asyncpg as an opaque scalar.
        from unittest.mock import AsyncMock, MagicMock, PropertyMock

        db = MagicMock()
        type(db).protocol = PropertyMock(return_value="postgresql")
        db.execute = AsyncMock(return_value=None)
        monkeypatch.setattr(ModelRegistry, "get_database", classmethod(lambda cls: db))

        class Doc(AUniqORMBase, table="jdocs"):
            id: int = PrimaryKey()
            tags: list = Column(db_type="JSONB")
            attrs: dict = Column(db_type="JSONB")

        Doc.__database__ = db

        await Doc.bulk_insert(
            [
                Doc(tags=[], attrs={}),
                Doc(tags=["curated"], attrs={"k": 1}),
            ]
        )

        sql, params = db.execute.call_args[0]
        # JSON columns are passed as ``text[]`` and cast back to JSONB, never as
        # a raw ``jsonb[]`` (which would let asyncpg mangle the list payloads).
        assert "::text[]" in sql
        assert "::JSONB" in sql
        assert "jsonb[]" not in sql.lower()
        # The ``tags`` list column is serialised to JSON text, so the empty list
        # survives as ``"[]"`` rather than collapsing to NULL.
        assert ["[]", '["curated"]'] in params
        assert ["{}", '{"k":1}'] in params

    async def test_json_roundtrip_insert(self, sqlite_db):
        # JSON containers must survive a real write/read round trip on backends
        # without a native JSON codec (SQLite/MySQL): serialised to text on
        # write, decoded back to list/dict on read.
        class JDoc(AUniqORMBase, table="jdocs_rt"):
            id: int = PrimaryKey()
            tags: list
            attrs: dict
            meta: dict | None = None

        await JDoc.create_table()

        await JDoc(tags=["a", "b"], attrs={"k": 1, "nested": [1, 2]}).insert()
        await JDoc(tags=[], attrs={}, meta={"x": True}).insert()

        rows = await JDoc.filter(order_by="id")
        assert [r.tags for r in rows] == [["a", "b"], []]
        assert [r.attrs for r in rows] == [{"k": 1, "nested": [1, 2]}, {}]
        assert [r.meta for r in rows] == [None, {"x": True}]

    async def test_json_roundtrip_bulk_insert(self, db):
        # Same round trip via bulk_insert, exercised across all three dialects
        # (PostgreSQL uses the text[]/JSONB-cast path; SQLite/MySQL serialise
        # each value to text inline).
        class JBulk(AUniqORMBase, table="jbulk_rt"):
            id: int = PrimaryKey()
            tags: list
            attrs: dict

        await JBulk.create_table()
        JBulk.__database__ = db

        await JBulk.bulk_insert(
            [
                JBulk(tags=[], attrs={}),
                JBulk(tags=["curated", "x"], attrs={"k": 1}),
            ]
        )

        rows = await JBulk.filter(order_by="id")
        assert [r.tags for r in rows] == [[], ["curated", "x"]]
        assert [r.attrs for r in rows] == [{}, {"k": 1}]


# ---------------------------------------------------------------------------
# bulk_update
# ---------------------------------------------------------------------------


class TestBulkUpdate:
    async def test_bulk_update_sqlite(self, db):
        class Item(AUniqORMBase, table="items_bu"):
            id: int = PrimaryKey()
            name: str
            score: int = Column(default=0)

        await Item.create_table()
        Item.__database__ = db

        a = await Item(name="a", score=1).insert()
        b = await Item(name="b", score=2).insert()
        c = await Item(name="c", score=3).insert()

        # Mutate locally and bulk_update
        a.score = 10
        b.score = 20
        c.score = 30

        await Item.bulk_update([a, b, c], fields=["score"])

        refreshed = await Item.filter(order_by="id")
        assert [r.score for r in refreshed] == [10, 20, 30]
        # Names unchanged
        assert [r.name for r in refreshed] == ["a", "b", "c"]

    async def test_bulk_update_multiple_fields(self, db):
        class Multi(AUniqORMBase, table="multis_bu"):
            id: int = PrimaryKey()
            name: str
            value: int = Column(default=0)

        await Multi.create_table()
        Multi.__database__ = db

        x = await Multi(name="x", value=1).insert()
        y = await Multi(name="y", value=2).insert()

        x.name = "xx"
        x.value = 11
        y.name = "yy"
        y.value = 22

        await Multi.bulk_update([x, y], fields=["name", "value"])

        rows = await Multi.filter(order_by="id")
        assert [(r.name, r.value) for r in rows] == [("xx", 11), ("yy", 22)]

    async def test_bulk_update_empty_list(self, db):
        class Noop(AUniqORMBase, table="noops_bu"):
            id: int = PrimaryKey()
            val: str

        await Noop.create_table()
        # Should be a no-op
        await Noop.bulk_update([], fields=["val"])

    async def test_bulk_update_empty_fields(self, db):
        class Noop2(AUniqORMBase, table="noops2_bu"):
            id: int = PrimaryKey()
            val: str

        await Noop2.create_table()
        Noop2.__database__ = db
        item = await Noop2(val="hi").insert()
        # Empty fields list → no-op
        await Noop2.bulk_update([item], fields=[])


# ---------------------------------------------------------------------------
# count
# ---------------------------------------------------------------------------


class TestCount:
    @pytest.mark.parametrize(
        "statuses,filter_kwargs,expected",
        [
            pytest.param(["a", "b", "a"], {}, 3, id="all"),
            pytest.param(["a", "b", "a"], {"status": "a"}, 2, id="filtered"),
            pytest.param([], {}, 0, id="empty"),
        ],
    )
    async def test_count(self, db, statuses, filter_kwargs, expected, request):
        table = f"ct_{request.node.callspec.id}".replace("-", "_")

        class Ct(AUniqORMBase, table=table):
            id: int = PrimaryKey()
            status: str | None = Column(default=None)

        await Ct.create_table()
        Ct.__database__ = db
        for s in statuses:
            await Ct(status=s).insert()

        assert await Ct.count(**filter_kwargs) == expected


# ---------------------------------------------------------------------------
# exists
# ---------------------------------------------------------------------------


class TestExists:
    @pytest.mark.parametrize(
        "insert_name,filter_kwargs,expected",
        [
            pytest.param("yes", {"name": "yes"}, True, id="true"),
            pytest.param(None, {"name": "nope"}, False, id="false"),
        ],
    )
    async def test_exists(self, db, insert_name, filter_kwargs, expected, request):
        table = f"ex_{request.node.callspec.id}".replace("-", "_")

        class Ex(AUniqORMBase, table=table):
            id: int = PrimaryKey()
            name: str | None = Column(default=None)

        await Ex.create_table()
        Ex.__database__ = db
        if insert_name is not None:
            await Ex(name=insert_name).insert()
        assert await Ex.exists(**filter_kwargs) is expected

    async def test_exists_no_filter(self, db):
        class Ex3(AUniqORMBase):
            id: int = PrimaryKey()

        await Ex3.create_table()
        Ex3.__database__ = db
        assert await Ex3.exists() is False
        await Ex3(id=1).insert()
        assert await Ex3.exists() is True


# ---------------------------------------------------------------------------
# distinct
# ---------------------------------------------------------------------------


class TestDistinct:
    async def test_distinct_values(self, db):
        class Di(AUniqORMBase):
            id: int = PrimaryKey()
            color: str

        await Di.create_table()
        Di.__database__ = db
        await Di(color="red").insert()
        await Di(color="blue").insert()
        await Di(color="red").insert()

        colors = await Di.distinct("color")
        assert set(colors) == {"red", "blue"}

    async def test_distinct_with_filter(self, db):
        class Di2(AUniqORMBase):
            id: int = PrimaryKey()
            grp: str
            label: str

        await Di2.create_table()
        Di2.__database__ = db
        await Di2(grp="a", label="x").insert()
        await Di2(grp="a", label="y").insert()
        await Di2(grp="b", label="x").insert()

        labels = await Di2.distinct("label", grp="a")
        assert set(labels) == {"x", "y"}

    async def test_distinct_desc_order(self, db):
        class Di3(AUniqORMBase):
            id: int = PrimaryKey()
            n: int

        await Di3.create_table()
        Di3.__database__ = db
        await Di3(n=1).insert()
        await Di3(n=3).insert()
        await Di3(n=2).insert()

        vals = await Di3.distinct("n", order_by="-n")
        assert vals == [3, 2, 1]

    async def test_distinct_invalid_column_raises(self, db):
        class Di4(AUniqORMBase):
            id: int = PrimaryKey()
            name: str

        await Di4.create_table()
        Di4.__database__ = db

        with pytest.raises(ValueError, match="not a field"):
            await Di4.distinct("nonexistent")


class TestInsertReturnsSelf:
    async def test_insert_returns_self_with_pk(self, db):
        class Ret(AUniqORMBase):
            id: int = PrimaryKey()
            name: str

        await Ret.create_table()
        Ret.__database__ = db
        r = Ret(name="hello")
        result = await r.insert()
        assert result is r
        assert r.id is not None
        assert isinstance(r.id, int)
        assert r.name == "hello"

    async def test_insert_returns_self_identity(self, db):
        class Ret2(AUniqORMBase):
            id: int = PrimaryKey()
            val: int

        await Ret2.create_table()
        Ret2.__database__ = db
        r = Ret2(val=42)
        result = await r.insert()
        assert result is r
        assert r.val == 42


# ---------------------------------------------------------------------------
# update + delete
# ---------------------------------------------------------------------------


class TestUpdateDelete:
    async def test_update(self, db):
        class Up(AUniqORMBase):
            id: int = PrimaryKey()
            val: str

        await Up.create_table()
        Up.__database__ = db
        u = Up(val="old")
        await u.insert()

        # Re-fetch to get the auto-generated id
        u = await Up.get(val="old")
        await u.update(val="new")
        assert u.val == "new"

        refreshed = await Up.get(id=u.id)
        assert refreshed.val == "new"

    async def test_update_with_raw_sql(self, db):
        class Counter(AUniqORMBase):
            id: int = PrimaryKey()
            n: int

        await Counter.create_table()
        Counter.__database__ = db
        c = Counter(n=5)
        await c.insert()

        c = await Counter.get(n=5)
        await c.update(n=RawSQL("n + 10"))
        refreshed = await Counter.get(id=c.id)
        assert refreshed.n == 15

    async def test_delete(self, db):
        class Del(AUniqORMBase):
            id: int = PrimaryKey()
            name: str

        await Del.create_table()
        Del.__database__ = db
        d = Del(name="gone")
        await d.insert()

        d = await Del.get(name="gone")
        await d.delete()
        assert await Del.get(id=d.id) is None


# ---------------------------------------------------------------------------
# Error guards: not bound
# ---------------------------------------------------------------------------


class TestNotBoundErrors:
    @pytest.mark.parametrize(
        "method,args",
        [
            pytest.param("get", {"id": 1}, id="get"),
            pytest.param("filter", {}, id="filter"),
            pytest.param("count", {}, id="count"),
            pytest.param("exists", {}, id="exists"),
        ],
    )
    async def test_classmethod_not_bound(self, method, args, request):
        table = f"unb_{request.node.callspec.id}".replace("-", "_")

        class Unb(AUniqORMBase, table=table):
            id: int = PrimaryKey()

        Unb.__database__ = None
        with pytest.raises(RuntimeError, match="not bound"):
            await getattr(Unb, method)(**args)

    async def test_distinct_not_bound(self):
        class Unb5(AUniqORMBase):
            id: int = PrimaryKey()
            col: str

        Unb5.__database__ = None
        with pytest.raises(RuntimeError, match="not bound"):
            await Unb5.distinct("col")

    async def test_update_not_bound(self):
        class Unb6(AUniqORMBase):
            id: int = PrimaryKey()

        ModelRegistry._database = None
        with pytest.raises(RuntimeError, match="not bound"):
            inst = Unb6(id=1)
            await inst.update(id=2)

    async def test_delete_not_bound(self):
        class Unb7(AUniqORMBase):
            id: int = PrimaryKey()

        ModelRegistry._database = None
        with pytest.raises(RuntimeError, match="not bound"):
            inst = Unb7(id=1)
            await inst.delete()

    async def test_preamble_not_bound(self):
        class Unb8(AUniqORMBase):
            id: int = PrimaryKey()

        with pytest.raises(RuntimeError, match="not bound"):
            Unb8.preamble()

    @pytest.mark.parametrize("method", ["create_all", "drop_all"])
    async def test_registry_not_bound(self, method):
        ModelRegistry._database = None
        with pytest.raises(RuntimeError, match="No database bound"):
            await getattr(ModelRegistry, method)()


# ---------------------------------------------------------------------------
# Connection not-connected guards
# ---------------------------------------------------------------------------


class TestConnectionNotConnectedGuards:
    @pytest.mark.parametrize(
        ("method", "args"),
        [
            ("execute_many", ("SELECT 1", [])),
            ("fetch_one", ("SELECT 1",)),
            ("fetch_all", ("SELECT 1",)),
            ("fetch_val", ("SELECT 1",)),
            ("transaction", ()),
        ],
    )
    async def test_not_connected(self, method, args):
        db = Database("sqlite://:memory:")
        with pytest.raises(ConnectionError):
            await getattr(db, method)(*args)


# ---------------------------------------------------------------------------
# Transaction
# ---------------------------------------------------------------------------


class TestTransaction:
    async def test_transaction_commit(self, sqlite_db):
        await sqlite_db.execute("CREATE TABLE txn (id INTEGER PRIMARY KEY, val TEXT)")
        async with await sqlite_db.transaction():
            await sqlite_db.execute("INSERT INTO txn (val) VALUES (?)", ["committed"])
        row = await sqlite_db.fetch_one("SELECT val FROM txn")
        assert row["val"] == "committed"

    async def test_transaction_rollback_on_error(self, sqlite_db):
        await sqlite_db.execute(
            "CREATE TABLE txn2 (id INTEGER PRIMARY KEY, val TEXT NOT NULL)"
        )
        with pytest.raises(Exception):
            async with await sqlite_db.transaction():
                await sqlite_db.execute("INSERT INTO txn2 (val) VALUES (?)", ["ok"])
                await sqlite_db.execute("INSERT INTO txn2 (val) VALUES (?)", [None])
        rows = await sqlite_db.fetch_all("SELECT * FROM txn2")
        assert len(rows) == 0


# ---------------------------------------------------------------------------
# DDL: mysql dialect paths
# ---------------------------------------------------------------------------


class TestMySQLDDLPaths:
    def test_mysql_create_table(self):
        from auniqorm.columns import Column, PrimaryKey
        from auniqorm.schema import DDLGenerator

        class FakeModel:
            pass

        FakeModel.__table_name__ = "things"
        FakeModel.__annotations__ = {"id": int, "name": str}
        FakeModel.__struct_defaults__ = [PrimaryKey(), Column(nullable=False)]
        FakeModel.__struct_fields__ = ("id", "name")

        ddl = DDLGenerator("mysql")
        sql = ddl.create_table(FakeModel)
        assert "PRIMARY KEY AUTO_INCREMENT" in sql
        assert "name TEXT NOT NULL" in sql

    def test_mysql_introspect(self):
        from auniqorm.schema import DDLGenerator

        ddl = DDLGenerator("mysql")
        sql, params = ddl.introspect_columns_sql("test")
        assert "%s" in sql
        assert params == ["test"]

    def test_mysql_parse_column_names(self):
        from auniqorm.schema import DDLGenerator

        ddl = DDLGenerator("mysql")
        rows = [{"column_name": "id"}, {"column_name": "name"}]
        assert ddl.parse_column_names(rows) == {"id", "name"}


# ---------------------------------------------------------------------------
# DDL: drop table without IF EXISTS
# ---------------------------------------------------------------------------


class TestDropTable:
    def test_drop_without_if_exists(self):
        class FakeModel:
            __table_name__ = "t"

        ddl = DDLGenerator("sqlite")
        sql = ddl.drop_table(FakeModel, if_exists=False)
        assert sql == "DROP TABLE t"
        assert "IF EXISTS" not in sql


# ---------------------------------------------------------------------------
# Metaclass validation
# ---------------------------------------------------------------------------


class TestMetaclassValidation:
    def test_bare_annotation_gets_column_auto_injected(self):
        """Field with annotation but no default gets Column() injected."""

        class AutoCol(AUniqORMBase):
            id: int = PrimaryKey()
            required: int
            optional: str | None

        req_meta = AutoCol.__struct_defaults__[1]
        opt_meta = AutoCol.__struct_defaults__[2]
        assert isinstance(req_meta, Column)
        assert req_meta.nullable is False
        assert isinstance(opt_meta, Column)
        assert opt_meta.nullable is True

    def test_bad_server_default_type_raises(self):
        with pytest.raises(TypeError, match="invalid server_default"):

            class Bad2(AUniqORMBase):
                id: int = PrimaryKey()
                ts: str = Column(server_default=123)

    def test_callable_default_becomes_factory(self):
        class WithFactory(AUniqORMBase, table="with_factories"):
            id: int = PrimaryKey()
            tags: list = list

        meta = WithFactory.__struct_defaults__[1]
        assert isinstance(meta, Column)
        assert meta.default_factory is list

    def test_lambda_default_becomes_factory(self):
        fn = lambda: []  # noqa: E731

        class WithLambda(AUniqORMBase):
            id: int = PrimaryKey()
            items: list = fn

        meta = WithLambda.__struct_defaults__[1]
        assert isinstance(meta, Column)
        assert meta.default_factory is fn

    def test_string_with_parens_becomes_server_default(self):
        class WithSD(AUniqORMBase):
            id: int = PrimaryKey()
            ts: str = "now()"

        meta = WithSD.__struct_defaults__[1]
        assert isinstance(meta, Column)
        assert meta.server_default == "now()"

    def test_plain_string_becomes_default(self):
        class WithStr(AUniqORMBase):
            id: int = PrimaryKey()
            status: str = "active"

        meta = WithStr.__struct_defaults__[1]
        assert isinstance(meta, Column)
        assert meta.default == "active"

    def test_plain_int_becomes_default(self):
        class WithInt(AUniqORMBase):
            id: int = PrimaryKey()
            count: int = 0

        meta = WithInt.__struct_defaults__[1]
        assert isinstance(meta, Column)
        assert meta.default == 0


# ---------------------------------------------------------------------------
# create_all failure path
# ---------------------------------------------------------------------------


class TestCreateAllFailure:
    async def test_create_all_unresolvable_raises(self, db):
        """create_all raises when a model always fails to create."""
        from unittest.mock import AsyncMock, patch

        class Broken(AUniqORMBase):
            id: int = PrimaryKey()

        ModelRegistry.bind(db)

        with patch.object(
            Broken,
            "create_table",
            new_callable=AsyncMock,
            side_effect=RuntimeError("boom"),
        ):
            with pytest.raises(RuntimeError, match="Could not create tables"):
                await ModelRegistry.create_all()


# ---------------------------------------------------------------------------
# DDL edge cases
# ---------------------------------------------------------------------------


class TestDDLEdgeCases:
    def _make_model(self, table_name, annotations, defaults):
        class FakeModel:
            pass

        FakeModel.__table_name__ = table_name
        FakeModel.__annotations__ = annotations
        FakeModel.__struct_defaults__ = defaults
        FakeModel.__struct_fields__ = tuple(annotations.keys())
        return FakeModel

    def test_bigint_becomes_bigserial_postgresql(self):
        ddl = DDLGenerator("postgresql")
        model = self._make_model(
            "big",
            {"id": int},
            [PrimaryKey(db_type="BIGINT")],
        )
        sql = ddl.create_table(model)
        assert "BIGSERIAL PRIMARY KEY" in sql

    def test_foreign_key_on_update(self):
        ddl = DDLGenerator("postgresql")
        model = self._make_model(
            "refs",
            {"id": int, "parent_id": int},
            [
                PrimaryKey(),
                Column(
                    foreign_key="parents.id", on_delete="CASCADE", on_update="SET NULL"
                ),
            ],
        )
        sql = ddl.create_table(model)
        assert "ON UPDATE SET NULL" in sql
        assert "ON DELETE CASCADE" in sql

    def test_column_definition_without_column_meta(self):
        ddl = DDLGenerator("postgresql")
        col_def = ddl._column_definition("name", str, None)
        assert col_def == "name TEXT NOT NULL"

    def test_column_definition_optional_without_meta(self):
        ddl = DDLGenerator("postgresql")
        col_def = ddl._column_definition("name", str | None, None)
        assert col_def == "name TEXT"
        assert "NOT NULL" not in col_def

    def test_introspect_unsupported_dialect_raises(self):
        ddl = DDLGenerator("oracle")
        with pytest.raises(ValueError, match="Unsupported dialect"):
            ddl.introspect_columns_sql("t")


# ---------------------------------------------------------------------------
# SQLite URL parsing
# ---------------------------------------------------------------------------


class TestSQLiteURLParsing:
    @pytest.mark.parametrize(
        ("url", "expected"),
        [
            pytest.param("sqlite:///tmp/db.sqlite", "tmp/db.sqlite", id="absolute"),
            pytest.param("sqlite://./data/db.sqlite", "data/db.sqlite", id="relative"),
            pytest.param("sqlite://:memory:", ":memory:", id="memory"),
        ],
    )
    def test_db_path(self, url, expected):
        from auniqorm.drivers.sqlite import SQLiteDriver

        assert SQLiteDriver(url)._get_db_path() == expected


# ---------------------------------------------------------------------------
# Connection failed-connect path
# ---------------------------------------------------------------------------


class TestConnectionFailedConnect:
    async def test_connect_invalid_path_raises(self):
        db = Database("sqlite:///nonexistent/deep/path/db.sqlite")
        with pytest.raises(ConnectionError, match="Failed to connect"):
            await db.connect()


# ---------------------------------------------------------------------------
# keys() / __getitem__() for dict unpacking
# ---------------------------------------------------------------------------


class TestDictUnpacking:
    async def test_keys_returns_field_names(self, db):
        class DU(AUniqORMBase, table="du_keys"):
            id: int = PrimaryKey()
            name: str

        await DU.create_table()
        DU.__database__ = db

        inst = await DU(name="test").insert()
        assert "id" in inst.keys()
        assert "name" in inst.keys()

    async def test_getitem_returns_value(self, db):
        class DU2(AUniqORMBase, table="du_getitem"):
            id: int = PrimaryKey()
            name: str

        await DU2.create_table()
        DU2.__database__ = db

        inst = await DU2(name="hello").insert()
        assert inst["name"] == "hello"
        assert inst["id"] is not None

    async def test_dict_star_unpacking(self, db):
        class DU3(AUniqORMBase, table="du_unpack"):
            id: int = PrimaryKey()
            name: str

        await DU3.create_table()
        DU3.__database__ = db

        inst = await DU3(name="world").insert()
        d = dict(**inst)
        assert d["name"] == "world"


# ---------------------------------------------------------------------------
# update/delete validation errors
# ---------------------------------------------------------------------------


class TestUpdateDeleteValidation:
    async def test_update_no_pk_value_raises(self, db):
        class NoPKVal(AUniqORMBase, table="no_pk_upd"):
            id: int = PrimaryKey()
            name: str

        await NoPKVal.create_table()
        NoPKVal.__database__ = db

        inst = NoPKVal(name="test")
        # Force pk to None to trigger the validation
        inst.id = None
        with pytest.raises(ValueError, match="Cannot update"):
            await inst.update(name="new")

    async def test_delete_no_pk_value_raises(self, db):
        class NoPKDel(AUniqORMBase, table="no_pk_del"):
            id: int = PrimaryKey()
            name: str

        await NoPKDel.create_table()
        NoPKDel.__database__ = db

        inst = NoPKDel(name="test")
        inst.id = None
        with pytest.raises(ValueError, match="Cannot delete"):
            await inst.delete()


# ---------------------------------------------------------------------------
# ModelRegistry.drop_all and create_all failure path
# ---------------------------------------------------------------------------


class TestRegistryDropAll:
    async def test_drop_all(self, sqlite_db):
        class DropMe(AUniqORMBase, table="drop_me"):
            id: int = PrimaryKey()

        await DropMe.create_table()

        # Confirm table exists
        row = await sqlite_db.fetch_val(
            "SELECT count(*) FROM sqlite_master WHERE name='drop_me'"
        )
        assert row == 1

        await ModelRegistry.drop_all()

        row = await sqlite_db.fetch_val(
            "SELECT count(*) FROM sqlite_master WHERE name='drop_me'"
        )
        assert row == 0


class TestRegistryCreateAllFails:
    async def test_create_all_raises_on_failure(self, db):
        """A model that always fails create_table should trigger RuntimeError."""
        from unittest.mock import AsyncMock, patch

        class Bad(AUniqORMBase, table="bad_table"):
            id: int = PrimaryKey()

        Bad.__database__ = db
        # Make create_table always fail
        with patch.object(
            Bad, "create_table", new=AsyncMock(side_effect=RuntimeError("nope"))
        ):
            with pytest.raises(RuntimeError, match="Could not create tables"):
                await ModelRegistry.create_all()


# ---------------------------------------------------------------------------
# _build_conditions with PostgreSQL dialect (no real DB needed)
# ---------------------------------------------------------------------------


class TestBuildConditionsPostgresql:
    def _model(self):
        class M(AUniqORMBase, table="m"):
            id: int = PrimaryKey()
            name: str
            score: int = Column(default=0)

        return M

    def test_exact_pg(self):
        model = self._model()
        conds, vals, idx = model._build_conditions({"name": "x"}, "postgresql")
        assert conds == ["name = $1"]
        assert vals == ["x"]
        assert idx == 2

    def test_in_pg(self):
        model = self._model()
        conds, vals, idx = model._build_conditions({"id__in": [1, 2, 3]}, "postgresql")
        assert conds == ["id = ANY($1)"]
        assert vals == [[1, 2, 3]]

    @pytest.mark.parametrize(
        ("lookup", "value", "condition", "values"),
        [
            ("score__lt", 10, "score < $1", [10]),
            ("score__lte", 10, "score <= $1", [10]),
            ("score__gt", 10, "score > $1", [10]),
            ("score__gte", 10, "score >= $1", [10]),
            ("name__isnull", True, "name IS NULL", []),
            ("name__isnull", False, "name IS NOT NULL", []),
        ],
    )
    def test_lookup_pg(self, lookup, value, condition, values):
        conds, vals, _ = self._model()._build_conditions({lookup: value}, "postgresql")
        assert conds == [condition]
        assert vals == values

    def test_multiple_conditions_pg(self):
        model = self._model()
        conds, vals, idx = model._build_conditions(
            {"name": "alice", "score__gte": 5}, "postgresql"
        )
        assert len(conds) == 2
        assert idx == 3

    def test_in_type_error(self):
        model = self._model()
        with pytest.raises(TypeError, match="requires a list/tuple/set"):
            model._build_conditions({"name__in": "bad"}, "postgresql")


# ---------------------------------------------------------------------------
# PostgreSQL-path CRUD via mock database
# ---------------------------------------------------------------------------


class TestPostgresqlPaths:
    """Exercise PostgreSQL code paths in insert/update/delete/bulk using mocks."""

    @pytest.fixture
    async def pg_db(self, monkeypatch):
        from unittest.mock import AsyncMock, MagicMock, PropertyMock

        db = MagicMock()
        type(db).protocol = PropertyMock(return_value="postgresql")
        db.execute = AsyncMock(return_value=None)
        db.fetch_one = AsyncMock(return_value={"id": 1, "name": "test", "score": 0})
        db.fetch_all = AsyncMock(return_value=[{"id": 1, "name": "test", "score": 0}])
        db.fetch_val = AsyncMock(return_value=1)
        monkeypatch.setattr(ModelRegistry, "get_database", classmethod(lambda cls: db))
        yield db

    def _model(self, pg_db):
        class M(AUniqORMBase, table="things"):  # noqa: N801
            id: int = PrimaryKey()
            name: str
            score: int = Column(default=0)

        M.__database__ = pg_db
        return M

    async def test_insert_pg(self, pg_db):
        model = self._model(pg_db)
        inst = model(name="a", score=5)
        result = await inst.insert()
        # Should call fetch_one with RETURNING *
        pg_db.fetch_one.assert_awaited_once()
        call_sql = pg_db.fetch_one.call_args[0][0]
        assert "RETURNING *" in call_sql
        assert "$1" in call_sql
        assert result.id == 1

    async def test_insert_pg_on_conflict(self, pg_db):
        model = self._model(pg_db)
        inst = model(name="a", score=5)
        await inst.insert(on_conflict="(name) DO NOTHING")
        call_sql = pg_db.fetch_one.call_args[0][0]
        assert "ON CONFLICT (name) DO NOTHING" in call_sql
        assert "RETURNING *" in call_sql

    async def test_bulk_insert_pg(self, pg_db):
        model = self._model(pg_db)
        instances = [model(name="a", score=1), model(name="b", score=2)]
        await model.bulk_insert(instances)
        pg_db.execute.assert_awaited_once()
        call_sql = pg_db.execute.call_args[0][0]
        assert "unnest" in call_sql
        assert "$1" in call_sql

    async def test_bulk_insert_pg_on_conflict(self, pg_db):
        model = self._model(pg_db)
        instances = [model(name="a", score=1)]
        await model.bulk_insert(instances, on_conflict="(name) DO NOTHING")
        call_sql = pg_db.execute.call_args[0][0]
        assert "ON CONFLICT (name) DO NOTHING" in call_sql

    async def test_bulk_update_pg(self, pg_db):
        model = self._model(pg_db)
        inst = model(id=1, name="a", score=10)
        await model.bulk_update([inst], fields=["score"])
        pg_db.execute.assert_awaited_once()
        call_sql = pg_db.execute.call_args[0][0]
        assert "unnest" in call_sql
        assert "SET score = _d.score" in call_sql

    async def test_update_pg(self, pg_db):
        model = self._model(pg_db)
        inst = model(id=1, name="old", score=0)
        await inst.update(name="new")
        pg_db.execute.assert_awaited_once()
        call_sql = pg_db.execute.call_args[0][0]
        assert "SET name = $1" in call_sql
        assert "WHERE id = $2" in call_sql

    async def test_update_pg_with_rawsql(self, pg_db):
        model = self._model(pg_db)
        inst = model(id=1, name="x", score=0)
        pg_db.fetch_one.return_value = {"id": 1, "name": "x", "score": 99}
        await inst.update(score=RawSQL("score + 1"))
        call_sql = pg_db.execute.call_args[0][0]
        assert "score = score + 1" in call_sql
        # Should re-fetch for RawSQL
        assert pg_db.fetch_one.await_count >= 1

    async def test_delete_pg(self, pg_db):
        model = self._model(pg_db)
        inst = model(id=1, name="x", score=0)
        await inst.delete()
        pg_db.execute.assert_awaited_once()
        call_sql = pg_db.execute.call_args[0][0]
        assert "DELETE FROM things WHERE id = $1" in call_sql

    async def test_delete_where_pg(self, pg_db):
        model = self._model(pg_db)
        await model.delete_where(score__gt=5)
        call_sql = pg_db.execute.call_args[0][0]
        assert "DELETE FROM things WHERE score > $1" in call_sql

    async def test_update_where_pg(self, pg_db):
        model = self._model(pg_db)
        await model.update_where({"score": 99}, name="alice")
        call_sql = pg_db.execute.call_args[0][0]
        assert "SET score = $1" in call_sql
        assert "WHERE name = $2" in call_sql

    async def test_update_where_pg_rawsql(self, pg_db):
        model = self._model(pg_db)
        await model.update_where({"score": RawSQL("score + 1")}, name="bob")
        call_sql = pg_db.execute.call_args[0][0]
        assert "score = score + 1" in call_sql
        assert "WHERE name = $1" in call_sql

    async def test_get_pg(self, pg_db):
        model = self._model(pg_db)
        result = await model.get(id=1)
        call_sql = pg_db.fetch_one.call_args[0][0]
        assert "WHERE id = $1" in call_sql
        assert result is not None

    async def test_filter_pg(self, pg_db):
        model = self._model(pg_db)
        await model.filter(name="test", order_by="id")
        call_sql = pg_db.fetch_all.call_args[0][0]
        assert "WHERE name = $1" in call_sql
        assert "ORDER BY id" in call_sql

    async def test_count_pg(self, pg_db):
        model = self._model(pg_db)
        pg_db.fetch_val.return_value = 5
        result = await model.count(score__gte=1)
        call_sql = pg_db.fetch_val.call_args[0][0]
        assert "COUNT(*)" in call_sql
        assert "WHERE score >= $1" in call_sql
        assert result == 5


# ---------------------------------------------------------------------------
# MySQL insert paths (mock)
# ---------------------------------------------------------------------------


class TestMySQLInsertPaths:
    @pytest.fixture
    async def mysql_db(self, monkeypatch):
        from unittest.mock import AsyncMock, MagicMock, PropertyMock

        db = MagicMock()
        type(db).protocol = PropertyMock(return_value="mysql")
        db.execute = AsyncMock(return_value=None)
        db.fetch_one = AsyncMock(return_value={"id": 1, "name": "test", "score": 0})
        db.fetch_val = AsyncMock(return_value=1)
        monkeypatch.setattr(ModelRegistry, "get_database", classmethod(lambda cls: db))
        yield db

    def _model(self, mysql_db):
        class M(AUniqORMBase, table="my_things"):  # noqa: N801
            id: int = PrimaryKey()
            name: str
            score: int = Column(default=0)

        M.__database__ = mysql_db
        return M

    async def test_insert_mysql_auto_pk(self, mysql_db):
        model = self._model(mysql_db)
        inst = model(name="a", score=5)
        result = await inst.insert()
        # MySQL path: execute + LAST_INSERT_ID
        mysql_db.execute.assert_awaited_once()
        call_sql = mysql_db.execute.call_args[0][0]
        assert "INSERT INTO my_things" in call_sql
        assert "RETURNING" not in call_sql
        # Should fetch last insert id
        mysql_db.fetch_val.assert_awaited()
        assert result.id == 1

    async def test_insert_mysql_with_server_defaults(self, mysql_db):
        class SD(AUniqORMBase, table="sd_things"):
            id: int = PrimaryKey()
            name: str
            ts: str = Column(server_default="NOW()")

        SD.__database__ = mysql_db
        inst = SD(name="hello")
        mysql_db.fetch_one.return_value = {"id": 1, "name": "hello", "ts": "2026-01-01"}
        await inst.insert()
        # Should refetch full row for server defaults
        assert mysql_db.fetch_one.await_count >= 1

    async def test_bulk_insert_mysql_on_conflict(self, mysql_db):
        model = self._model(mysql_db)
        instances = [model(name="a", score=1)]
        await model.bulk_insert(instances, on_conflict="UPDATE name=VALUES(name)")
        call_sql = mysql_db.execute.call_args[0][0]
        assert "ON DUPLICATE KEY" in call_sql


# ---------------------------------------------------------------------------
# _sync_columns with index creation
# ---------------------------------------------------------------------------


class TestSyncColumnsIndex:
    async def test_index_created_on_create_table(self, sqlite_db):
        class Indexed(AUniqORMBase, table="indexed_t"):
            id: int = PrimaryKey()
            email: str = Column(index=True)

        await Indexed.create_table()
        Indexed.__database__ = sqlite_db

        # Check that the index was created in SQLite
        row = await sqlite_db.fetch_one(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND tbl_name='indexed_t' AND name LIKE 'idx_%'"
        )
        assert row is not None
        assert "email" in row["name"]


# ---------------------------------------------------------------------------
# SQLite insert with server_default (RETURNING *)
# ---------------------------------------------------------------------------


class TestSQLiteServerDefault:
    async def test_insert_with_server_default(self, db):
        class WithDefault(AUniqORMBase, table="with_default"):
            id: int = PrimaryKey()
            name: str
            created: str = Column(server_default="'1970-01-01'")

        await WithDefault.create_table()
        WithDefault.__database__ = db

        inst = await WithDefault(name="hello").insert()
        # Server default should be populated from RETURNING *
        assert inst.id is not None
        assert inst.created == "1970-01-01"


# ---------------------------------------------------------------------------
# bulk_update validation errors
# ---------------------------------------------------------------------------


class TestBulkUpdateValidation:
    async def test_bulk_update_no_pk_defined(self, db):
        # A model without any PrimaryKey column — should raise
        # We can't easily define a AUniqORMBase without a PK due to metaclass,
        # so we test with an instance missing its PK value
        class HasPK(AUniqORMBase, table="has_pk_val"):
            id: int = PrimaryKey()
            val: str

        await HasPK.create_table()
        HasPK.__database__ = db

        inst = HasPK(val="test")  # id is still the Column sentinel
        with pytest.raises(ValueError, match="missing primary key value"):
            await HasPK.bulk_update([inst], fields=["val"])


# ---------------------------------------------------------------------------
# bulk_insert skips server-default columns
# ---------------------------------------------------------------------------


class TestBulkInsertServerDefault:
    async def test_bulk_insert_skips_server_default_columns(self, db):
        class WithSD(AUniqORMBase, table="with_sd_bulk"):
            id: int = PrimaryKey()
            name: str
            status: str = Column(server_default="'pending'")

        await WithSD.create_table()
        WithSD.__database__ = db

        items = [WithSD(name="a"), WithSD(name="b")]
        await WithSD.bulk_insert(items)

        rows = await WithSD.filter(order_by="id")
        assert len(rows) == 2
        assert rows[0].status == "pending"
        assert rows[1].status == "pending"


# ---------------------------------------------------------------------------
# MySQL insert with both auto-PK and server defaults
# ---------------------------------------------------------------------------


class TestMySQLInsertAutoAndDefaults:
    async def test_insert_auto_pk_and_server_defaults(self):
        from unittest.mock import AsyncMock, MagicMock, PropertyMock

        db = MagicMock()
        type(db).protocol = PropertyMock(return_value="mysql")
        db.execute = AsyncMock(return_value=None)
        db.fetch_one = AsyncMock(
            return_value={"id": 42, "name": "hi", "ts": "2026-01-01"}
        )
        db.fetch_val = AsyncMock(return_value=42)

        class SD(AUniqORMBase, table="sd_auto"):  # noqa: N801
            id: int = PrimaryKey()
            name: str
            ts: str = Column(server_default="NOW()")

        SD.__database__ = db

        # Monkeypatch ModelRegistry.get_database to return our mock
        orig = ModelRegistry.get_database
        ModelRegistry.get_database = classmethod(lambda cls: db)
        try:
            inst = SD(name="hi")
            await inst.insert()
            # Should have re-fetched full row via LAST_INSERT_ID
            assert db.fetch_one.await_count >= 1
            call_sql = db.fetch_one.call_args[0][0]
            assert "LAST_INSERT_ID" in call_sql
        finally:
            ModelRegistry.get_database = orig

    async def test_insert_explicit_pk_and_server_defaults(self):
        from unittest.mock import AsyncMock, MagicMock, PropertyMock

        db = MagicMock()
        type(db).protocol = PropertyMock(return_value="mysql")
        db.execute = AsyncMock(return_value=None)
        db.fetch_one = AsyncMock(
            return_value={"id": 5, "name": "explicit", "ts": "2026-01-01"}
        )
        db.fetch_val = AsyncMock(return_value=5)

        class SD2(AUniqORMBase, table="sd_explicit"):  # noqa: N801
            id: int = PrimaryKey()
            name: str
            ts: str = Column(server_default="NOW()")

        SD2.__database__ = db

        orig = ModelRegistry.get_database
        ModelRegistry.get_database = classmethod(lambda cls: db)
        try:
            inst = SD2(id=5, name="explicit")
            await inst.insert()
            # Should have re-fetched via WHERE pk = %s
            assert db.fetch_one.await_count >= 1
            call_sql = db.fetch_one.call_args[0][0]
            assert "WHERE id = %s" in call_sql
        finally:
            ModelRegistry.get_database = orig


class TestInsertWritebackUsesSetattr:
    """msgspec.Struct subclasses may override __setattr__ for validation or
    transformation. ``object.__setattr__`` bypasses that hook (and on some
    msgspec configurations raises ``TypeError`` outright). The insert()
    writeback path must use ``setattr`` so descriptors and overrides run.
    """

    async def test_writeback_invokes_setattr_override(self, db):
        sets: list[tuple[str, object]] = []

        class Item(AUniqORMBase, table="issue1_items"):
            id: int = PrimaryKey()
            name: str

            def __setattr__(self, name: str, value: object) -> None:
                sets.append((name, value))
                super().__setattr__(name, value)

        await Item.create_table()
        Item.__database__ = db

        item = Item(name="hello")
        sets.clear()  # ignore any sets during construction
        await item.insert()

        # Writeback must populate the auto-generated PK via __setattr__.
        assert any(n == "id" for n, _ in sets), (
            f"insert() writeback bypassed __setattr__ override: {sets}"
        )


# ---------------------------------------------------------------------------
# Writeback rejects driver-native types that don't match the annotation
#
# asyncpg returns PG UUID columns as uuid.UUID, NUMERIC as Decimal, etc.,
# regardless of how the field is annotated on the model. aUniqORM refuses to
# silently coerce — a mismatch indicates a wrong model annotation and must
# surface as a loud error so the developer fixes the annotation (or registers
# a driver-level codec).
# ---------------------------------------------------------------------------


class TestWritebackValidatesAnnotation:
    @pytest.fixture
    def pg_db_factory(self, monkeypatch):
        from unittest.mock import AsyncMock, MagicMock, PropertyMock

        def make(returning_row):
            db = MagicMock()
            type(db).protocol = PropertyMock(return_value="postgresql")
            db.execute = AsyncMock(return_value=None)
            db.fetch_one = AsyncMock(return_value=returning_row)
            db.fetch_all = AsyncMock(return_value=[returning_row])
            db.fetch_val = AsyncMock(return_value=returning_row.get("id"))
            monkeypatch.setattr(
                ModelRegistry, "get_database", classmethod(lambda cls: db)
            )
            return db

        return make

    async def test_uuid_returned_for_str_annotation_raises(self, pg_db_factory):
        import uuid

        u = uuid.uuid4()
        db = pg_db_factory({"id": u, "name": "x"})

        class Clip(AUniqORMBase, table="clips_validate"):
            id: str = PrimaryKey(db_type="UUID", server_default="gen_random_uuid()")
            name: str

        Clip.__database__ = db
        c = Clip(name="x")
        with pytest.raises(TypeError, match=r"Clip\.id.*UUID.*str"):
            await c.insert()

    async def test_decimal_returned_for_float_annotation_raises(self, pg_db_factory):
        import decimal

        d = decimal.Decimal("1.5")
        db = pg_db_factory({"id": 1, "ratio": d})

        class Stat(AUniqORMBase, table="stats_validate"):
            id: int = PrimaryKey()
            ratio: float

        Stat.__database__ = db
        s = Stat(ratio=0.0)
        with pytest.raises(TypeError, match=r"Stat\.ratio.*Decimal.*float"):
            await s.insert()

    async def test_none_passes_through_for_optional(self, pg_db_factory):
        db = pg_db_factory({"id": 1, "label": None})

        class Tag(AUniqORMBase, table="tags_validate"):
            id: int = PrimaryKey()
            label: str | None = None

        Tag.__database__ = db
        t = Tag()
        await t.insert()
        assert t.label is None

    async def test_matching_uuid_annotation_passes(self, pg_db_factory):
        import uuid

        u = uuid.uuid4()
        db = pg_db_factory({"id": u, "name": "x"})

        class Asset(AUniqORMBase, table="assets_validate"):
            id: uuid.UUID = PrimaryKey(
                db_type="UUID", server_default="gen_random_uuid()"
            )
            name: str

        Asset.__database__ = db
        a = Asset(name="x")
        await a.insert()
        assert a.id == u

    async def test_generic_alias_checks_origin_only(self, pg_db_factory):
        db = pg_db_factory({"id": 1, "tags": [1, 2, 3]})

        class Post(AUniqORMBase, table="posts_validate"):
            id: int = PrimaryKey()
            tags: list[int] = []

        Post.__database__ = db
        p = Post()
        await p.insert()
        assert p.tags == [1, 2, 3]
