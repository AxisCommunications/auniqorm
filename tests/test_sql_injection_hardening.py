# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Regression tests for identifier validation, F operands, LIKE escaping and
``to_sql`` quoting.  The ``real_db`` tests also run against PostgreSQL/MySQL
when reachable (override with ``AUNIQORM_TEST_PG_URL``/``AUNIQORM_TEST_MYSQL_URL``).
"""

from __future__ import annotations

import enum
import os
import uuid
from typing import Any

import pytest

from auniqorm.columns import Column, ForeignKey, PrimaryKey
from auniqorm.connection import Database, dynamic_model
from auniqorm.models import (
    AUniqORMBase,
    AUniqORMBaseMeta,
    Count,
    F,
    ManyToMany,
    ModelRegistry,
    Q,
    RawSQL,
    Sum,
)
from auniqorm.pandas_io import quote_identifier, read_sql, to_sql
from auniqorm.schema import validate_identifier

PG_URL = os.getenv(
    "AUNIQORM_TEST_PG_URL", "postgresql://bench:bench@localhost:5432/bench"
)
MYSQL_URL = os.getenv(
    "AUNIQORM_TEST_MYSQL_URL", "mysql://bench:bench@localhost:3306/bench"
)

# Proven exploits from the security review.
BLIND = (
    "(CASE WHEN (SELECT substr(token,1,1) FROM secrets WHERE id=1)='s' "
    "THEN id ELSE -id END)"
)
TAUTOLOGY = "1=1 OR name"
SET_EVIL = "name = (SELECT token FROM secrets WHERE id=1), qty"


def _item_model(table: str = "items") -> type[AUniqORMBase]:
    class Item(AUniqORMBase, table=table):
        id: int = PrimaryKey()
        name: str
        qty: int = Column(default=0)

    return Item


async def _ready(db: Database, model: type[AUniqORMBase]) -> None:
    ModelRegistry.bind(db)
    await model.create_table()


@pytest.fixture
async def secrets_db(sqlite_db):
    await sqlite_db.execute("CREATE TABLE secrets (id INTEGER PRIMARY KEY, token TEXT)")
    await sqlite_db.execute("INSERT INTO secrets VALUES (1, 'sekret')")
    item = _item_model()
    await _ready(sqlite_db, item)
    for i, name in enumerate(["alpha", "beta", "gamma"], 1):
        await item(id=i, name=name, qty=i).insert()
    return item


async def _drain(it: Any) -> list[Any]:
    return [x async for x in it]


async def _update_first(m: Any, **kw: Any) -> None:
    await (await m.get(id=1)).update(**kw)


# ── Runtime identifiers ──────────────────────────────────────────────────────

REJECTED = {
    "filter-order_by": lambda m: m.filter(order_by=BLIND),
    "filter-order_by-list": lambda m: m.filter(order_by=["id", "-" + BLIND]),
    "iterate-order_by": lambda m: _drain(m.iterate(order_by=BLIND)),
    "distinct-order_by": lambda m: m.distinct("name", order_by=BLIND),
    "values-order_by": lambda m: m.values("name", order_by=BLIND),
    "filter-key": lambda m: m.filter(**{TAUTOLOGY: "zzz"}),
    "filter-key-lookup": lambda m: m.filter(**{TAUTOLOGY + "__contains": "z"}),
    "q-key": lambda m: m.filter(Q(**{TAUTOLOGY: "zzz"})),
    "get-key": lambda m: m.get(**{TAUTOLOGY: "zzz"}),
    "get-related-key": lambda m: m.get(select_related=["x"], **{TAUTOLOGY: 1}),
    "count-key": lambda m: m.count(**{TAUTOLOGY: "zzz"}),
    "exists-key": lambda m: m.exists(**{TAUTOLOGY: "zzz"}),
    "delete_where-key": lambda m: m.delete_where(**{TAUTOLOGY: "zzz"}),
    "distinct-key": lambda m: m.distinct("name", **{TAUTOLOGY: "zzz"}),
    "aggregate-key": lambda m: m.aggregate(n=Count(), **{TAUTOLOGY: "zzz"}),
    "aggregate-alias": lambda m: m.aggregate(**{"n FROM secrets --": Count()}),
    "aggregate-column": lambda m: m.aggregate(n=Count("id) FROM secrets --")),
    "columns": lambda m: m.filter(columns=["(SELECT token FROM secrets) AS n"]),
    "values-field": lambda m: m.values("(SELECT token FROM secrets)"),
    "values-group_by": lambda m: m.values("name", group_by="name, token"),
    "update_where-key": lambda m: m.update_where({"qty": 0}, **{TAUTOLOGY: 1}),
    "update_where-q-key": lambda m: m.update_where(
        {"qty": 0}, Q(id=1), **{TAUTOLOGY: 1}
    ),
    "update_where-set": lambda m: m.update_where({SET_EVIL: 1}, id=1),
    "update_where-q-set": lambda m: m.update_where({SET_EVIL: 1}, Q(id=1)),
    "update_where-f-column": lambda m: m.update_where({"qty": F("token") + 1}, id=1),
    "update-set": lambda m: _update_first(m, **{SET_EVIL: 1}),
    "update-f-column": lambda m: _update_first(m, qty=F("qty) --")),
    "batch-set": lambda m: m.batch_update_where(
        {SET_EVIL: 1}, [[1]], filter_fields=["id"]
    ),
    "batch-filter": lambda m: m.batch_update_where(
        {"qty": 7}, [[7, 1]], filter_fields=["id OR 1=1"]
    ),
    "bulk_update": lambda m: m.bulk_update([m(id=1, name="a", qty=0)], ["qty = 0 --"]),
}


@pytest.mark.parametrize("call", REJECTED.values(), ids=REJECTED.keys())
async def test_rejects_unknown_identifiers(secrets_db, call):
    with pytest.raises(ValueError):
        await call(secrets_db)
    rows = await secrets_db.filter(order_by="id")
    assert [(r.name, r.qty) for r in rows] == [("alpha", 1), ("beta", 2), ("gamma", 3)]


async def test_valid_identifiers_still_work(secrets_db):
    item = secrets_db
    assert [r.id for r in await _drain(item.iterate(order_by="-id"))] == [3, 2, 1]
    assert await item.distinct("name", order_by="-name") == ["gamma", "beta", "alpha"]
    assert await item.filter(columns=["name"], flat=True, order_by=["-qty"]) == [
        "gamma",
        "beta",
        "alpha",
    ]
    rows = await item.values(
        "name", total=Sum("qty"), group_by="name", order_by="-total"
    )
    assert [r["name"] for r in rows] == ["gamma", "beta", "alpha"]
    assert await item.aggregate(n=Count("id"), total=Sum("qty")) == {"n": 3, "total": 6}
    await item.batch_update_where({"qty": 7}, [[7, 1]], filter_fields=["id"])
    assert (await item.get(id=1)).qty == 7


async def test_select_related_still_works(sqlite_db):
    class Author(AUniqORMBase, table="authors"):
        id: int = PrimaryKey()
        name: str

    class Post(AUniqORMBase, table="posts"):
        id: int = PrimaryKey()
        author_id: int = ForeignKey("authors.id")

    for m in (Author, Post):
        await _ready(sqlite_db, m)
    await Author(id=1, name="a").insert()
    await Post(id=1, author_id=1).insert()
    posts = await Post.filter(select_related=["author"], id=1, order_by="-id")
    assert posts[0].author.name == "a"  # type: ignore[attr-defined]
    with pytest.raises(ValueError):
        await Post.filter(select_related=["author"], order_by=BLIND)
    with pytest.raises(ValueError):
        await Post.filter(select_related=["author) --"])


# ── F expressions ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "operand",
    [
        "(SELECT length(token) FROM secrets)",
        True,
        None,
        float("nan"),
        float("inf"),
        b"1",
        [1],
    ],
)
def test_f_rejects_non_numeric_operands(operand):
    with pytest.raises(TypeError):
        F("qty") + operand
    with pytest.raises(TypeError):
        operand - F("qty")


def test_f_numeric_rendering():
    class Level(enum.IntEnum):
        HIGH = 3

    assert (F("qty") - -5).to_sql() == "(qty - (-5))"
    assert (F("qty") * 2.5).to_sql() == "(qty * 2.5)"
    assert (F("qty") + Level.HIGH).to_sql() == "(qty + 3)"
    assert (-1 * F("qty")).to_sql() == "((-1) * qty)"


async def test_update_where_cache_distinguishes_expressions(secrets_db):
    await secrets_db.update_where({"qty": F("qty") + 10}, id=1)
    await secrets_db.update_where({"qty": F("qty") + 100}, id=1)
    await secrets_db.update_where({"qty": RawSQL("5")}, id=2)
    await secrets_db.update_where({"qty": RawSQL("6")}, id=2)
    assert [r.qty for r in await secrets_db.filter(order_by="id")] == [111, 6, 3]


# ── Model / table definitions ────────────────────────────────────────────────


def _define(table: str, fk: str = "users.id") -> Any:
    ns = {
        "__annotations__": {"id": "int", "user_id": "int"},
        "id": PrimaryKey(),
        "user_id": ForeignKey(fk),
    }
    return AUniqORMBaseMeta("M", (AUniqORMBase,), ns, table=table)


@pytest.mark.parametrize(
    "define",
    [
        lambda: _define("t; DROP TABLE secrets"),
        lambda: _define("a.b.c"),
        lambda: _define("t_fk", fk="users(id); DROP TABLE x; --.id"),
        lambda: ManyToMany("tags", through="article_tags; DROP TABLE x"),
        lambda: ManyToMany("tags", through="article_tags", source_fk="a b"),
        lambda: dynamic_model("t; DROP TABLE secrets; --"),
        lambda: dynamic_model("1t"),
        lambda: dynamic_model("t", {"id": int, "x TEXT, y": str}),
        lambda: dynamic_model("t", {"id": int, "näme": str}),
    ],
)
def test_rejects_invalid_model_identifiers(define):
    with pytest.raises(ValueError):
        define()


def test_valid_model_identifiers():
    assert _define("analytics.events").__table_name__ == "analytics.events"
    assert dynamic_model("public.t_v3").__table_name__ == "public.t_v3"
    assert validate_identifier("abc_1") == "abc_1"
    for bad in ["", "1a", "a b", "a-b", "a;b", "a\n", "a.b", 'a"', None]:
        with pytest.raises(ValueError):
            validate_identifier(bad)
    assert quote_identifier('we"ird', "postgresql") == '"we""ird"'
    assert quote_identifier("we`ird", "mysql") == "`we``ird`"


# ── Pattern lookups ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "dialect,ph,escape",
    [
        ("postgresql", "$1", " ESCAPE '\\'"),
        ("sqlite", "?", " ESCAPE '\\'"),
        ("mysql", "%s", " ESCAPE '\\\\'"),
    ],
)
def test_like_sql(dialect, ph, escape):
    item = _item_model("like_sql")
    lookups = {"name__contains": "50%_\\", "name__icontains": "a", "name__iexact": "%"}
    conds, values, _ = item._build_conditions(lookups, dialect)
    ph2, ph3 = ("$2", "$3") if dialect == "postgresql" else (ph, ph)
    ilike = (
        f"name ILIKE {ph2}"
        if dialect == "postgresql"
        else f"LOWER(name) LIKE LOWER({ph2})"
    )
    assert conds == [
        f"name LIKE {ph}{escape}",
        ilike + escape,
        f"LOWER(name) = LOWER({ph3})",
    ]
    assert values == ["%50\\%\\_\\\\%", "%a%", "%"]


LIKE_CASES = [  # (lookup, value, expected ids)
    ("contains", "%", {1, 8}),
    ("contains", "_", {3}),
    ("contains", "\\", {5, 8}),
    ("contains", "\\%", {8}),
    ("icontains", "_B", {3}),
    ("startswith", "a_", {3}),
    ("istartswith", "A_", {3}),
    ("endswith", "\\slash", {5}),
    ("iendswith", "% SURE", {1}),
    ("iendswith", "%", set()),
    ("iexact", "%", set()),
    ("iexact", "_____", set()),
    ("iexact", "A_B", {3}),
    ("iexact", "mixed", {7}),
]


async def _check_like_cases(db: Database) -> None:
    item = _item_model(f"like_{uuid.uuid4().hex[:10]}")
    await _ready(db, item)
    try:
        rows = [
            "100% sure",
            "100 percent",
            "a_b",
            "axb",
            "back\\slash",
            "plain",
            "MiXeD",
            "x\\%y",
        ]
        for i, name in enumerate(rows, 1):
            await item(id=i, name=name, qty=0).insert()
        for lookup, value, expected in LIKE_CASES:
            found = {r.id for r in await item.filter(**{f"name__{lookup}": value})}
            assert found == expected, (db.protocol, lookup, value)
        assert await item.get(name__iexact="%") is None  # Invite.get(code__iexact="%")
    finally:
        await item.drop_table()


async def test_like_escaping_dialect_sql(db):
    """Dialect-specific SQL, executed through the conftest SQLite translation."""
    await _check_like_cases(db)


@pytest.fixture(params=["sqlite", "postgresql", "mysql"])
async def real_db(request):
    url = {"sqlite": "sqlite://:memory:", "postgresql": PG_URL, "mysql": MYSQL_URL}
    db = Database(url[request.param])
    try:
        await db.connect()
    except Exception:
        pytest.skip(f"{request.param} server not reachable")
    ModelRegistry.bind(db)
    yield db
    await db.disconnect()


async def test_like_escaping_real_servers(real_db):
    await _check_like_cases(real_db)


async def test_f_and_order_by_real_servers(real_db):
    item = _item_model(f"f_real_{uuid.uuid4().hex[:10]}")
    await _ready(real_db, item)
    try:
        await item(id=1, name="a", qty=5).insert()
        await item.update_where({"qty": F("qty") * 2 - -1}, id=1)
        await item.update_where({"qty": F("qty") + 1.5 * 2}, id=1)
        assert (await item.get(id=1)).qty == 14
        with pytest.raises(ValueError):
            await item.filter(order_by=BLIND)
    finally:
        await item.drop_table()


# ── pandas to_sql ────────────────────────────────────────────────────────────


async def test_to_sql_hostile_names(sqlite_db):
    pd = pytest.importorskip("pandas")
    await sqlite_db.execute("CREATE TABLE secrets (id INTEGER, token TEXT)")
    evil = "name) SELECT token FROM secrets WHERE ? IS NOT NULL --"
    table = "x; DROP TABLE secrets; --"
    df = pd.DataFrame({evil: ["x"], 'we"ird': ["y"]})
    for _ in range(2):  # second run exercises DROP TABLE IF EXISTS
        assert await to_sql(sqlite_db, df, table, if_exists="replace") == 1
    out = await read_sql(
        sqlite_db, f"SELECT * FROM {quote_identifier(table, 'sqlite')}"
    )
    assert out.to_dict("records") == [{evil: "x", 'we"ird': "y"}]
    assert await sqlite_db.fetch_val("SELECT COUNT(*) FROM secrets") == 0


async def test_to_sql_real_servers(real_db):
    pd = pytest.importorskip("pandas")
    table = f"Pd_{uuid.uuid4().hex[:8]}"
    cols = ['we"ird', "back`tick", "sp ace)--"]
    quoted = quote_identifier(table, real_db.protocol)
    try:
        df = pd.DataFrame({c: [f"v{i}"] for i, c in enumerate(cols)})
        await to_sql(real_db, df, table, if_exists="replace")
        await to_sql(real_db, df, table)
        out = await read_sql(real_db, f"SELECT * FROM {quoted}")
        assert list(out.columns) == cols and len(out) == 2
    finally:
        await real_db.execute(f"DROP TABLE IF EXISTS {quoted}")
