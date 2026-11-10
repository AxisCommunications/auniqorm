# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Security hardening of the native (Rust) PostgreSQL/MySQL/MongoDB drivers.

Covers TLS modes, option-driven driver selection, checked PostgreSQL binding
and decoding, the pinned-connection handle contract and session reset, and
MongoDB keyword-option translation.

Database tests use the benchmark compose stack (PostgreSQL on 5432 without TLS,
MySQL 8.4 on 3306 with auto-generated TLS certificates) and skip when it is
unreachable. A TLS PostgreSQL is used when ``AUNIQORM_TEST_PG_TLS_URL`` (with a
certificate for ``localhost``), ``AUNIQORM_TEST_PG_TLS_CA`` (its CA) and
``AUNIQORM_TEST_OTHER_CA`` (an unrelated CA) are set, and a MySQL without TLS
when ``AUNIQORM_TEST_MYSQL_NOTLS_URL`` is set.
"""

from __future__ import annotations

import asyncio
import gc
import importlib.util
import os
import ssl
import uuid
from datetime import UTC, date, datetime, time, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import msgspec
import pytest

native = pytest.importorskip("auniqorm_native")

from auniqorm import AUniqORMBase, Column, Database, ModelRegistry, PrimaryKey  # noqa: E402
from auniqorm.drivers import mongo  # noqa: E402
from auniqorm.drivers import mysql_native as mysql_mod  # noqa: E402
from auniqorm.drivers import postgresql_native as pg_mod  # noqa: E402
from auniqorm.drivers.registry import DriverRegistry  # noqa: E402
from auniqorm.exceptions import ConnectionError, DriverNotFoundError  # noqa: E402

from ._services import MYSQL_URL, PG_URL  # noqa: E402

PG_TLS_URL = os.getenv("AUNIQORM_TEST_PG_TLS_URL", "")
PG_TLS_CA = os.getenv("AUNIQORM_TEST_PG_TLS_CA")
OTHER_CA = os.getenv("AUNIQORM_TEST_OTHER_CA")
MYSQL_NOTLS_URL = os.getenv("AUNIQORM_TEST_MYSQL_NOTLS_URL", "")


needs_pg = pytest.mark.needs_pg
needs_mysql = pytest.mark.needs_mysql
needs_pg_tls = pytest.mark.skipif(
    not (PG_TLS_URL and PG_TLS_CA and OTHER_CA), reason="needs a TLS PostgreSQL"
)


@pytest.fixture(autouse=True)
def _native_env(monkeypatch):
    for name in ("AUNIQORM_PG_NATIVE", "AUNIQORM_MYSQL_NATIVE", "PGSSLMODE"):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(params=[False, True], ids=["no-fallback", "fallback"])
def fallback_installed(request, monkeypatch):
    """Pretend asyncpg/aiomysql are (not) installed."""
    real = importlib.util.find_spec

    def find_spec(name, *args, **kwargs):
        if name in ("asyncpg", "aiomysql"):
            return SimpleNamespace(name=name) if request.param else None
        return real(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", find_spec)
    return request.param


# ── Keyword options and driver selection ────────────────────────────────────


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        ({"ssl": None}, {}),
        ({"ssl": True}, {"sslmode": "verify-full", "sslrootcert": "system"}),
        ({"ssl": False}, {"sslmode": "disable"}),
        (
            {"ssl": "VERIFY_FULL", "max_size": 4},
            {"sslmode": "verify-full", "max_size": 4},
        ),
    ],
)
def test_pg_option_translation(options, expected) -> None:
    assert pg_mod._options(options) == (expected, [])


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        ({"ssl": True}, {"ssl_mode": "verify_identity"}),
        ({"ssl": False}, {"ssl_mode": "disabled"}),
        ({"ssl": "REQUIRE", "min_size": 1}, {"ssl_mode": "required", "min_size": 1}),
        ({"ssl": "verify-full"}, {"ssl_mode": "verify_identity"}),
    ],
)
def test_mysql_option_translation(options, expected) -> None:
    assert mysql_mod.MySQLNativeDriver.unsupported_options(options) == []
    assert mysql_mod.native_options(options, mysql_mod._ssl)[0] == expected


UNSUPPORTED = [
    {"ssl": ssl.create_default_context()},
    {"ssl": "bogus"},
    {"command_timeout": 5},
    {"max_size": "10"},
]


@pytest.mark.parametrize("options", UNSUPPORTED)
@pytest.mark.parametrize(
    "driver", [pg_mod.PostgresNativeDriver, mysql_mod.MySQLNativeDriver]
)
def test_native_drivers_refuse_unsupported_options(driver, options) -> None:
    assert driver.unsupported_options(options) == list(options)
    with pytest.raises(ConnectionError, match="cannot honour"):
        driver("x://h/db", **options)


@pytest.mark.parametrize(
    "setting", ["PGSSLCERT", "PGSSLCRL", "PGSSLMINPROTOCOLVERSION"]
)
def test_pg_unsupported_tls_environment(monkeypatch, setting) -> None:
    monkeypatch.setenv(setting, "x")
    assert pg_mod._options({})[1] == [setting]
    assert pg_mod._options({"ssl": False})[1] == []


@pytest.mark.parametrize(
    ("protocol", "native_name", "fallback"),
    [
        ("postgresql", "PostgresNativeDriver", "PostgresAsyncpgDriver"),
        ("mysql", "MySQLNativeDriver", "MySQLDriver"),
    ],
)
@pytest.mark.parametrize("options", UNSUPPORTED[:1] + UNSUPPORTED[2:3])
def test_registry_selection(
    fallback_installed, protocol, native_name, fallback, options
) -> None:
    for supported in (None, {"ssl": "require"}, {"max_size": 3}):
        assert DriverRegistry.get(protocol, supported).__name__ == native_name
    if fallback_installed:
        assert DriverRegistry.get(protocol, options).__name__ == fallback
    else:
        with pytest.raises(DriverNotFoundError, match="cannot honour option"):
            DriverRegistry.get(protocol, options)


async def test_database_never_ignores_ssl_context(monkeypatch) -> None:
    monkeypatch.setattr(importlib.util, "find_spec", lambda name, *a, **k: None)
    db = Database(PG_URL, ssl=ssl.create_default_context())
    with pytest.raises(DriverNotFoundError):
        await db.connect()
    assert db._driver is None


# ── PostgreSQL TLS ──────────────────────────────────────────────────────────


async def _pg_ssl(url: str, **kwargs) -> bool:
    engine = await native.connect_pg(url, **kwargs)
    try:
        sql = "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()"
        return (await engine.fetch_one(sql, None))["ssl"]
    finally:
        await engine.close()


@needs_pg
class TestPgTlsAgainstPlaintextServer:
    async def test_prefer_and_allow_fall_back_to_plaintext(self) -> None:
        assert await _pg_ssl(PG_URL) is False
        assert await _pg_ssl(PG_URL + "?sslmode=allow") is False

    @pytest.mark.parametrize(
        ("query", "kwargs", "env"),
        [
            ("?sslmode=require", {}, None),
            ("?sslmode=verify-full&sslrootcert=system", {}, None),
            ("?sslmode=disable", {"sslmode": "require"}, None),
            ("", {}, "require"),
        ],
    )
    async def test_required_tls_is_refused(self, monkeypatch, query, kwargs, env):
        if env:
            monkeypatch.setenv("PGSSLMODE", env)
        with pytest.raises(RuntimeError, match="does not support TLS"):
            await native.connect_pg(PG_URL + query, **kwargs)

    async def test_verify_without_root_certificate_fails(self, monkeypatch) -> None:
        monkeypatch.setenv("HOME", "/nonexistent-home")
        monkeypatch.delenv("PGSSLROOTCERT", raising=False)
        with pytest.raises(RuntimeError, match="no root certificate"):
            await native.connect_pg(PG_URL + "?sslmode=verify-ca")

    @pytest.mark.parametrize("param", ["sslcert=/x", "sslpassword=x", "sslcrl=/x"])
    async def test_unsupported_tls_url_parameters_fail(self, param) -> None:
        with pytest.raises(RuntimeError, match="unknown option"):
            await native.connect_pg(f"{PG_URL}?{param}")

    @pytest.mark.parametrize("ssl_option", ["require", True, "verify-full", False])
    async def test_database_ssl_option(self, ssl_option) -> None:
        db = Database(PG_URL, ssl=ssl_option, models=False)
        if ssl_option is not False:
            with pytest.raises(ConnectionError):
                await db.connect()
            return
        await db.connect()
        try:
            row = await db.fetch_one(
                "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()"
            )
            assert row["ssl"] is False
        finally:
            await db.disconnect()


@needs_pg_tls
class TestPgTlsAgainstTlsServer:
    @pytest.mark.parametrize(
        ("query", "host", "kwargs", "error"),
        [
            ("", "localhost", {}, None),
            ("?sslmode=require", "localhost", {}, None),
            ("?sslmode=require&sslrootcert={other}", "localhost", {}, "UnknownIssuer"),
            ("?sslmode=verify-ca&sslrootcert={ca}", "localhost", {}, None),
            (
                "?sslmode=verify-ca&sslrootcert={other}",
                "localhost",
                {},
                "UnknownIssuer",
            ),
            ("?sslmode=verify-ca&sslrootcert={ca}", "127.0.0.1", {}, None),
            ("?sslmode=verify-full&sslrootcert={ca}", "localhost", {}, None),
            (
                "?sslmode=verify-full&sslrootcert={ca}",
                "127.0.0.1",
                {},
                "not valid for name",
            ),
            (
                "",
                "localhost",
                {"sslmode": "verify-full", "sslrootcert": "system"},
                "UnknownIssuer",
            ),
        ],
    )
    async def test_modes(self, monkeypatch, query, host, kwargs, error) -> None:
        monkeypatch.setenv("HOME", "/nonexistent-home")
        url = PG_TLS_URL.replace("localhost", host) + query.format(
            ca=PG_TLS_CA, other=OTHER_CA
        )
        if error:
            with pytest.raises(RuntimeError, match=error):
                await _pg_ssl(url, **kwargs)
        else:
            assert await _pg_ssl(url, **kwargs) is True

    async def test_disable(self) -> None:
        assert await _pg_ssl(PG_TLS_URL + "?sslmode=disable") is False


# ── MySQL TLS ───────────────────────────────────────────────────────────────


async def _mysql_cipher(url: str, **kwargs) -> str:
    engine = await native.connect_mysql(url, **kwargs)
    try:
        row = await engine.fetch_one("SHOW SESSION STATUS LIKE 'Ssl_cipher'", None)
        return row["Value"]
    finally:
        await engine.close()


@needs_mysql
class TestMySqlTls:
    @pytest.mark.parametrize(
        ("query", "mode", "encrypted"),
        [
            ("", None, False),
            ("", "required", True),
            ("", "preferred", True),
            ("?require_ssl=true&verify_ca=false", None, True),
            ("?require_ssl=true&verify_ca=false", "disabled", False),
        ],
    )
    async def test_modes(self, query, mode, encrypted) -> None:
        cipher = await _mysql_cipher(MYSQL_URL + query, ssl_mode=mode)
        assert bool(cipher) is encrypted

    @pytest.mark.parametrize(
        ("query", "mode"), [("", "verify_identity"), ("?require_ssl=true", None)]
    )
    async def test_untrusted_certificate_is_rejected(self, query, mode) -> None:
        # MySQL's auto-generated certificate is self-signed.
        with pytest.raises(RuntimeError, match="invalid peer certificate"):
            await _mysql_cipher(MYSQL_URL + query, ssl_mode=mode)

    @pytest.mark.parametrize(
        ("arg", "error"), [("verify_ca", "VERIFY_CA"), ("x", "invalid")]
    )
    async def test_refused_modes(self, arg, error) -> None:
        with pytest.raises(ValueError, match=error):
            await native.connect_mysql(MYSQL_URL, ssl_mode=arg)

    async def test_unknown_url_parameters_fail(self) -> None:
        with pytest.raises(RuntimeError, match="Unknown connection URL parameter"):
            await native.connect_mysql(MYSQL_URL + "?ssl_ca=/x")

    async def test_database_ssl_option(self) -> None:
        db = Database(MYSQL_URL, ssl="required", models=False)
        await db.connect()
        try:
            row = await db.fetch_one("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
            assert row["Value"] != ""
        finally:
            await db.disconnect()
        with pytest.raises(ConnectionError):
            await Database(MYSQL_URL, ssl=True, models=False).connect()


@pytest.mark.skipif(not MYSQL_NOTLS_URL, reason="set AUNIQORM_TEST_MYSQL_NOTLS_URL")
async def test_mysql_without_tls() -> None:
    with pytest.raises(RuntimeError, match="server does not have this capability"):
        await native.connect_mysql(MYSQL_NOTLS_URL, ssl_mode="required")
    assert await _mysql_cipher(MYSQL_NOTLS_URL, ssl_mode="preferred") == ""


# ── PostgreSQL parameter binding ────────────────────────────────────────────


@pytest.fixture
async def pg():
    engine = await native.connect_pg(PG_URL)
    await engine.execute("DROP TABLE IF EXISTS _hard_bind", None)
    await engine.execute(
        "CREATE TABLE _hard_bind (id int4, small int2, big int8, qty int4, "
        "name text, flag bool, f4 float4, n numeric, u uuid, ts timestamptz, "
        "t timestamp, d date, tm time, j jsonb, b bytea, ints int4[], texts text[])",
        None,
    )
    try:
        yield engine
    finally:
        await engine.execute("DROP TABLE IF EXISTS _hard_bind", None)
        await engine.close()


def _ins(column: str) -> str:
    return f"INSERT INTO _hard_bind ({column}) VALUES ($1)"


@needs_pg
class TestPgCheckedBinding:
    @pytest.mark.parametrize(
        ("column", "value", "error", "match"),
        [
            ("id", 2**31, RuntimeError, "out of range"),
            ("id", -(2**31) - 1, RuntimeError, "out of range"),
            ("id", 2**32 + 7, RuntimeError, "out of range"),
            ("qty", 2**32 - 1, RuntimeError, "out of range"),
            ("small", 2**15, RuntimeError, "out of range"),
            ("ints", [1, 2**31], RuntimeError, "out of range"),
            ("f4", 1e300, RuntimeError, "out of range"),
            ("big", 2**63, OverflowError, "64-bit"),
            ("n", 2**70, OverflowError, "64-bit"),
            ("n", Decimal("1." + "1" * 40), ValueError, "28 digits"),
            ("id", "7", RuntimeError, "cannot bind"),
            ("name", 7, RuntimeError, "cannot bind"),
            ("flag", 1, RuntimeError, "cannot bind"),
            ("id", 1.5, RuntimeError, "cannot bind"),
            ("ts", "2024-01-01T00:00:00+00:00", RuntimeError, "cannot bind"),
            ("b", "text", RuntimeError, "cannot bind"),
            ("u", 5, RuntimeError, "cannot bind"),
            ("name", uuid.uuid4(), RuntimeError, "cannot bind"),
            ("d", datetime(2024, 1, 1), RuntimeError, "cannot bind"),
            ("t", datetime(2024, 1, 1, tzinfo=UTC), RuntimeError, "aware"),
            ("j", b"bytes", RuntimeError, "cannot bind"),
            ("name", object(), TypeError, "unsupported bind parameter type"),
            ("name", timedelta(1), TypeError, "unsupported bind parameter type"),
        ],
    )
    async def test_invalid_values_raise(self, pg, column, value, error, match) -> None:
        with pytest.raises(error, match=match):
            await pg.execute(_ins(column), [value])
        assert await pg.fetch_all("SELECT * FROM _hard_bind", None) == []

    async def test_deny_list_bypass_is_rejected(self, pg) -> None:
        await pg.execute("INSERT INTO _hard_bind (id) VALUES (7)", None)
        with pytest.raises(RuntimeError, match="out of range"):
            await pg.fetch_all("SELECT id FROM _hard_bind WHERE id = $1", [2**32 + 7])

    async def test_value_types_round_trip(self, pg) -> None:
        ts = datetime(2024, 5, 6, 7, 8, 9, 123456, tzinfo=timezone(timedelta(hours=2)))
        values = {
            "id": 2**31 - 1,
            "small": -(2**15),
            "big": 2**63 - 1,
            "f4": 3,
            "flag": True,
            "n": Decimal("-12345.678900"),
            "u": uuid.UUID("12345678-1234-5678-1234-567812345678"),
            "ts": ts,
            "t": datetime(2024, 5, 6, 7, 8, 9),
            "d": date(1999, 12, 31),
            "tm": time(23, 59, 58, 1),
            "j": {"a": [1, 2.5, None, 'x"y']},
            "b": b"\x00\x01\xff",
            "ints": [1, None, 3],
            "texts": ["a", None, "c"],
        }
        cols = ", ".join(values)
        marks = ", ".join(f"${i}" for i in range(1, len(values) + 1))
        await pg.execute(
            f"INSERT INTO _hard_bind ({cols}) VALUES ({marks})", list(values.values())
        )
        row = await pg.fetch_one(f"SELECT {cols} FROM _hard_bind", None)
        expected = dict(
            values, f4=3.0, ts=ts.astimezone(UTC), j='{"a": [1, 2.5, null, "x\\"y"]}'
        )
        assert row == expected
        for where, value in (("flag", True), ("u", str(values["u"])), ("n", 2**62)):
            sql = f"SELECT count(1) AS c FROM _hard_bind WHERE {where} = $1"
            assert (await pg.fetch_one(sql, [value]))["c"] == (where != "n")

    async def test_json_list(self, pg) -> None:
        await pg.execute(_ins("j"), [[1, "two", {"three": 3}]])
        row = await pg.fetch_one("SELECT j FROM _hard_bind", None)
        assert row == {"j": '[1, "two", {"three": 3}]'}


# ── PostgreSQL result decoding ──────────────────────────────────────────────


class DecodedRow(msgspec.Struct):
    ts: datetime
    t: datetime
    d: date
    tm: time
    u: uuid.UUID
    n: Decimal
    j: str
    jb: str
    by: bytes
    ints: list[int]
    texts: list[str | None]
    mood: str
    dom: int
    empty: None
    nul: int | None


DECODE_SQL = (
    "SELECT '2024-01-02 03:04:05.678901+00'::timestamptz AS ts, "
    "'2024-01-02 03:04:05'::timestamp AS t, '2024-02-29'::date AS d, "
    "'13:14:15.5'::time AS tm, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid AS u, "
    "'-1234567890.000123'::numeric AS n, '{\"k\": [1, 2]}'::json AS j, "
    "'{\"k\": [1, 2]}'::jsonb AS jb, '\\x00ff'::bytea AS by, "
    "ARRAY[1, 2, 3]::int4[] AS ints, ARRAY['x', NULL]::text[] AS texts, "
    "'happy'::_hard_mood AS mood, 5::_hard_posint AS dom, "
    "pg_sleep(0) AS empty, NULL::int AS nul"
)
EXPECTED = {
    "ts": datetime(2024, 1, 2, 3, 4, 5, 678901, tzinfo=UTC),
    "t": datetime(2024, 1, 2, 3, 4, 5),
    "d": date(2024, 2, 29),
    "tm": time(13, 14, 15, 500000),
    "u": uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
    "n": Decimal("-1234567890.000123"),
    "j": '{"k": [1, 2]}',
    "jb": '{"k": [1, 2]}',
    "by": b"\x00\xff",
    "ints": [1, 2, 3],
    "texts": ["x", None],
    "mood": "happy",
    "dom": 5,
    "empty": None,
    "nul": None,
}


@pytest.fixture
async def pg_types():
    engine = await native.connect_pg(PG_URL)
    for sql in (
        "DROP TYPE IF EXISTS _hard_mood",
        "DROP DOMAIN IF EXISTS _hard_posint",
        "CREATE TYPE _hard_mood AS ENUM ('sad', 'happy')",
        "CREATE DOMAIN _hard_posint AS int4 CHECK (VALUE > 0)",
    ):
        await engine.execute(sql, None)
    try:
        yield engine
    finally:
        await engine.execute("DROP TYPE IF EXISTS _hard_mood", None)
        await engine.execute("DROP DOMAIN IF EXISTS _hard_posint", None)
        await engine.close()


@needs_pg
class TestPgDecoding:
    async def test_dict_path(self, pg_types) -> None:
        row = await pg_types.fetch_one(DECODE_SQL, None)
        assert row == EXPECTED
        assert (row["ts"].tzinfo, row["t"].tzinfo) == (UTC, None)
        assert await pg_types.fetch_all(DECODE_SQL, None) == [EXPECTED]

    async def test_msgpack_path(self, pg_types) -> None:
        one = await pg_types.fetch_one_msgpack(DECODE_SQL, None)
        assert (
            msgspec.structs.asdict(msgspec.msgpack.decode(one, type=DecodedRow))
            == EXPECTED
        )
        rows = msgspec.msgpack.decode(
            await pg_types.fetch_all_msgpack(DECODE_SQL, None), type=list[DecodedRow]
        )
        assert [msgspec.structs.asdict(r) for r in rows] == [EXPECTED]

    @pytest.mark.parametrize(
        "method", ["fetch_one", "fetch_all", "fetch_one_msgpack", "fetch_all_msgpack"]
    )
    @pytest.mark.parametrize(
        ("sql", "match"),
        [
            ("SELECT point(1, 2) AS p", "unsupported PostgreSQL result type point"),
            (
                "SELECT '1 day'::interval AS i",
                "unsupported PostgreSQL result type interval",
            ),
            ("SELECT 'infinity'::timestamptz AS ts", "out of range"),
            ("SELECT '0001-01-01 BC'::date AS d", "out of range"),
            ("SELECT '24:00'::time AS t", "out of range"),
            ("SELECT ('1' || repeat('0', 30))::numeric AS n", "more than 28"),
            ("SELECT 'NaN'::numeric AS n", "NaN"),
        ],
    )
    async def test_unrepresentable_values_raise(self, pg_types, method, sql, match):
        with pytest.raises(RuntimeError, match=match):
            await getattr(pg_types, method)(sql, None)


@needs_pg
async def test_pg_model_hydration_round_trips_all_types() -> None:
    db = Database(PG_URL)
    await db.connect()
    assert isinstance(db._driver, pg_mod.PostgresNativeDriver)
    ModelRegistry.bind(db)

    class HardEvent(AUniqORMBase, table="_hard_events"):
        id: int = PrimaryKey()
        name: str = Column(nullable=False)
        active: bool = Column(nullable=False)
        created_at: datetime = Column(nullable=False)
        day: date = Column(nullable=False)
        at: time = Column(nullable=False)
        ref: uuid.UUID = Column(nullable=False)
        amount: Decimal = Column(nullable=False)
        blob: bytes = Column(nullable=False)
        revoked_at: datetime | None = None

    class HardDoc(AUniqORMBase, table="_hard_docs"):
        id: int = PrimaryKey()
        data: dict = Column(nullable=False)
        tags: list = Column(nullable=False)

    try:
        for model in (HardEvent, HardDoc):
            await model.drop_table()
            await model.create_table()
        created = datetime(2025, 1, 2, 3, 4, 5, 6, tzinfo=UTC)
        values = {
            "name": "a",
            "active": True,
            "created_at": created,
            "day": date(2025, 1, 2),
            "at": time(3, 4, 5),
            "ref": uuid.uuid4(),
            "amount": Decimal("10.50"),
            "blob": b"\x01\x02",
            "revoked_at": created + timedelta(days=1),
        }
        await HardEvent(**values).insert()
        await HardEvent.bulk_insert(
            [HardEvent(**dict(values, name=f"b{i}")) for i in range(3)]
        )
        got = await HardEvent.get(name="a")
        assert {key: getattr(got, key) for key in values} == values
        assert len(await HardEvent.filter(created_at=created)) == 4
        rows = await db.fetch_all("SELECT * FROM _hard_events ORDER BY id")
        assert rows[0]["revoked_at"] == values["revoked_at"]
        await HardDoc(data={"k": [1, "x"]}, tags=["a", "b"]).insert()
        doc = await HardDoc.get(id=1)
        assert (doc.data, doc.tags) == ({"k": [1, "x"]}, ["a", "b"])
    finally:
        for model in (HardEvent, HardDoc):
            await model.drop_table()
        await db.disconnect()


# ── Handle contract and session reset ───────────────────────────────────────


class _Backend:
    """Per-database SQL for the handle-contract tests."""

    def __init__(self, name: str) -> None:
        self.pg = name == "pg"
        self.ph = "$1" if self.pg else "?"
        self.insert = f"INSERT INTO _hard_txn (id) VALUES ({self.ph})"
        self.conn_id = (
            f"SELECT {'pg_backend_pid()' if self.pg else 'CONNECTION_ID()'} AS cid"
        )
        self.sleep = "SELECT pg_sleep(0.5)" if self.pg else "SELECT SLEEP(0.5)"

    async def connect(self, max_size: int = 1):
        connect = native.connect_pg if self.pg else native.connect_mysql
        return await connect(
            PG_URL if self.pg else MYSQL_URL, max_size=max_size, min_size=1
        )

    async def ids(self) -> list[int]:
        """Committed rows, read through a separate engine."""
        other = await self.connect()
        try:
            rows = await other.fetch_all("SELECT id FROM _hard_txn ORDER BY id", None)
        finally:
            await other.close()
        return [row["id"] for row in rows]

    async def dirty_session(self, handle) -> None:
        """Change session state that must not survive a return to the pool."""
        statements = (
            (
                "SET app.x = 'leak'",
                "CREATE TEMP TABLE _hard_tmp (a int)",
                "SELECT pg_advisory_lock(4242)",
                "LISTEN _hard_channel",
                "SET statement_timeout = 1234",
            )
            if self.pg
            else (
                "SET @x = 'leak'",
                "CREATE TEMPORARY TABLE _hard_tmp (a int)",
                "SELECT GET_LOCK('_hard_lock', 0)",
                "SET SESSION wait_timeout = 1234",
            )
        )
        for sql in statements:
            await handle.execute(sql, None)

    async def assert_clean(self, engine) -> None:
        if self.pg:
            row = await engine.fetch_one(
                "SELECT current_setting('app.x', true) AS x, "
                "to_regclass('pg_temp._hard_tmp') IS NULL AS no_tmp, "
                "(SELECT count(1) FROM pg_locks WHERE locktype = 'advisory' "
                "AND pid = pg_backend_pid()) = 0 AS no_locks, "
                "(SELECT count(1) FROM pg_listening_channels()) = 0 AS no_listen, "
                "current_setting('statement_timeout') = '0' AS timeout_reset",
                None,
            )
            assert row.pop("x") in (None, "")
        else:
            row = await engine.fetch_one(
                "SELECT @x IS NULL AS x, IS_USED_LOCK('_hard_lock') IS NULL AS lk, "
                "@@session.wait_timeout = @@global.wait_timeout AS wt",
                None,
            )
            with pytest.raises(RuntimeError, match="doesn't exist"):
                await engine.fetch_all("SELECT a FROM _hard_tmp", None)
        assert all(row.values()), row


@pytest.fixture(
    params=[
        pytest.param("pg", marks=needs_pg),
        pytest.param("mysql", marks=needs_mysql),
    ]
)
async def backend(request):
    be = _Backend(request.param)
    engine = await be.connect()
    await engine.execute("DROP TABLE IF EXISTS _hard_txn", None)
    await engine.execute("CREATE TABLE _hard_txn (id int primary key)", None)
    try:
        yield be, engine
    finally:
        await engine.execute("DROP TABLE IF EXISTS _hard_txn", None)
        await engine.close()


class TestHandleContract:
    async def test_transaction_mode(self, backend) -> None:
        be, engine = backend
        handle = await engine.begin()
        assert handle.closed is False
        with pytest.raises(RuntimeError, match="only valid on a pinned"):
            await handle.begin()
        await handle.execute(be.insert, [1])
        assert await handle.fetch_all("SELECT id FROM _hard_txn", None) == [{"id": 1}]
        await handle.commit()
        assert handle.closed is True
        for call in (
            lambda: handle.execute("SELECT 1", None),
            lambda: handle.fetch_all("SELECT 1", None),
            lambda: handle.fetch_one("SELECT 1", None),
            lambda: handle.fetch_all_msgpack("SELECT 1", None),
            lambda: handle.fetch_one_msgpack("SELECT 1", None),
            handle.commit,
            handle.rollback,
            handle.begin,
        ):
            with pytest.raises(RuntimeError, match="already ended"):
                await call()
        await handle.release()  # idempotent
        handle = await engine.begin()
        await handle.execute(be.insert, [2])
        await handle.rollback()
        with pytest.raises(RuntimeError, match="already ended"):
            await handle.rollback()
        assert await engine.fetch_all("SELECT id FROM _hard_txn", None) == [{"id": 1}]

    async def test_pinned_mode(self, backend) -> None:
        be, engine = backend
        handle = await engine.acquire()
        cid = (await handle.fetch_one(be.conn_id, None))["cid"]
        for value, end in ((3, handle.commit), (4, handle.rollback)):
            await handle.begin()
            await handle.execute(be.insert, [value])
            await end()
            assert handle.closed is False
        assert (await handle.fetch_one(be.conn_id, None))["cid"] == cid
        await handle.begin()
        await handle.execute(be.insert, [5])
        await handle.release()  # rolls back the open transaction
        assert handle.closed is True
        await handle.release()
        for call in (handle.begin, lambda: handle.fetch_one("SELECT 1", None)):
            with pytest.raises(RuntimeError, match="already ended"):
                await call()
        assert await engine.fetch_all("SELECT id FROM _hard_txn", None) == [{"id": 3}]

    @pytest.mark.parametrize("end", ["commit", "rollback", "release"])
    async def test_session_state_is_reset(self, backend, end) -> None:
        be, engine = backend
        cid = (await engine.fetch_one(be.conn_id, None))["cid"]
        handle = await (engine.acquire() if end == "release" else engine.begin())
        if end == "release":
            await handle.begin()
        await be.dirty_session(handle)
        await getattr(handle, end)()
        # max_size=1: the pooled query runs on the very same connection.
        assert (await engine.fetch_one(be.conn_id, None))["cid"] == cid
        await be.assert_clean(engine)

    async def test_dropped_handle_is_rolled_back_and_reset(self, backend) -> None:
        be, engine = backend
        cid = (await engine.fetch_one(be.conn_id, None))["cid"]
        handle = await engine.begin()
        await handle.execute(be.insert, [6])
        await be.dirty_session(handle)
        del handle
        gc.collect()
        # Not run inside the leaked transaction: committed on its own.
        await engine.execute(be.insert, [7])
        assert (await engine.fetch_one(be.conn_id, None))["cid"] == cid
        await be.assert_clean(engine)
        assert await be.ids() == [7]

    async def test_cancelled_task_does_not_leak_transaction(self, backend) -> None:
        be, engine = backend
        started = asyncio.Event()

        async def work() -> None:
            handle = await engine.begin()
            await handle.execute(be.insert, [8])
            started.set()
            await handle.fetch_all(be.sleep, None)
            await handle.commit()

        task = asyncio.create_task(work())
        await started.wait()
        await asyncio.sleep(0.1)  # let the sleep query reach the server
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        del task
        gc.collect()
        await engine.execute(be.insert, [9])
        assert await be.ids() == [9]

    async def test_concurrent_use_of_one_handle(self, backend) -> None:
        be, engine = backend
        handle = await engine.begin()
        await asyncio.gather(*(handle.execute(be.insert, [i]) for i in range(20)))
        sql = "SELECT COUNT(1) AS c FROM _hard_txn"
        counts = await asyncio.gather(*(handle.fetch_one(sql, None) for _ in range(10)))
        assert {c["c"] for c in counts} == {20}
        await handle.commit()
        assert len(await engine.fetch_all("SELECT id FROM _hard_txn", None)) == 20


@needs_pg
async def test_pg_commit_failure_reports_error_and_cleans_up() -> None:
    engine = await native.connect_pg(PG_URL, max_size=1)
    try:
        for sql in (
            "DROP TABLE IF EXISTS _hard_child",
            "DROP TABLE IF EXISTS _hard_parent",
            "CREATE TABLE _hard_parent (id int primary key)",
            "CREATE TABLE _hard_child (pid int REFERENCES _hard_parent(id) "
            "DEFERRABLE INITIALLY DEFERRED)",
        ):
            await engine.execute(sql, None)
        handle = await engine.begin()
        await handle.execute("SET app.x = 'leak'", None)
        await handle.execute("INSERT INTO _hard_child VALUES (1)", None)
        with pytest.raises(RuntimeError, match="foreign key"):
            await handle.commit()
        assert handle.closed
        sql = (
            "SELECT current_setting('app.x', true) AS x, "
            "(SELECT count(1) FROM _hard_child) AS n"
        )
        row = await engine.fetch_one(sql, None)
        assert (row["x"] in (None, ""), row["n"]) == (True, 0)
    finally:
        await engine.execute("DROP TABLE IF EXISTS _hard_child", None)
        await engine.execute("DROP TABLE IF EXISTS _hard_parent", None)
        await engine.close()


@needs_mysql
async def test_mysql_ints_never_round() -> None:
    engine = await native.connect_mysql(MYSQL_URL)
    try:
        with pytest.raises(OverflowError):
            await engine.fetch_one("SELECT ? AS v", [2**64])
        row = await engine.fetch_one("SELECT CAST(? AS UNSIGNED) AS v", [2**64 - 1])
        assert row == {"v": 2**64 - 1}
        assert await engine.fetch_one("SELECT ? AS v", [True]) == {"v": 1}
    finally:
        await engine.close()


async def _prepares(handle) -> int:
    row = await handle.fetch_one("SHOW SESSION STATUS LIKE 'Com_stmt_prepare'", None)
    return int(row["Value"])


@needs_mysql
class TestMySqlBackgroundReset:
    """MySQL resets released connections after COMMIT/ROLLBACK resolves."""

    async def test_statements_are_prepared_again_after_reset(self) -> None:
        engine = await native.connect_mysql(MYSQL_URL, max_size=1)
        try:
            handle = await engine.begin()
            await handle.execute("SET @x = 'leak'", None)
            await handle.fetch_one("SELECT ? + 1 AS v", [1])
            await _prepares(handle)
            await handle.commit()
            # max_size=1: this waits until the background reset has finished.
            handle = await engine.begin()
            assert await handle.fetch_one("SELECT @x AS x", None) == {"x": None}
            before = await _prepares(handle)
            assert await handle.fetch_one("SELECT ? + 1 AS v", [2]) == {"v": 3}
            assert await _prepares(handle) == before
            await handle.fetch_one("SELECT ? + 2 AS v", [2])
            assert await _prepares(handle) == before + 1
            await handle.rollback()
            assert await engine.fetch_one("SELECT ? + 2 AS v", [1]) == {"v": 3}
        finally:
            await engine.close()

    async def test_close_waits_for_background_reset(self) -> None:
        # The re-prepare runs only after a successful reset, so a second global
        # prepare proves the background task finished before close() returned.
        status = "SHOW GLOBAL STATUS LIKE 'Com_stmt_prepare'"
        probe = await native.connect_mysql(MYSQL_URL)
        try:
            start = int((await probe.fetch_one(status, None))["Value"])
            engine = await native.connect_mysql(MYSQL_URL)
            handle = await engine.begin()
            await handle.execute("SELECT GET_LOCK('_hard_close', 0) + ?", [0])
            await handle.commit()
            await engine.close()
            assert int((await probe.fetch_one(status, None))["Value"]) >= start + 2
            sql = "SELECT IS_FREE_LOCK('_hard_close') AS f"
            assert await probe.fetch_one(sql, None) == {"f": 1}
        finally:
            await probe.close()

    async def test_commit_after_cancelled_operation_reports_error(self) -> None:
        engine = await native.connect_mysql(MYSQL_URL, max_size=1)
        try:
            await engine.execute("DROP TABLE IF EXISTS _hard_cancel", None)
            await engine.execute("CREATE TABLE _hard_cancel (id int)", None)
            handle = await engine.begin()
            await handle.execute("INSERT INTO _hard_cancel VALUES (1)", None)
            task = asyncio.ensure_future(handle.fetch_all("SELECT SLEEP(0.5)", None))
            await asyncio.sleep(0.1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            with pytest.raises(RuntimeError, match="discarded"):
                await handle.commit()
            assert await engine.fetch_all("SELECT id FROM _hard_cancel", None) == []
        finally:
            await engine.execute("DROP TABLE IF EXISTS _hard_cancel", None)
            await engine.close()


# ── MongoDB keyword options ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("url", "options", "expected"),
    [
        ("mongodb://h/db?tz_aware=true&w=1", None, "mongodb://h/db?w=1"),
        ("mongodb://h/db", {"tz_aware": True, "connect": False}, "mongodb://h/db"),
        (
            "mongodb://h/db?tls=false&w=1&replicaSet=old",
            {
                "tls": True,
                "tlsCAFile": "/certs dir/ca",
                "replicaset": "rs0",
                "serverSelectionTimeoutMS": 5000.0,
            },
            "mongodb://h/db?w=1&tls=true&tlscafile=%2Fcerts%20dir%2Fca&replicaset=rs0"
            "&serverselectiontimeoutms=5000",
        ),
        (
            "mongodb://h1,h2/db",
            {"username": "us@r", "password": "p:/?#[]@"},
            "mongodb://us%40r:p%3A%2F%3F%23%5B%5D%40@h1,h2/db",
        ),
    ],
)
def test_mongo_native_url(url, options, expected) -> None:
    assert mongo._native_url(url, options) == expected


@pytest.mark.parametrize(
    ("url", "options"),
    [
        ("mongodb://h/db", {"tlsContext": ssl.create_default_context()}),
        ("mongodb://h/db", {"event_listeners": [object()]}),
        ("mongodb://h/db", {"type_registry": object()}),
        ("mongodb://h/db", {"document_class": dict}),
        ("mongodb://h/db", {"server_selector": lambda servers: servers}),
        ("mongodb://h/db", {"readPreference": object()}),
        ("mongodb://h/db", {"tlsAllowInvalidHostnames": True}),
        ("mongodb://h/db", {"socketTimeoutMS": 1000}),
        ("mongodb://h/db", {"authMechanismProperties": {"SERVICE_NAME": "x"}}),
        ("mongodb://h/db", {"connectTimeoutMS": 1.5}),
        ("mongodb://h/db", {"password": "x"}),
        ("mongodb://u:p@h/db", {"username": "u"}),
    ],
)
def test_mongo_unrepresentable_options_disable_native(url, options) -> None:
    assert mongo._native_url(url, options) is None


def test_mongo_logging_never_contains_credentials(caplog) -> None:
    caplog.set_level("INFO", logger="auniqorm")
    options = {"username": "u", "password": "s3cret", "event_listeners": []}
    assert mongo._native_url("mongodb://h/db", options) is None
    assert "event_listeners" in caplog.text and "s3cret" not in caplog.text


@pytest.fixture
def mocked_mongo(monkeypatch):
    client = MagicMock(aconnect=AsyncMock(), codec_options=MagicMock())
    pymongo_client = MagicMock(return_value=client)
    connect = AsyncMock(return_value=MagicMock(close=AsyncMock()))
    monkeypatch.setattr(mongo, "AsyncMongoClient", pymongo_client)
    monkeypatch.setattr(
        mongo, "auniqorm_native", SimpleNamespace(connect_mongo=connect)
    )
    monkeypatch.setattr(mongo, "_attach_bridge", lambda engine: None)
    monkeypatch.delenv("AUNIQORM_MONGODB_NATIVE", raising=False)
    return pymongo_client, connect


async def test_mongo_native_client_receives_keyword_options(mocked_mongo) -> None:
    pymongo_client, connect = mocked_mongo
    options = {"tls": True, "tlsCAFile": "/ca", "username": "u", "password": "p"}
    driver = mongo.MongoDriver("mongodb://h/db", **options)
    await driver.connect()
    assert pymongo_client.call_args.kwargs == options
    connect.assert_awaited_once_with(
        "mongodb://u:p@h/db?tls=true&tlscafile=%2Fca", "db"
    )
    assert driver._native is not None


async def test_mongo_unrepresentable_option_disables_native(mocked_mongo) -> None:
    _, connect = mocked_mongo
    driver = mongo.MongoDriver("mongodb://h/db", tls=True, event_listeners=[object()])
    await driver.connect()
    connect.assert_not_awaited()
    assert driver._native is None and driver.is_connected
