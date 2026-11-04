# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Behavior tests for the synchronous adapter (auniqorm.sync).

These run against a real file-backed SQLite database on the adapter's shared
background loop — no ``await``, no pytest-asyncio.
"""

from __future__ import annotations

import asyncio
import contextvars
import threading
from pathlib import Path

import pytest

from auniqorm import AUniqORMBase, Column, PrimaryKey
from auniqorm.models import ModelRegistry
from auniqorm.sync import SyncDatabase, run, sync_model


@pytest.fixture(autouse=True)
def _clear_registry():
    ModelRegistry.clear()
    yield
    ModelRegistry.clear()


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    return f"sqlite:///{(tmp_path / 'sync.db').as_posix()}"


@pytest.fixture
def user_model() -> type[AUniqORMBase]:
    class User(AUniqORMBase, table="users"):
        id: int = PrimaryKey()
        name: str = Column(nullable=False)
        email: str = Column(unique=True)
        score: int = Column(nullable=False, default=0)

    return User


def test_context_manager_connects_and_disconnects(
    db_url: str, user_model: type[AUniqORMBase]
) -> None:
    with SyncDatabase(db_url) as db:
        assert db.is_connected is True
        assert db.protocol == "sqlite"
    assert db.is_connected is False


def test_insert_and_get_roundtrip(db_url: str, user_model: type[AUniqORMBase]) -> None:
    with SyncDatabase(db_url):
        users = sync_model(user_model)
        alice = users(name="Alice", email="alice@example.com", score=10)
        alice.insert()
        assert alice.id is not None

        found = users.get(email="alice@example.com")
        assert found is not None
        assert found.name == "Alice"
        assert found.score == 10


def test_get_missing_returns_none(db_url: str, user_model: type[AUniqORMBase]) -> None:
    with SyncDatabase(db_url):
        users = sync_model(user_model)
        assert users.get(email="nobody@example.com") is None


def test_filter_returns_wrapped_list(
    db_url: str, user_model: type[AUniqORMBase]
) -> None:
    with SyncDatabase(db_url):
        users = sync_model(user_model)
        for i in range(3):
            users(name=f"U{i}", email=f"u{i}@example.com", score=i).insert()

        rows = users.filter(order_by="-id")
        assert [u.name for u in rows] == ["U2", "U1", "U0"]


def test_count_and_exists(db_url: str, user_model: type[AUniqORMBase]) -> None:
    with SyncDatabase(db_url):
        users = sync_model(user_model)
        users(name="A", email="a@example.com", score=1).insert()
        users(name="B", email="b@example.com", score=1).insert()

        assert users.count(score=1) == 2
        assert users.exists(email="a@example.com") is True
        assert users.exists(email="missing@example.com") is False


def test_update_and_delete_instance(
    db_url: str, user_model: type[AUniqORMBase]
) -> None:
    with SyncDatabase(db_url):
        users = sync_model(user_model)
        user = users(name="Carol", email="carol@example.com", score=5)
        user.insert()

        user.update(score=99)
        refreshed = users.get(email="carol@example.com")
        assert refreshed is not None
        assert refreshed.score == 99

        refreshed.delete()
        assert users.get(email="carol@example.com") is None


def test_raw_query_helpers(db_url: str, user_model: type[AUniqORMBase]) -> None:
    with SyncDatabase(db_url) as db:
        db.execute(
            "INSERT INTO users (name, email, score) VALUES (?, ?, ?)",
            ["Dave", "dave@example.com", 7],
        )
        row = db.fetch_one(
            "SELECT name, score FROM users WHERE email = ?",
            ["dave@example.com"],
        )
        assert row == {"name": "Dave", "score": 7}
        assert db.fetch_val("SELECT COUNT(*) FROM users") == 1
        assert db.fetch_all("SELECT name FROM users") == [{"name": "Dave"}]


def test_iterate_yields_rows(db_url: str, user_model: type[AUniqORMBase]) -> None:
    with SyncDatabase(db_url) as db:
        users = sync_model(user_model)
        for i in range(5):
            users(name=f"N{i}", email=f"n{i}@example.com", score=i).insert()

        names = [
            row["name"] for row in db.iterate("SELECT name FROM users ORDER BY id")
        ]
        assert names == ["N0", "N1", "N2", "N3", "N4"]


def test_transaction_commit(db_url: str, user_model: type[AUniqORMBase]) -> None:
    with SyncDatabase(db_url) as db:
        users = sync_model(user_model)
        with db.transaction():
            users(name="Tx", email="tx@example.com", score=1).insert()
        assert users.exists(email="tx@example.com") is True


def test_transaction_rollback(db_url: str, user_model: type[AUniqORMBase]) -> None:
    with SyncDatabase(db_url) as db:
        users = sync_model(user_model)
        with pytest.raises(RuntimeError, match="boom"):
            with db.transaction():
                users(name="Tx", email="tx@example.com", score=1).insert()
                raise RuntimeError("boom")
        assert users.exists(email="tx@example.com") is False


def test_sync_model_cached(db_url: str, user_model: type[AUniqORMBase]) -> None:
    assert sync_model(user_model) is sync_model(user_model)


def test_run_escape_hatch(db_url: str, user_model: type[AUniqORMBase]) -> None:
    with SyncDatabase(db_url):
        users = sync_model(user_model)
        users(name="Eve", email="eve@example.com", score=3).insert()
        # Reach the underlying async model coroutine directly.
        found = run(user_model.get(email="eve@example.com"))
        assert found is not None
        assert found.name == "Eve"


def test_no_event_loop_on_calling_thread(
    db_url: str, user_model: type[AUniqORMBase]
) -> None:
    with SyncDatabase(db_url) as db:
        db.fetch_val("SELECT 1")
        # The caller thread must remain a plain sync thread (no running loop).
        with pytest.raises(RuntimeError):
            asyncio.get_running_loop()
        assert threading.current_thread() is threading.main_thread()


_CONTEXT_PROBE: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "auniqorm_sync_context_probe", default=None
)


def test_sync_calls_share_a_context_per_thread() -> None:
    async def set_value(value: str) -> None:
        _CONTEXT_PROBE.set(value)

    async def get_value() -> str | None:
        return _CONTEXT_PROBE.get()

    run(set_value("main"))
    assert run(get_value()) == "main"

    seen: list[str | None] = []

    def other_thread() -> None:
        seen.append(run(get_value()))
        run(set_value("other"))
        seen.append(run(get_value()))

    thread = threading.Thread(target=other_thread)
    thread.start()
    thread.join()

    assert seen == [None, "other"]
    assert run(get_value()) == "main"
