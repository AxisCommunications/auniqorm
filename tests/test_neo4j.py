"""Focused unit tests for the optional Neo4j backend."""

from unittest.mock import AsyncMock, Mock

import pytest

from auniqorm.connection import Database
from auniqorm.drivers import neo4j
from auniqorm.drivers.registry import DriverRegistry


class _Result:
    summary = "summary"

    def __init__(self, rows):
        self._rows = rows

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for row in self._rows:
            yield row

    async def consume(self):
        return self.summary


class _Record:
    def __init__(self, values):
        self._values = values

    def data(self):
        return self._values


class _Session:
    def __init__(self, result):
        self.result = result
        self.run = AsyncMock(return_value=result)
        self.close = AsyncMock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


@pytest.fixture
def fake_neo4j(monkeypatch):
    driver = Mock()
    session = _Session(_Result([_Record({"name": "Ada", "age": 37})]))
    driver.session.return_value = session
    driver.close = AsyncMock()
    factory = Mock()
    factory.driver.return_value = driver
    monkeypatch.setattr(neo4j, "AsyncGraphDatabase", factory)
    return factory, driver, session


def test_neo4j_protocol_aliases():
    assert Database._parse_protocol("neo4j://localhost") == "neo4j"
    assert Database._parse_protocol("bolt://localhost") == "neo4j"


def test_registry_discovers_neo4j():
    assert DriverRegistry.get("neo4j") is neo4j.Neo4jDriver
    assert DriverRegistry.get("bolt") is neo4j.Neo4jDriver


@pytest.mark.asyncio
async def test_neo4j_driver_queries_and_parameters(fake_neo4j):
    _factory, driver, session = fake_neo4j
    backend = neo4j.Neo4jDriver("neo4j://alice:secret@localhost:7687/app")
    await backend.connect()

    assert await backend.fetch_one("MATCH (n) WHERE n.id = ? RETURN n", [7]) == {
        "name": "Ada",
        "age": 37,
    }
    await backend.execute("CREATE (n {id: ?})", [7])

    driver.session.assert_called_with(database="app")
    assert session.run.await_args_list[0].args == (
        "MATCH (n) WHERE n.id = $p0 RETURN n",
        {"p0": 7},
    )
    assert session.run.await_args_list[1].args == (
        "CREATE (n {id: $p0})",
        {"p0": 7},
    )
    _factory.driver.assert_called_once_with(
        "neo4j://alice:secret@localhost:7687/app",
        auth=("alice", "secret"),
    )
    await backend.disconnect()
    driver.close.assert_awaited_once()
