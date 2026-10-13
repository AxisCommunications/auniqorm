# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Smoke tests for the benchmark harness.

Runs each scenario at small scale using only the aUniqORM harness to verify
correctness and catch regressions without needing benchmark dependencies.
"""

import pytest

from benchmarks.orm_comparison import AUniqORMHarness, YaraHarness, build_payloads

SCENARIOS = [
    "insert",
    "get",
    "filter",
    "aggregate",
    "update_expr",
    "get_or_create",
    "partial_select",
    "prefetch",
    "m2m",
]


@pytest.fixture
async def harness(tmp_path):
    db_path = tmp_path / "bench.sqlite3"
    db_url = f"sqlite:///{db_path.as_posix()}"
    h = AUniqORMHarness(db_url, table_name="bench_users", db_name="sqlite")
    await h.create_schema()
    yield h
    await h.close()


@pytest.fixture
async def yara_harness(tmp_path):
    db_path = tmp_path / "yara.sqlite3"
    db_url = f"sqlite:///{db_path.as_posix()}"
    h = YaraHarness(db_url, table_name="yara_bench_users", db_name="sqlite")
    await h.create_schema()
    yield h
    await h.close()


async def _run_scenario(harness, scenario):
    """Run a scenario against a benchmark adapter with a small dataset."""
    payloads = build_payloads(50)
    relational = {"prefetch", "m2m"}

    if scenario in relational:
        await harness._seed_relational(50)
    elif scenario != "insert":
        await harness.seed(payloads)

    if scenario == "insert":
        await harness.insert_rows(payloads)
    elif scenario == "get":
        await harness.point_lookups([1, 2, 3, 4, 5])
    elif scenario == "filter":
        await harness.equality_filters([0, 1, 2])
    elif scenario == "aggregate":
        assert await harness.aggregate(1) == 50
    elif scenario == "update_expr":
        await harness.bulk_update_expr([0, 1, 2])
    elif scenario == "get_or_create":
        await harness.get_or_create(5)
    elif scenario == "partial_select":
        await harness.partial_select([0, 1, 2])
    elif scenario == "prefetch":
        await harness.prefetch(3)
    elif scenario == "m2m":
        await harness.m2m_query(3)


@pytest.mark.parametrize("scenario", SCENARIOS)
async def test_benchmark_scenario(harness, scenario):
    """Each benchmark scenario completes without error at small scale."""
    await _run_scenario(harness, scenario)


@pytest.mark.parametrize("scenario", SCENARIOS)
async def test_yara_benchmark_scenario(yara_harness, scenario):
    """Yara ORM completes the same scenario contract as aUniqORM."""
    await _run_scenario(yara_harness, scenario)
