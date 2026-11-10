# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for aggregate functions and values() with GROUP BY."""

import pytest

from auniqorm.columns import PrimaryKey
from auniqorm.models import (
    Aggregate,
    AUniqORMBase,
    Avg,
    Count,
    Max,
    Min,
    Sum,
)

from ._helpers import ready


class Order(AUniqORMBase):
    id: int = PrimaryKey()
    status: str
    amount: float
    customer_id: int


@pytest.fixture
async def seeded_db(db):
    """Create table and seed data."""
    await ready(db, Order, register=True)

    await Order(status="completed", amount=100.0, customer_id=1).insert()
    await Order(status="completed", amount=200.0, customer_id=1).insert()
    await Order(status="completed", amount=150.0, customer_id=2).insert()
    await Order(status="pending", amount=50.0, customer_id=2).insert()
    await Order(status="pending", amount=75.0, customer_id=3).insert()
    await Order(status="cancelled", amount=30.0, customer_id=3).insert()

    return db


# ---------------------------------------------------------------------------
# Aggregate expression classes
# ---------------------------------------------------------------------------


class TestAggregateExpressions:
    @pytest.mark.parametrize(
        "cls,column,expected",
        [
            pytest.param(Count, "id", "COUNT(id)", id="count"),
            pytest.param(Sum, "amount", "SUM(amount)", id="sum"),
            pytest.param(Avg, "amount", "AVG(amount)", id="avg"),
            pytest.param(Min, "amount", "MIN(amount)", id="min"),
            pytest.param(Max, "amount", "MAX(amount)", id="max"),
        ],
    )
    def test_to_sql(self, cls, column, expected):
        assert cls(column).to_sql() == expected

    def test_count_star(self):
        assert Count().to_sql() == "COUNT(*)"

    def test_count_distinct(self):
        assert Count("status", distinct=True).to_sql() == "COUNT(DISTINCT status)"

    def test_repr(self):
        assert repr(Count("id")) == "Count('id')"
        assert repr(Sum("amount")) == "Sum('amount')"

    def test_base_class(self):
        agg = Aggregate("col")
        assert agg.to_sql() == "(col)"  # no function name on base


# ---------------------------------------------------------------------------
# Model.aggregate()
# ---------------------------------------------------------------------------


class TestAggregate:
    async def test_count_all(self, seeded_db):
        result = await Order.aggregate(count=Count("id"))
        assert result["count"] == 6

    async def test_sum(self, seeded_db):
        result = await Order.aggregate(total=Sum("amount"))
        assert result["total"] == 605.0

    async def test_avg(self, seeded_db):
        result = await Order.aggregate(avg=Avg("amount"))
        assert abs(result["avg"] - 100.83) < 0.1

    async def test_min_max(self, seeded_db):
        result = await Order.aggregate(
            low=Min("amount"),
            high=Max("amount"),
        )
        assert result["low"] == 30.0
        assert result["high"] == 200.0

    async def test_filtered_aggregate(self, seeded_db):
        result = await Order.aggregate(
            status="completed",
            total=Sum("amount"),
            count=Count("id"),
        )
        assert result["total"] == 450.0
        assert result["count"] == 3

    async def test_count_distinct(self, seeded_db):
        result = await Order.aggregate(
            unique_customers=Count("customer_id", distinct=True),
        )
        assert result["unique_customers"] == 3

    async def test_no_aggregates_raises(self, seeded_db):
        with pytest.raises(ValueError, match="at least one Aggregate"):
            await Order.aggregate(status="completed")

    async def test_no_database_raises(self):
        class Unbound(AUniqORMBase, table="unbound_agg"):
            id: int = PrimaryKey()

        with pytest.raises(RuntimeError, match="not bound"):
            await Unbound.aggregate(count=Count("id"))


# ---------------------------------------------------------------------------
# Model.values()
# ---------------------------------------------------------------------------


class TestValues:
    async def test_select_columns(self, seeded_db):
        rows = await Order.values("id", "status", order_by="id")
        assert len(rows) == 6
        assert rows[0] == {"id": 1, "status": "completed"}

    async def test_flat(self, seeded_db):
        statuses = await Order.values("status", flat=True, order_by="id")
        assert statuses == [
            "completed",
            "completed",
            "completed",
            "pending",
            "pending",
            "cancelled",
        ]

    async def test_flat_requires_single_field(self, seeded_db):
        with pytest.raises(ValueError, match="flat=True requires exactly one"):
            await Order.values("id", "status", flat=True)

    async def test_group_by_with_aggregate(self, seeded_db):
        rows = await Order.values(
            "status",
            total=Sum("amount"),
            count=Count("id"),
            group_by="status",
            order_by="-total",
        )
        assert rows[0]["status"] == "completed"
        assert rows[0]["total"] == 450.0
        assert rows[0]["count"] == 3

    async def test_group_by_flat_aggregate(self, seeded_db):
        totals = await Order.values(
            total=Sum("amount"),
            group_by="status",
            order_by="-total",
            flat=True,
        )
        assert totals == [450.0, 125.0, 30.0]

    async def test_values_with_filter(self, seeded_db):
        rows = await Order.values(
            "customer_id",
            total=Sum("amount"),
            status="completed",
            group_by="customer_id",
            order_by="customer_id",
        )
        assert len(rows) == 2
        assert rows[0] == {"customer_id": 1, "total": 300.0}
        assert rows[1] == {"customer_id": 2, "total": 150.0}

    async def test_values_limit_offset(self, seeded_db):
        rows = await Order.values("id", order_by="id", limit=2, offset=2)
        assert len(rows) == 2
        assert rows[0]["id"] == 3

    async def test_no_fields_raises(self, seeded_db):
        with pytest.raises(ValueError, match="at least one field"):
            await Order.values()

    async def test_no_database_raises(self):
        class Unbound2(AUniqORMBase, table="unbound_val"):
            id: int = PrimaryKey()

        with pytest.raises(RuntimeError, match="not bound"):
            await Unbound2.values("id")
