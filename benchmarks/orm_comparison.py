# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from __future__ import annotations

import argparse
import asyncio
import gc
import statistics
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from auniqorm import (  # noqa: E402
    AUniqORMBase,
    Column,
    Count,
    Database,
    F,
    ForeignKey,
    ManyToMany,
    ModelRegistry,
    PrimaryKey,
    Sum,
)

# ── Database URL helpers ──────────────────────────────────────────────────────

DB_URLS = {
    "sqlite": None,  # generated per-iteration from a temp file
    "postgres": "postgresql://bench:bench@localhost:5432/bench",
    "mysql": "mysql://bench:bench@localhost:3306/bench",
}


def get_sa_url(db_name: str, db_path: Path | None = None) -> str:
    if db_name == "sqlite":
        assert db_path is not None
        return f"sqlite+aiosqlite:///{db_path.as_posix()}"
    if db_name == "postgres":
        return "postgresql+asyncpg://bench:bench@localhost:5432/bench"
    if db_name == "mysql":
        return "mysql+aiomysql://bench:bench@localhost:3306/bench"
    raise ValueError(f"Unknown database: {db_name}")


def get_tortoise_url(db_name: str, db_path: Path | None = None) -> str:
    if db_name == "sqlite":
        assert db_path is not None
        return f"sqlite://{db_path.as_posix()}"
    if db_name == "postgres":
        return "asyncpg://bench:bench@localhost:5432/bench"
    if db_name == "mysql":
        return "mysql://bench:bench@localhost:3306/bench"
    raise ValueError(f"Unknown database: {db_name}")


def get_auniqorm_url(db_name: str, db_path: Path | None = None) -> str:
    if db_name == "sqlite":
        assert db_path is not None
        return f"sqlite:///{db_path.as_posix()}"
    if db_name == "postgres":
        return "postgresql://bench:bench@localhost:5432/bench"
    if db_name == "mysql":
        return "mysql://bench:bench@localhost:3306/bench"
    raise ValueError(f"Unknown database: {db_name}")


def get_yara_url(db_name: str, db_path: Path | None = None) -> str:
    if db_name == "sqlite":
        assert db_path is not None
        return f"sqlite:///{db_path.as_posix()}"
    if db_name == "postgres":
        return "postgres://bench:bench@localhost:5432/bench"
    if db_name == "mysql":
        return "mysql://bench:bench@localhost:3306/bench"
    raise ValueError(f"Unknown database: {db_name}")


def is_sqlite(db_name: str) -> bool:
    return db_name == "sqlite"


@dataclass(slots=True)
class BenchmarkConfig:
    rows: int
    lookups: int
    filters: int
    repeats: int
    warmups: int
    database: str | None


@dataclass(slots=True)
class ScenarioReport:
    library: str
    scenario: str
    database: str
    operations: int
    timings: list[float]

    @property
    def median(self) -> float:
        return statistics.median(self.timings)

    @property
    def best(self) -> float:
        return min(self.timings)

    @property
    def ops_per_second(self) -> float:
        return self.operations / self.median


def build_payloads(row_count: int) -> list[dict[str, Any]]:
    return [
        {
            "email": f"user{index}@example.com",
            "name": f"User {index}",
            "score": index % 100,
        }
        for index in range(row_count)
    ]


def build_lookup_ids(row_count: int, iterations: int) -> list[int]:
    return [(index % row_count) + 1 for index in range(iterations)]


def build_filter_scores(iterations: int) -> list[int]:
    return [index % 100 for index in range(iterations)]


class Harness:
    name: str

    async def create_schema(self) -> None:
        raise NotImplementedError

    async def seed(self, payloads: Sequence[dict[str, Any]]) -> None:
        raise NotImplementedError

    async def insert_rows(self, payloads: Sequence[dict[str, Any]]) -> None:
        raise NotImplementedError

    async def point_lookups(self, identifiers: Sequence[int]) -> None:
        raise NotImplementedError

    async def equality_filters(self, scores: Sequence[int]) -> None:
        raise NotImplementedError

    async def aggregate(self, iterations: int) -> int:
        raise NotImplementedError

    async def bulk_update_expr(self, scores: Sequence[int]) -> None:
        raise NotImplementedError

    async def get_or_create(self, iterations: int) -> None:
        raise NotImplementedError

    async def partial_select(self, scores: Sequence[int]) -> None:
        raise NotImplementedError

    async def prefetch(self, iterations: int) -> None:
        raise NotImplementedError

    async def m2m_query(self, iterations: int) -> None:
        raise NotImplementedError

    async def _seed_relational(self, count: int) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError

    async def drop_tables(self) -> None:
        raise NotImplementedError


class AUniqORMHarness(Harness):
    name = "auniqorm"

    def __init__(self, db_url: str, table_name: str, db_name: str) -> None:
        self._database = Database(db_url)
        self._db_name = db_name
        ModelRegistry.clear()

        class BenchmarkUser(AUniqORMBase, table=table_name):
            id: int = PrimaryKey()
            email: str = Column(nullable=False, unique=True, index=True, max_length=255)
            name: str = Column(nullable=False, max_length=255)
            score: int = Column(nullable=False, index=True)

        self._model = BenchmarkUser

        # Relational models for prefetch/m2m scenarios
        class BenchAuthor(AUniqORMBase, table=f"{table_name}_authors"):
            id: int = PrimaryKey()
            name: str = Column(nullable=False, max_length=255)

        class BenchPost(AUniqORMBase, table=f"{table_name}_posts"):
            id: int = PrimaryKey()
            title: str = Column(nullable=False, max_length=255)
            author_id: int = ForeignKey(f"{table_name}_authors.id")

        class BenchTag(AUniqORMBase, table=f"{table_name}_tags"):
            id: int = PrimaryKey()
            label: str = Column(nullable=False, max_length=255)

        class BenchArticle(AUniqORMBase, table=f"{table_name}_articles"):
            id: int = PrimaryKey()
            title: str = Column(nullable=False, max_length=255)
            tags = ManyToMany(
                f"{table_name}_tags", through=f"{table_name}_article_tags"
            )

        self._author_model = BenchAuthor
        self._post_model = BenchPost
        self._tag_model = BenchTag
        self._article_model = BenchArticle

    async def create_schema(self) -> None:
        await self._database.connect()
        ModelRegistry.bind(self._database)
        await self._model.create_table()

    async def _create_relational_schema(self) -> None:
        await self._author_model.create_table()
        await self._post_model.create_table()
        await self._tag_model.create_table()
        await self._article_model.create_table()
        tbl = self._model.__table_name__
        await self._database.execute(
            f"CREATE TABLE IF NOT EXISTS {tbl}_article_tags ("
            f"{tbl}_article_id INTEGER REFERENCES {tbl}_articles(id), "
            f"{tbl}_tag_id INTEGER REFERENCES {tbl}_tags(id), "
            f"PRIMARY KEY ({tbl}_article_id, {tbl}_tag_id))"
        )

    async def seed(self, payloads: Sequence[dict[str, Any]]) -> None:
        async with await self._database.transaction():
            for payload in payloads:
                await self._model(**payload).insert()

    async def _seed_relational(self, count: int) -> None:
        """Seed authors/posts and articles/tags for relational benchmarks."""
        await self._create_relational_schema()
        num_authors = max(count // 10, 1)
        num_tags = 20

        async with await self._database.transaction():
            for i in range(num_authors):
                await self._author_model(name=f"Author {i}").insert()
            for i in range(count):
                await self._post_model(
                    title=f"Post {i}", author_id=(i % num_authors) + 1
                ).insert()
            for i in range(num_tags):
                await self._tag_model(label=f"tag-{i}").insert()
            for i in range(count):
                await self._article_model(title=f"Article {i}").insert()

        # Assign tags (each article gets 1-3 tags)
        tbl = self._model.__table_name__
        if self._db_name == "sqlite":
            insert_sql = (
                f"INSERT OR IGNORE INTO {tbl}_article_tags "
                f"({tbl}_article_id, {tbl}_tag_id) VALUES (?, ?)"
            )
        elif self._db_name == "postgres":
            insert_sql = (
                f"INSERT INTO {tbl}_article_tags "
                f"({tbl}_article_id, {tbl}_tag_id) VALUES ($1, $2) "
                f"ON CONFLICT DO NOTHING"
            )
        else:  # mysql
            insert_sql = (
                f"INSERT IGNORE INTO {tbl}_article_tags "
                f"({tbl}_article_id, {tbl}_tag_id) VALUES (%s, %s)"
            )
        async with await self._database.transaction():
            for i in range(count):
                article_id = i + 1
                for t in range(1, (i % 3) + 2):
                    tag_id = ((i + t) % num_tags) + 1
                    await self._database.execute(insert_sql, [article_id, tag_id])

    async def insert_rows(self, payloads: Sequence[dict[str, Any]]) -> None:
        instances = [self._model(**payload) for payload in payloads]
        await self._model.bulk_insert(instances)

    async def point_lookups(self, identifiers: Sequence[int]) -> None:
        for identifier in identifiers:
            result = await self._model.get(id=identifier)
            if result is None:
                raise RuntimeError(f"missing row for id={identifier}")

    async def equality_filters(self, scores: Sequence[int]) -> None:
        for score in scores:
            await self._model.filter(score=score)

    async def aggregate(self, iterations: int) -> int:
        group_count = 0
        for _ in range(iterations):
            rows = await self._model.values(
                "score",
                total=Sum("score"),
                count=Count("id"),
                group_by="score",
            )
            group_count = len(rows)
        return group_count

    async def bulk_update_expr(self, scores: Sequence[int]) -> None:
        for score in scores:
            await self._model.update_where({"score": F("score") + 1}, score=score)

    async def get_or_create(self, iterations: int) -> None:
        for i in range(iterations):
            await self._model.get(
                email=f"user{i}@example.com",
                defaults={"name": f"User {i}", "score": i % 100},
            )

    async def partial_select(self, scores: Sequence[int]) -> None:
        for score in scores:
            await self._model.filter(columns=["id", "name"], score=score)

    async def prefetch(self, iterations: int) -> None:
        for _ in range(iterations):
            await self._post_model.filter(prefetch_related=["author"], limit=50)

    async def m2m_query(self, iterations: int) -> None:
        for _ in range(iterations):
            await self._article_model.filter(prefetch_related=["tags"], limit=50)

    async def close(self) -> None:
        await self._database.disconnect()
        ModelRegistry.clear()

    async def drop_tables(self) -> None:
        """Drop all benchmark tables (for reuse between iterations)."""
        tbl = self._model.__table_name__
        tables = [
            f"{tbl}_article_tags",
            f"{tbl}_articles",
            f"{tbl}_tags",
            f"{tbl}_posts",
            f"{tbl}_authors",
            tbl,
        ]
        for table in tables:
            await self._database.execute(
                f"DROP TABLE IF EXISTS {table} CASCADE"
                if self._db_name != "sqlite"
                else f"DROP TABLE IF EXISTS {table}"
            )


class YaraHarness(Harness):
    name = "yara"

    def __init__(self, db_url: str, table_name: str, db_name: str) -> None:
        from yara_orm import Count as YaraCount
        from yara_orm import F as YaraF
        from yara_orm import Model, YaraOrm, fields
        from yara_orm import Sum as YaraSum

        class BenchmarkUser(Model):
            id = fields.IntField(pk=True)
            email = fields.CharField(max_length=255, unique=True, index=True)
            name = fields.CharField(max_length=255)
            score = fields.IntField(index=True)

            class Meta:
                table = table_name

        class BenchAuthor(Model):
            id = fields.IntField(pk=True)
            name = fields.CharField(max_length=255)

            class Meta:
                table = f"{table_name}_authors"

        class BenchPost(Model):
            id = fields.IntField(pk=True)
            title = fields.CharField(max_length=255)
            author = fields.ForeignKeyField("BenchAuthor", related_name="posts")

            class Meta:
                table = f"{table_name}_posts"

        class BenchTag(Model):
            id = fields.IntField(pk=True)
            label = fields.CharField(max_length=255)

            class Meta:
                table = f"{table_name}_tags"

        class BenchArticle(Model):
            id = fields.IntField(pk=True)
            title = fields.CharField(max_length=255)
            tags = fields.ManyToManyField(
                "BenchTag",
                related_name="articles",
                through=f"{table_name}_article_tags",
                backward_key=f"{table_name}_article_id",
                forward_key=f"{table_name}_tag_id",
            )

            class Meta:
                table = f"{table_name}_articles"

        self._db_url = db_url
        self._db_name = db_name
        self._table_name = table_name
        self._orm = YaraOrm
        self._count = YaraCount
        self._f = YaraF
        self._sum = YaraSum
        self._model = BenchmarkUser
        self._author_model = BenchAuthor
        self._post_model = BenchPost
        self._tag_model = BenchTag
        self._article_model = BenchArticle
        self._models = [BenchmarkUser, BenchAuthor, BenchPost, BenchTag, BenchArticle]

    async def create_schema(self) -> None:
        await self._orm.init(self._db_url)
        await self._orm.generate_schemas(safe=True, models=self._models)

    async def seed(self, payloads: Sequence[dict[str, Any]]) -> None:
        await self._model.bulk_create([self._model(**payload) for payload in payloads])

    async def insert_rows(self, payloads: Sequence[dict[str, Any]]) -> None:
        await self._model.bulk_create([self._model(**payload) for payload in payloads])

    async def point_lookups(self, identifiers: Sequence[int]) -> None:
        for identifier in identifiers:
            result = await self._model.get_or_none(id=identifier)
            if result is None:
                raise RuntimeError(f"missing row for id={identifier}")

    async def equality_filters(self, scores: Sequence[int]) -> None:
        for score in scores:
            await self._model.filter(score=score)

    async def aggregate(self, iterations: int) -> int:
        group_count = 0
        for _ in range(iterations):
            rows = await (
                self._model.annotate(total=self._sum("score"), count=self._count("id"))
                .group_by("score")
                .values("score", "total", "count")
            )
            group_count = len(rows)
        return group_count

    async def bulk_update_expr(self, scores: Sequence[int]) -> None:
        for score in scores:
            await self._model.filter(score=score).update(score=self._f("score") + 1)

    async def get_or_create(self, iterations: int) -> None:
        for index in range(iterations):
            await self._model.get_or_create(
                email=f"user{index}@example.com",
                defaults={"name": f"User {index}", "score": index % 100},
            )

    async def partial_select(self, scores: Sequence[int]) -> None:
        for score in scores:
            await self._model.filter(score=score).only("id", "name")

    async def _seed_relational(self, count: int) -> None:
        num_authors = max(count // 10, 1)
        num_tags = 20
        await self._author_model.bulk_create(
            [self._author_model(name=f"Author {index}") for index in range(num_authors)]
        )
        await self._post_model.bulk_create(
            [
                self._post_model(
                    title=f"Post {index}", author_id=(index % num_authors) + 1
                )
                for index in range(count)
            ]
        )
        await self._tag_model.bulk_create(
            [self._tag_model(label=f"tag-{index}") for index in range(num_tags)]
        )
        for index in range(count):
            article = await self._article_model.create(title=f"Article {index}")
            tag_ids = [
                ((index + offset) % num_tags) + 1
                for offset in range(1, (index % 3) + 2)
            ]
            tags = await self._tag_model.filter(id__in=tag_ids)
            await article.tags.add(*tags)

    async def prefetch(self, iterations: int) -> None:
        for _ in range(iterations):
            await self._post_model.all().limit(50).prefetch_related("author")

    async def m2m_query(self, iterations: int) -> None:
        for _ in range(iterations):
            await self._article_model.all().limit(50).prefetch_related("tags")

    async def close(self) -> None:
        await self._orm.close()

    async def drop_tables(self) -> None:
        tables = [
            f"{self._table_name}_article_tags",
            f"{self._table_name}_articles",
            f"{self._table_name}_tags",
            f"{self._table_name}_posts",
            f"{self._table_name}_authors",
            self._table_name,
        ]
        connection = self._orm.get_connection()
        for table in tables:
            await connection.execute(f"DROP TABLE IF EXISTS {table}")


class SQLAlchemyHarness(Harness):
    name = "sqlalchemy"

    def __init__(self, db_url: str, table_name: str, db_name: str) -> None:
        from sqlalchemy import Column as SAColumn
        from sqlalchemy import ForeignKey as SAForeignKey
        from sqlalchemy import Integer, String, Table, func, select, update
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
        from sqlalchemy.orm import DeclarativeBase, relationship, selectinload

        class Base(DeclarativeBase):
            pass

        class BenchmarkUser(Base):
            __tablename__ = table_name

            id = SAColumn(Integer, primary_key=True)
            email = SAColumn(String(255), unique=True, index=True)
            name = SAColumn(String(255), nullable=False)
            score = SAColumn(Integer, index=True, nullable=False)

        # Relational models
        class SAAuthor(Base):
            __tablename__ = f"{table_name}_authors"
            id = SAColumn(Integer, primary_key=True)
            name = SAColumn(String(255), nullable=False)
            posts = relationship("SAPost", back_populates="author")

        class SAPost(Base):
            __tablename__ = f"{table_name}_posts"
            id = SAColumn(Integer, primary_key=True)
            title = SAColumn(String(255), nullable=False)
            author_id = SAColumn(
                Integer, SAForeignKey(f"{table_name}_authors.id"), nullable=False
            )
            author = relationship("SAAuthor", back_populates="posts")

        sa_article_tags = Table(
            f"{table_name}_article_tags",
            Base.metadata,
            SAColumn(
                "article_id",
                Integer,
                SAForeignKey(f"{table_name}_articles.id"),
                primary_key=True,
            ),
            SAColumn(
                "tag_id",
                Integer,
                SAForeignKey(f"{table_name}_tags.id"),
                primary_key=True,
            ),
        )

        class SATag(Base):
            __tablename__ = f"{table_name}_tags"
            id = SAColumn(Integer, primary_key=True)
            label = SAColumn(String(255), nullable=False)

        class SAArticle(Base):
            __tablename__ = f"{table_name}_articles"
            id = SAColumn(Integer, primary_key=True)
            title = SAColumn(String(255), nullable=False)
            tags = relationship("SATag", secondary=sa_article_tags)

        self._base = Base
        self._model = BenchmarkUser
        self._author_model = SAAuthor
        self._post_model = SAPost
        self._tag_model = SATag
        self._article_model = SAArticle
        self._select = select
        self._update = update
        self._func = func
        self._selectinload = selectinload
        self._db_name = db_name
        self._engine = create_async_engine(db_url)
        self._session_factory = async_sessionmaker(self._engine, expire_on_commit=False)

    async def create_schema(self) -> None:
        async with self._engine.begin() as connection:
            await connection.run_sync(self._base.metadata.create_all)

    async def seed(self, payloads: Sequence[dict[str, Any]]) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                for payload in payloads:
                    session.add(self._model(**payload))

    async def insert_rows(self, payloads: Sequence[dict[str, Any]]) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                for payload in payloads:
                    session.add(self._model(**payload))

    async def point_lookups(self, identifiers: Sequence[int]) -> None:
        async with self._session_factory() as session:
            for identifier in identifiers:
                result = await session.get(self._model, identifier)
                if result is None:
                    raise RuntimeError(f"missing row for id={identifier}")

    async def equality_filters(self, scores: Sequence[int]) -> None:
        async with self._session_factory() as session:
            for score in scores:
                result = await session.scalars(
                    self._select(self._model).where(self._model.score == score)
                )
                result.all()

    async def aggregate(self, iterations: int) -> int:
        group_count = 0
        async with self._session_factory() as session:
            for _ in range(iterations):
                result = await session.execute(
                    self._select(
                        self._model.score,
                        self._func.sum(self._model.score),
                        self._func.count(self._model.id),
                    ).group_by(self._model.score)
                )
                group_count = len(result.all())
        return group_count

    async def bulk_update_expr(self, scores: Sequence[int]) -> None:
        async with self._session_factory() as session:
            for score in scores:
                await session.execute(
                    self._update(self._model)
                    .where(self._model.score == score)
                    .values(score=self._model.score + 1)
                )
                await session.commit()

    async def get_or_create(self, iterations: int) -> None:
        async with self._session_factory() as session:
            for i in range(iterations):
                result = await session.scalars(
                    self._select(self._model).where(
                        self._model.email == f"user{i}@example.com"
                    )
                )
                obj = result.first()
                if obj is None:
                    session.add(
                        self._model(
                            email=f"user{i}@example.com",
                            name=f"User {i}",
                            score=i % 100,
                        )
                    )
                    await session.commit()

    async def partial_select(self, scores: Sequence[int]) -> None:
        async with self._session_factory() as session:
            for score in scores:
                result = await session.execute(
                    self._select(self._model.id, self._model.name).where(
                        self._model.score == score
                    )
                )
                result.all()

    async def _seed_relational(self, count: int) -> None:
        from sqlalchemy import text

        num_authors = max(count // 10, 1)
        num_tags = 20
        async with self._session_factory() as session:
            async with session.begin():
                for i in range(num_authors):
                    session.add(self._author_model(name=f"Author {i}"))
            async with session.begin():
                for i in range(count):
                    session.add(
                        self._post_model(
                            title=f"Post {i}", author_id=(i % num_authors) + 1
                        )
                    )
            async with session.begin():
                for i in range(num_tags):
                    session.add(self._tag_model(label=f"tag-{i}"))
            async with session.begin():
                for i in range(count):
                    session.add(self._article_model(title=f"Article {i}"))

        # Insert M2M links via raw SQL to avoid lazy-load issues
        tbl = self._article_model.__tablename__.replace("_articles", "_article_tags")
        if self._db_name == "sqlite":
            stmt_tpl = (
                f"INSERT OR IGNORE INTO {tbl} (article_id, tag_id) VALUES (:aid, :tid)"
            )
        elif self._db_name == "postgres":
            stmt_tpl = (
                f"INSERT INTO {tbl} (article_id, tag_id) "
                f"VALUES (:aid, :tid) ON CONFLICT DO NOTHING"
            )
        else:  # mysql
            stmt_tpl = (
                f"INSERT IGNORE INTO {tbl} (article_id, tag_id) VALUES (:aid, :tid)"
            )
        async with self._engine.begin() as conn:
            for i in range(count):
                article_id = i + 1
                for t in range(1, (i % 3) + 2):
                    tag_id = ((i + t) % num_tags) + 1
                    await conn.execute(
                        text(stmt_tpl),
                        {"aid": article_id, "tid": tag_id},
                    )

    async def prefetch(self, iterations: int) -> None:
        async with self._session_factory() as session:
            for _ in range(iterations):
                result = await session.scalars(
                    self._select(self._post_model)
                    .options(self._selectinload(self._post_model.author))
                    .limit(50)
                )
                result.all()

    async def m2m_query(self, iterations: int) -> None:
        async with self._session_factory() as session:
            for _ in range(iterations):
                result = await session.scalars(
                    self._select(self._article_model)
                    .options(self._selectinload(self._article_model.tags))
                    .limit(50)
                )
                result.unique().all()

    async def close(self) -> None:
        await self._engine.dispose()

    async def drop_tables(self) -> None:
        async with self._engine.begin() as connection:
            await connection.run_sync(self._base.metadata.drop_all)


class TortoiseHarness(Harness):
    name = "tortoise"

    def __init__(self, db_url: str, table_name: str, db_name: str) -> None:
        from tortoise import Tortoise

        from benchmarks.tortoise_models import (
            BenchArticle,
            BenchAuthor,
            BenchmarkUser,
            BenchPost,
            BenchTag,
        )

        self._db_url = db_url
        self._db_name = db_name
        self._tortoise = Tortoise
        self._model = BenchmarkUser
        self._author_model = BenchAuthor
        self._post_model = BenchPost
        self._tag_model = BenchTag
        self._article_model = BenchArticle

    async def create_schema(self) -> None:
        await self._tortoise.init(
            db_url=self._db_url,
            modules={"models": ["benchmarks.tortoise_models"]},
        )
        await self._tortoise.generate_schemas(safe=True)

    async def seed(self, payloads: Sequence[dict[str, Any]]) -> None:
        for payload in payloads:
            await self._model.create(**payload)

    async def insert_rows(self, payloads: Sequence[dict[str, Any]]) -> None:
        for payload in payloads:
            await self._model.create(**payload)

    async def point_lookups(self, identifiers: Sequence[int]) -> None:
        for identifier in identifiers:
            result = await self._model.get_or_none(id=identifier)
            if result is None:
                raise RuntimeError(f"missing row for id={identifier}")

    async def equality_filters(self, scores: Sequence[int]) -> None:
        for score in scores:
            await self._model.filter(score=score)

    async def aggregate(self, iterations: int) -> int:
        from tortoise.functions import Count as TCount
        from tortoise.functions import Sum as TSum

        group_count = 0
        for _ in range(iterations):
            rows = await (
                self._model.annotate(total=TSum("score"), count=TCount("id"))
                .group_by("score")
                .values("score", "total", "count")
            )
            group_count = len(rows)
        return group_count

    async def bulk_update_expr(self, scores: Sequence[int]) -> None:
        from tortoise.expressions import F as TF

        for score in scores:
            await self._model.filter(score=score).update(score=TF("score") + 1)

    async def get_or_create(self, iterations: int) -> None:
        for i in range(iterations):
            await self._model.get_or_create(
                email=f"user{i}@example.com",
                defaults={"name": f"User {i}", "score": i % 100},
            )

    async def partial_select(self, scores: Sequence[int]) -> None:
        for score in scores:
            await self._model.filter(score=score).values_list("id", "name")

    async def _seed_relational(self, count: int) -> None:
        num_authors = max(count // 10, 1)
        num_tags = 20
        for i in range(num_authors):
            await self._author_model.create(name=f"Author {i}")
        for i in range(count):
            await self._post_model.create(
                title=f"Post {i}", author_id=(i % num_authors) + 1
            )
        for i in range(num_tags):
            await self._tag_model.create(label=f"tag-{i}")
        for i in range(count):
            art = await self._article_model.create(title=f"Article {i}")
            tag_ids = [((i + t) % num_tags) + 1 for t in range(1, (i % 3) + 2)]
            tags = await self._tag_model.filter(id__in=tag_ids)
            await art.tags.add(*tags)

    async def prefetch(self, iterations: int) -> None:
        for _ in range(iterations):
            await self._post_model.all().limit(50).prefetch_related("author")

    async def m2m_query(self, iterations: int) -> None:
        for _ in range(iterations):
            await self._article_model.all().limit(50).prefetch_related("tags")

    async def close(self) -> None:
        await self._tortoise.close_connections()

    async def drop_tables(self) -> None:
        from tortoise import connections

        conn = connections.get("default")
        tbl = self._model._meta.db_table
        tables = [
            "bench_articles_bench_tags",  # Tortoise auto-names M2M tables
            "bench_articles",
            "bench_tags",
            "bench_posts",
            "bench_authors",
            tbl,
        ]
        for table in tables:
            if self._db_name == "sqlite":
                await conn.execute_query(f"DROP TABLE IF EXISTS {table}")
            else:
                await conn.execute_query(f"DROP TABLE IF EXISTS {table} CASCADE")


def available_harnesses(db_name: str) -> list[type[Harness]]:
    harnesses: list[type[Harness]] = [AUniqORMHarness]

    try:
        import sqlalchemy  # noqa: F401
    except ImportError:
        pass
    else:
        harnesses.append(SQLAlchemyHarness)

    try:
        import tortoise  # noqa: F401
    except ImportError:
        pass
    else:
        harnesses.append(TortoiseHarness)

    try:
        import yara_orm  # noqa: F401
    except ImportError:
        pass
    else:
        harnesses.append(YaraHarness)

    return harnesses


async def time_call(function: Any, *args: Any) -> float:
    gc.collect()
    start = perf_counter()
    await function(*args)
    return perf_counter() - start


async def run_scenario(
    harness_type: type[Harness],
    scenario: str,
    payloads: Sequence[dict[str, Any]],
    lookups: Sequence[int],
    scores: Sequence[int],
    repeats: int,
    warmups: int,
    workspace: Path,
    db_name: str,
) -> ScenarioReport:
    timings: list[float] = []
    operations_map = {
        "insert": len(payloads),
        "get": len(lookups),
        "filter": len(scores),
        "aggregate": len(scores),
        "update_expr": len(scores),
        "get_or_create": len(scores),
        "partial_select": len(scores),
        "prefetch": len(scores),
        "m2m": len(scores),
    }
    relational_scenarios = {"prefetch", "m2m"}
    table_name = f"benchmark_users_{scenario}"

    for iteration in range(warmups + repeats):
        # Build the appropriate URL for this harness/database combo
        db_path: Path | None = None
        if is_sqlite(db_name):
            db_path = workspace / (
                f"{harness_type.name}_{scenario}_{iteration}.sqlite3"
            )

        if harness_type is AUniqORMHarness:
            url = get_auniqorm_url(db_name, db_path)
        elif harness_type is SQLAlchemyHarness:
            url = get_sa_url(db_name, db_path)
        elif harness_type is YaraHarness:
            url = get_yara_url(db_name, db_path)
        else:
            url = get_tortoise_url(db_name, db_path)

        harness = harness_type(url, table_name=table_name, db_name=db_name)
        try:
            await harness.create_schema()
            if scenario in relational_scenarios:
                await harness._seed_relational(len(payloads))
            elif scenario != "insert":
                await harness.seed(payloads)

            if scenario == "insert":
                elapsed = await time_call(harness.insert_rows, payloads)
            elif scenario == "get":
                elapsed = await time_call(harness.point_lookups, lookups)
            elif scenario == "filter":
                elapsed = await time_call(harness.equality_filters, scores)
            elif scenario == "aggregate":
                elapsed = await time_call(harness.aggregate, len(scores))
            elif scenario == "update_expr":
                elapsed = await time_call(harness.bulk_update_expr, scores)
            elif scenario == "get_or_create":
                elapsed = await time_call(harness.get_or_create, len(scores))
            elif scenario == "partial_select":
                elapsed = await time_call(harness.partial_select, scores)
            elif scenario == "prefetch":
                elapsed = await time_call(harness.prefetch, len(scores))
            elif scenario == "m2m":
                elapsed = await time_call(harness.m2m_query, len(scores))
            else:
                raise ValueError(f"Unknown scenario: {scenario}")

            if iteration >= warmups:
                timings.append(elapsed)
        finally:
            if not is_sqlite(db_name):
                await harness.drop_tables()
            await harness.close()
            if db_path and db_path.exists():
                db_path.unlink()

    return ScenarioReport(
        library=harness_type.name,
        scenario=scenario,
        database=db_name,
        operations=operations_map[scenario],
        timings=timings,
    )


def available_databases(config: BenchmarkConfig) -> list[str]:
    """Return list of databases to benchmark against."""
    if config.database is not None:
        return [config.database]
    # Auto-detect: sqlite always available, postgres/mysql if reachable
    databases = ["sqlite"]
    import socket

    for db_name, port in (("postgres", 5432), ("mysql", 3306)):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        try:
            sock.connect(("localhost", port))
            databases.append(db_name)
        except OSError:
            pass
        finally:
            sock.close()
    return databases


async def collect_reports(config: BenchmarkConfig) -> list[ScenarioReport]:
    payloads = build_payloads(config.rows)
    lookups = build_lookup_ids(config.rows, config.lookups)
    scores = build_filter_scores(config.filters)
    reports: list[ScenarioReport] = []
    databases = available_databases(config)

    with TemporaryDirectory(dir=ROOT, prefix=".benchmarks-") as temp_dir:
        workspace = Path(temp_dir)
        for db_name in databases:
            for scenario in (
                "insert",
                "get",
                "filter",
                "aggregate",
                "update_expr",
                "get_or_create",
                "partial_select",
                "prefetch",
                "m2m",
            ):
                for harness_type in available_harnesses(db_name):
                    report = await run_scenario(
                        harness_type=harness_type,
                        scenario=scenario,
                        payloads=payloads,
                        lookups=lookups,
                        scores=scores,
                        repeats=config.repeats,
                        warmups=config.warmups,
                        workspace=workspace,
                        db_name=db_name,
                    )
                    reports.append(report)

    return reports


def format_table(reports: Iterable[ScenarioReport]) -> str:
    ordered_reports = sorted(
        reports, key=lambda report: (report.scenario, report.library)
    )
    fastest_by_scenario = {
        scenario: min(
            report.median for report in ordered_reports if report.scenario == scenario
        )
        for scenario in {report.scenario for report in ordered_reports}
    }

    lines = []
    header = (
        f"{'scenario':<15} {'library':<12} {'ops':>8} {'median(s)':>12} "
        f"{'best(s)':>10} {'ops/s':>12} {'vs_best':>8}"
    )
    lines.append(header)
    lines.append("-" * len(header))

    for report in ordered_reports:
        fastest = fastest_by_scenario[report.scenario]
        relative = report.median / fastest if fastest else 1.0
        lines.append(
            f"{report.scenario:<15} {report.library:<12} {report.operations:>8} "
            f"{report.median:>12.4f} {report.best:>10.4f} "
            f"{report.ops_per_second:>12.1f} {relative:>8.2f}x"
        )

    return "\n".join(lines)


def parse_args() -> BenchmarkConfig:
    parser = argparse.ArgumentParser(
        description="Compare aUniqORM with other Python ORMs.",
    )
    parser.add_argument(
        "--database",
        choices=["sqlite", "postgres", "mysql"],
        default=None,
        help="database backend (default: all available)",
    )
    parser.add_argument(
        "--rows", type=int, default=5000, help="rows inserted or pre-seeded"
    )
    parser.add_argument(
        "--lookups", type=int, default=5000, help="number of primary-key lookups"
    )
    parser.add_argument(
        "--filters", type=int, default=1000, help="number of indexed equality filters"
    )
    parser.add_argument(
        "--repeats", type=int, default=5, help="measured runs per scenario"
    )
    parser.add_argument(
        "--warmups", type=int, default=1, help="warmup runs per scenario"
    )
    arguments = parser.parse_args()

    if arguments.rows <= 0:
        parser.error("--rows must be > 0")
    if arguments.lookups <= 0:
        parser.error("--lookups must be > 0")
    if arguments.filters <= 0:
        parser.error("--filters must be > 0")
    if arguments.repeats <= 0:
        parser.error("--repeats must be > 0")
    if arguments.warmups < 0:
        parser.error("--warmups must be >= 0")

    return BenchmarkConfig(
        rows=arguments.rows,
        lookups=arguments.lookups,
        filters=arguments.filters,
        repeats=arguments.repeats,
        warmups=arguments.warmups,
        database=arguments.database,
    )


async def async_main() -> int:
    config = parse_args()
    reports = await collect_reports(config)
    databases = available_databases(config)
    db_label = ", ".join(d.upper() for d in databases)
    print(
        f"{db_label} ORM benchmark "
        f"(rows={config.rows}, lookups={config.lookups}, filters={config.filters}, "
        f"repeats={config.repeats}, warmups={config.warmups})"
    )
    print()
    # Print per-database tables when multiple databases
    if len(databases) > 1:
        for db_name in databases:
            db_reports = [r for r in reports if r.database == db_name]
            print(f"── {db_name.upper()} ──")
            print()
            print(format_table(db_reports))
            print()
    else:
        print(format_table(reports))
    return 0


def _run_per_database_subprocesses(config: BenchmarkConfig) -> int:
    # Tortoise caches per-model insert SQL globally; running multiple dialects
    # in the same process makes the cache reuse a previous dialect's placeholders
    # (e.g. sqlite "?" against asyncpg). Isolate each DB in its own subprocess.
    import subprocess

    databases = available_databases(config)
    common = [
        "--rows",
        str(config.rows),
        "--lookups",
        str(config.lookups),
        "--filters",
        str(config.filters),
        "--repeats",
        str(config.repeats),
        "--warmups",
        str(config.warmups),
    ]
    rc = 0
    for db_name in databases:
        result = subprocess.run(
            [sys.executable, __file__, "--database", db_name, *common],
            check=False,
        )
        if result.returncode != 0:
            rc = result.returncode
    return rc


def main() -> int:
    config = parse_args()
    if config.database is None and len(available_databases(config)) > 1:
        return _run_per_database_subprocesses(config)
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
