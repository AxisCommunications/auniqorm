# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for relationship features: select_related, fetch_related."""

import pytest

from auniqorm.columns import ForeignKey, PrimaryKey
from auniqorm.models import AUniqORMBase, ModelRegistry

# ---------------------------------------------------------------------------
# Models for relationship testing
# ---------------------------------------------------------------------------


class Author(AUniqORMBase):
    id: int = PrimaryKey()
    name: str


class Post(AUniqORMBase):
    id: int = PrimaryKey()
    title: str
    author_id: int = ForeignKey("authors.id")


class Comment(AUniqORMBase):
    id: int = PrimaryKey()
    body: str
    post_id: int = ForeignKey("posts.id")


@pytest.fixture
async def seeded_db(db):
    """Create tables and seed data."""
    # Re-register models (conftest clears registry before each test)
    ModelRegistry.register(Author)
    ModelRegistry.register(Post)
    ModelRegistry.register(Comment)
    ModelRegistry.bind(db)

    await Author.create_table()
    await Post.create_table()
    await Comment.create_table()

    alice = await Author(name="Alice").insert()
    bob = await Author(name="Bob").insert()

    p1 = await Post(title="First Post", author_id=alice.id).insert()
    p2 = await Post(title="Second Post", author_id=bob.id).insert()
    p3 = await Post(title="Third Post", author_id=alice.id).insert()

    await Comment(body="Great!", post_id=p1.id).insert()
    await Comment(body="Thanks", post_id=p1.id).insert()
    await Comment(body="Nice", post_id=p2.id).insert()

    return {"alice": alice, "bob": bob, "p1": p1, "p2": p2, "p3": p3}


# ---------------------------------------------------------------------------
# fetch_related (lazy)
# ---------------------------------------------------------------------------


class TestFetchRelated:
    async def test_fetch_related_returns_related_instance(self, seeded_db):
        post = await Post.get(id=seeded_db["p1"].id)
        author = await post.fetch_related("author")
        assert author is not None
        assert author.name == "Alice"

    async def test_fetch_related_caches_result(self, seeded_db):
        post = await Post.get(id=seeded_db["p1"].id)
        author1 = await post.fetch_related("author")
        author2 = await post.fetch_related("author")
        assert author1 is author2  # same object from cache

    async def test_fetch_related_invalid_name_raises(self, seeded_db):
        post = await Post.get(id=seeded_db["p1"].id)
        with pytest.raises(ValueError, match="no forward FK"):
            await post.fetch_related("nonexistent")

    async def test_fetch_related_null_fk(self, db):
        class NullFK(AUniqORMBase, table="null_fk"):
            id: int = PrimaryKey()
            author_id: int | None = ForeignKey("authors.id", nullable=True)

        ModelRegistry.register(Author)
        ModelRegistry.bind(db)
        await Author.create_table()
        await NullFK.create_table()

        inst = await NullFK(author_id=None).insert()
        result = await inst.fetch_related("author")
        assert result is None

    async def test_fetch_related_chained(self, seeded_db):
        comment = await Comment.get(id=1)
        post = await comment.fetch_related("post")
        assert post is not None
        author = await post.fetch_related("author")
        assert author is not None
        assert author.name == "Alice"


# ---------------------------------------------------------------------------
# select_related on get()
# ---------------------------------------------------------------------------


class TestSelectRelatedGet:
    async def test_get_with_select_related(self, seeded_db):
        post = await Post.get(id=seeded_db["p1"].id, select_related=["author"])
        assert post is not None
        assert post.title == "First Post"
        # Related object accessible via attribute
        assert post.author is not None
        assert post.author.name == "Alice"

    async def test_get_select_related_no_match(self, seeded_db):
        result = await Post.get(id=9999, select_related=["author"])
        assert result is None

    async def test_get_without_select_related(self, seeded_db):
        post = await Post.get(id=seeded_db["p1"].id)
        assert post is not None
        # Without select_related, no .author attribute
        assert not hasattr(post, "author") or post.__dict__.get("author") is None


# ---------------------------------------------------------------------------
# select_related on filter()
# ---------------------------------------------------------------------------


class TestSelectRelatedFilter:
    async def test_filter_with_select_related(self, seeded_db):
        posts = await Post.filter(
            author_id=seeded_db["alice"].id,
            select_related=["author"],
            order_by="id",
        )
        assert len(posts) == 2
        assert posts[0].author.name == "Alice"
        assert posts[1].author.name == "Alice"

    async def test_filter_select_related_all(self, seeded_db):
        posts = await Post.filter(select_related=["author"], order_by="id")
        assert len(posts) == 3
        assert posts[0].author.name == "Alice"
        assert posts[1].author.name == "Bob"
        assert posts[2].author.name == "Alice"

    async def test_filter_select_related_with_limit(self, seeded_db):
        posts = await Post.filter(select_related=["author"], order_by="id", limit=2)
        assert len(posts) == 2

    async def test_filter_select_related_with_offset(self, seeded_db):
        posts = await Post.filter(
            select_related=["author"], order_by="id", limit=10, offset=1
        )
        assert len(posts) == 2
        assert posts[0].title == "Second Post"


# ---------------------------------------------------------------------------
# _resolve_relation
# ---------------------------------------------------------------------------


class TestResolveRelation:
    def test_resolve_valid_relation(self, seeded_db):
        fk_field, ref_col, target = Post._resolve_relation("author")
        assert fk_field == "author_id"
        assert ref_col == "id"
        assert target is Author

    def test_resolve_invalid_relation(self, seeded_db):
        with pytest.raises(ValueError, match="no ForeignKey field"):
            Post._resolve_relation("editor")

    def test_resolve_unregistered_target(self, db):
        class Orphan(AUniqORMBase):
            id: int = PrimaryKey()
            ghost_id: int = ForeignKey("ghosts.id")

        with pytest.raises(ValueError, match="not found in registry"):
            Orphan._resolve_relation("ghost")
