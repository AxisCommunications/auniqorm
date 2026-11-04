# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

"""Tests for prefetch_related and ManyToMany relationships."""

from auniqorm import (
    AUniqORMBase,
    Column,
    ForeignKey,
    ManyToMany,
    PrimaryKey,
)

# ── prefetch_related (forward FK) ────────────────────────────────────────────


class TestPrefetchForward:
    async def test_prefetch_forward_fk(self, db):
        class Author(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)

        class Post(AUniqORMBase):
            id: int = PrimaryKey()
            title: str = Column(nullable=False)
            author_id: int = ForeignKey("authors.id")

        await Author.create_table()
        await Post.create_table()

        a1 = Author(name="Alice")
        await a1.insert()
        a2 = Author(name="Bob")
        await a2.insert()

        await Post(title="Post 1", author_id=a1.id).insert()
        await Post(title="Post 2", author_id=a2.id).insert()
        await Post(title="Post 3", author_id=a1.id).insert()

        posts = await Post.filter(prefetch_related=["author"], order_by="id")
        assert len(posts) == 3
        # Attached as attribute via __dict__
        assert posts[0].__dict__["author"].name == "Alice"
        assert posts[1].__dict__["author"].name == "Bob"
        assert posts[2].__dict__["author"].name == "Alice"

    async def test_prefetch_forward_null_fk(self, db):
        class Category(AUniqORMBase, table="categories"):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)

        class Item(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)
            category_id: int | None = ForeignKey("categories.id", nullable=True)

        await Category.create_table()
        await Item.create_table()

        cat = Category(name="Electronics")
        await cat.insert()

        await Item(name="Phone", category_id=cat.id).insert()
        await Item(name="Unknown", category_id=None).insert()

        items = await Item.filter(prefetch_related=["category"], order_by="id")
        assert items[0].__dict__["category"].name == "Electronics"
        assert items[1].__dict__["category"] is None


# ── prefetch_related (reverse FK) ───────────────────────────────────────────


class TestPrefetchReverse:
    async def test_prefetch_reverse(self, db):
        class Team(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)

        class Player(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)
            team_id: int = ForeignKey("teams.id")

        await Team.create_table()
        await Player.create_table()

        t1 = Team(name="Red")
        await t1.insert()
        t2 = Team(name="Blue")
        await t2.insert()

        await Player(name="Alice", team_id=t1.id).insert()
        await Player(name="Bob", team_id=t1.id).insert()
        await Player(name="Charlie", team_id=t2.id).insert()

        teams = await Team.filter(prefetch_related=["players"], order_by="id")
        assert len(teams) == 2
        assert len(teams[0].__dict__["players"]) == 2
        assert len(teams[1].__dict__["players"]) == 1
        player_names = sorted(p.name for p in teams[0].__dict__["players"])
        assert player_names == ["Alice", "Bob"]

    async def test_prefetch_reverse_empty(self, db):
        class Project(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)

        class Task(AUniqORMBase):
            id: int = PrimaryKey()
            title: str = Column(nullable=False)
            project_id: int = ForeignKey("projects.id")

        await Project.create_table()
        await Task.create_table()

        p1 = Project(name="Active")
        await p1.insert()
        p2 = Project(name="Empty")
        await p2.insert()

        await Task(title="Do stuff", project_id=p1.id).insert()

        projects = await Project.filter(prefetch_related=["tasks"], order_by="id")
        assert len(projects[0].__dict__["tasks"]) == 1
        assert projects[1].__dict__["tasks"] == []


# ── ManyToMany ────────────────────────────────────────────────────────────────


class TestManyToMany:
    async def test_m2m_add_and_all(self, db):
        class Article(AUniqORMBase):
            id: int = PrimaryKey()
            title: str = Column(nullable=False)
            tags = ManyToMany("tags", through="article_tags")

        class Tag(AUniqORMBase):
            id: int = PrimaryKey()
            label: str = Column(nullable=False)

        await Article.create_table()
        await Tag.create_table()
        await db.execute(
            "CREATE TABLE article_tags ("
            "article_id INTEGER REFERENCES articles(id), "
            "tag_id INTEGER REFERENCES tags(id), "
            "PRIMARY KEY (article_id, tag_id))"
        )

        art = Article(title="Hello World")
        await art.insert()
        t1 = Tag(label="python")
        await t1.insert()
        t2 = Tag(label="orm")
        await t2.insert()

        await art.tags.add(t1, t2)

        result = await art.tags.all()
        labels = sorted(t.label for t in result)
        assert labels == ["orm", "python"]

    async def test_m2m_remove(self, db):
        class Book(AUniqORMBase):
            id: int = PrimaryKey()
            title: str = Column(nullable=False)
            genres = ManyToMany("genres", through="book_genres")

        class Genre(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)

        await Book.create_table()
        await Genre.create_table()
        await db.execute(
            "CREATE TABLE book_genres ("
            "book_id INTEGER REFERENCES books(id), "
            "genre_id INTEGER REFERENCES genres(id), "
            "PRIMARY KEY (book_id, genre_id))"
        )

        book = Book(title="Novel")
        await book.insert()
        g1 = Genre(name="fiction")
        await g1.insert()
        g2 = Genre(name="thriller")
        await g2.insert()

        await book.genres.add(g1, g2)
        await book.genres.remove(g1)

        result = await book.genres.all()
        assert len(result) == 1
        assert result[0].name == "thriller"

    async def test_m2m_clear(self, db):
        class Course(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)
            students = ManyToMany("students", through="enrollments")

        class Student(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)

        await Course.create_table()
        await Student.create_table()
        await db.execute(
            "CREATE TABLE enrollments ("
            "course_id INTEGER REFERENCES courses(id), "
            "student_id INTEGER REFERENCES students(id), "
            "PRIMARY KEY (course_id, student_id))"
        )

        course = Course(name="Math")
        await course.insert()
        s1 = Student(name="Alice")
        await s1.insert()
        s2 = Student(name="Bob")
        await s2.insert()

        await course.students.add(s1, s2)
        await course.students.clear()

        result = await course.students.all()
        assert result == []

    async def test_m2m_duplicate_add_ignored(self, db):
        class Event(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)
            attendees = ManyToMany("attendees", through="event_attendees")

        class Attendee(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)

        await Event.create_table()
        await Attendee.create_table()
        await db.execute(
            "CREATE TABLE event_attendees ("
            "event_id INTEGER REFERENCES events(id), "
            "attendee_id INTEGER REFERENCES attendees(id), "
            "PRIMARY KEY (event_id, attendee_id))"
        )

        event = Event(name="Conference")
        await event.insert()
        att = Attendee(name="Alice")
        await att.insert()

        await event.attendees.add(att)
        await event.attendees.add(att)  # duplicate — should be ignored

        result = await event.attendees.all()
        assert len(result) == 1


# ── prefetch_related with M2M ────────────────────────────────────────────────


class TestPrefetchM2M:
    async def test_prefetch_m2m(self, db):
        class Post(AUniqORMBase, table="blog_posts"):
            id: int = PrimaryKey()
            title: str = Column(nullable=False)
            categories = ManyToMany("blog_categories", through="post_categories")

        class BlogCategory(AUniqORMBase, table="blog_categories"):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)

        await Post.create_table()
        await BlogCategory.create_table()
        await db.execute(
            "CREATE TABLE post_categories ("
            "blog_post_id INTEGER REFERENCES blog_posts(id), "
            "blog_category_id INTEGER REFERENCES blog_categories(id), "
            "PRIMARY KEY (blog_post_id, blog_category_id))"
        )

        p1 = Post(title="Intro to Python")
        await p1.insert()
        p2 = Post(title="Advanced ORM")
        await p2.insert()

        c1 = BlogCategory(name="python")
        await c1.insert()
        c2 = BlogCategory(name="database")
        await c2.insert()

        await p1.categories.add(c1)
        await p2.categories.add(c1, c2)

        posts = await Post.filter(prefetch_related=["categories"], order_by="id")
        assert len(posts) == 2
        assert len(posts[0].__dict__["categories"]) == 1
        assert posts[0].__dict__["categories"][0].name == "python"
        cats = sorted(c.name for c in posts[1].__dict__["categories"])
        assert cats == ["database", "python"]

    async def test_prefetch_m2m_empty(self, db):
        class Album(AUniqORMBase):
            id: int = PrimaryKey()
            title: str = Column(nullable=False)
            artists = ManyToMany("album_artists", through="album_artist_links")

        class AlbumArtist(AUniqORMBase):
            id: int = PrimaryKey()
            name: str = Column(nullable=False)

        await Album.create_table()
        await AlbumArtist.create_table()
        await db.execute(
            "CREATE TABLE album_artist_links ("
            "album_id INTEGER REFERENCES albums(id), "
            "album_artist_id INTEGER REFERENCES album_artists(id), "
            "PRIMARY KEY (album_id, album_artist_id))"
        )

        album = Album(title="Silence")
        await album.insert()

        albums = await Album.filter(prefetch_related=["artists"])
        assert albums[0].__dict__["artists"] == []
