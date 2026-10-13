# Copyright (C) 2026 Axis Communications AB, Lund, Sweden
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

from tortoise import fields
from tortoise.models import Model


class BenchmarkUser(Model):
    id = fields.IntField(pk=True)
    email = fields.CharField(max_length=255, unique=True, index=True)
    name = fields.CharField(max_length=255)
    score = fields.IntField(index=True)

    class Meta:
        table = "benchmark_users"


class BenchAuthor(Model):
    id = fields.IntField(pk=True)
    name = fields.CharField(max_length=255)

    class Meta:
        table = "bench_authors"


class BenchPost(Model):
    id = fields.IntField(pk=True)
    title = fields.CharField(max_length=255)
    author = fields.ForeignKeyField("models.BenchAuthor", related_name="posts")

    class Meta:
        table = "bench_posts"


class BenchTag(Model):
    id = fields.IntField(pk=True)
    label = fields.CharField(max_length=255)

    class Meta:
        table = "bench_tags"


class BenchArticle(Model):
    id = fields.IntField(pk=True)
    title = fields.CharField(max_length=255)
    tags = fields.ManyToManyField("models.BenchTag", related_name="articles")

    class Meta:
        table = "bench_articles"
