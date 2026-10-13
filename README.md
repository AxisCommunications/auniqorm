<p align="left">
  <img src="auniqorm.png" alt="auniqorm logo" width="100%">
</p>

# aUniqORM

**One Python data layer for SQL, documents, and graphs.**

aUniqORM is a unified, typed Python data-mapping layer for relational,
document, and graph databases. It provides consistent connection management,
async and synchronous APIs, transactions, serialization, and database-native
query access without flattening every backend into the same lowest-common
denominator.

Use the relational ORM for SQL databases, the document API for MongoDB, and the
graph API for Neo4j. Rust-powered relational drivers provide the default
database path for performance-sensitive workloads.

## Why aUniqORM?

- **Async by default** with `asyncio`-native database access.
- **Typed models** built around Python annotations and [msgspec](https://github.com/msgspec/msgspec).
- **Multiple data models** through relational, document, and graph backends.
- **Practical ORM features** including relationships, migrations, transactions,
  pagination, bulk inserts, upserts, and prefetching.
- **Portable SQL** with literal-aware placeholder translation when raw SQL is
  needed.
- **Synchronous access** through a blocking adapter without maintaining a
  separate model layer.
- **Rust-powered drivers** for SQLite, PostgreSQL, and MySQL, with an explicit
  opt-out when the standard Python drivers are preferred.
- **Native query access** through SQL, MongoDB operation documents, or Cypher.

## Contents

- [Installation](#installation)
- [Supported databases](#supported-databases)
- [Document and graph databases](#document-and-graph-databases)
- [Quick start](#quick-start)
- [Synchronous usage](#synchronous-usage)
- [Query features](#query-features)
- [Native drivers](#native-drivers)
- [Benchmarking](#benchmarking)
- [Development](#development)

## Installation

Install aUniqORM with [uv](https://docs.astral.sh/uv/):

```bash
uv add auniqorm
```

Install the extra for the database driver you use:

```bash
uv add "auniqorm[sqlite]"
uv add "auniqorm[postgres]"
uv add "auniqorm[mysql]"
uv add "auniqorm[mongodb]"
uv add "auniqorm[neo4j]"
```

The `postgres` extra also supports CockroachDB, and the `mysql` extra supports
MariaDB through their wire-compatible drivers.

## Supported databases

| Database | Driver | URL scheme(s) |
|---|---|---|
| PostgreSQL | [asyncpg](https://github.com/MagicStack/asyncpg) | `postgresql://`, `postgres://`, `pg://` |
| CockroachDB | asyncpg (PostgreSQL wire-compatible) | `cockroachdb://`, `cockroach://`, `crdb://` |
| MySQL | [aiomysql](https://github.com/aio-libs/aiomysql) | `mysql://` |
| MariaDB | aiomysql (MySQL wire-compatible) | `mariadb://` |
| SQLite | [aiosqlite](https://github.com/omnilib/aiosqlite) | `sqlite://` |
| MongoDB | [PyMongo async API](https://pymongo.readthedocs.io/) | `mongodb://`, `mongodb+srv://`, `mongo://` |
| Neo4j | [Neo4j Python driver](https://neo4j.com/docs/python-manual/current/) | `neo4j://`, `bolt://` |

## Document and graph databases

MongoDB and Neo4j use native document and graph APIs rather than relational
model DDL. They share aUniqORM's connection, transaction, and async/sync
foundations, while keeping their native query languages and operation shapes.

### MongoDB

Install the optional backend:

```bash
uv add "auniqorm[mongodb]"
```

Pass JSON operation documents through `Database`:

```python
async with Database("mongodb://localhost/example") as db:
    await db.execute(
        '{"operation":"insert_one","collection":"users",'
        '"document":{"name":"Ada"}}'
    )
    users = await db.fetch_all(
        '{"operation":"find","collection":"users","filter":{}}'
    )
```

### Neo4j

Install the optional backend:

```bash
uv add "auniqorm[neo4j]"
```

Use Cypher with normal query parameters:

```python
async with Database("neo4j://localhost:7687/app") as db:
    users = await db.fetch_all(
        "MATCH (u:User) WHERE u.active = ? RETURN u.name AS name",
        [True],
    )
```

## Quick start

The following example shows the model API. Replace the connection URL with a
SQLite, PostgreSQL, MySQL, or MariaDB URL for your application:

```python
from auniqorm import Database, AUniqORMBase, Column, PrimaryKey


class User(AUniqORMBase, table="users"):
    id: int = PrimaryKey()
    name: str = Column(nullable=False)
    email: str = Column(unique=True)


async with Database("postgresql://user:pass@localhost/mydb") as db:
    user = User(name="Alice", email="alice@example.com")
    await user.insert()  # populates user.id and all server defaults
    print(user.id)  # e.g. 1

    found = await User.get(email="alice@example.com")
    users = await User.filter(name="Alice", order_by="-id", limit=10)
```

Install the matching database extra before connecting to a database server.

## Synchronous usage

aUniqORM is async-first, but non-async code can use the same models and
connection API without `await` via the `auniqorm.sync` adapter. A single
background event loop runs on a daemon thread, and every call blocks until it
completes — so pinned connections and open transactions stay on one loop.

```python
from auniqorm import AUniqORMBase, Column, PrimaryKey
from auniqorm.sync import SyncDatabase, sync_model


class User(AUniqORMBase, table="users"):
    id: int = PrimaryKey()
    name: str = Column(nullable=False)
    email: str = Column(unique=True)


with SyncDatabase("sqlite:///app.db") as db:
    Users = sync_model(User)

    user = Users(name="Alice", email="alice@example.com")
    user.insert()  # blocks, no await

    found = Users.get(email="alice@example.com")
    for u in Users.filter(order_by="-id", limit=10):
        print(u.name)

    # Raw queries, transactions, and streaming iteration are all blocking too
    with db.transaction():
        db.execute("UPDATE users SET name = ? WHERE id = ?", ["Alice B", user.id])
    for row in db.iterate("SELECT name FROM users"):
        print(row["name"])
```

`SyncDatabase` mirrors the async `Database` (`execute`, `fetch_all`,
`fetch_one`, `fetch_val`, `execute_many`, `iterate`, `iterate_batches`,
`transaction`, `connection`). `sync_model(Model)` returns a blocking proxy for a
model's classmethods and instances; `run(coro)` is an escape hatch for any
coroutine the proxies don't cover.

Note: model instances returned by the sync API are thin proxies, so
`isinstance(user, User)` is `False`. Use attribute access or `.to_dict()`, and
reach the underlying instance via `._async` if needed.

## Query features

### Filtering with ordering and pagination

```python
# Single column, ascending
tasks = await Task.filter(status="open", order_by="priority")

# Multi-column, descending with "-" prefix
tasks = await Task.filter(
    project_id=1,
    order_by=["-priority", "-created_at"],
    limit=20,
    offset=40,
)
```

### Counting and existence checks

```python
total = await Task.count(project_id=1)
has_pending = await Task.exists(project_id=1, status="pending")
```

### SQL expressions in updates

Use `RawSQL` for server-side expressions like `NOW()` or column references:

```python
from auniqorm import RawSQL

await task.update(status="done", updated_at=RawSQL("NOW()"))
await counter.update(hits=RawSQL("hits + 1"))
await comment.update(resolved=RawSQL("NOT resolved"))
```

The instance is automatically re-fetched to sync local state for `RawSQL` fields.

### Portable raw SQL placeholders

Raw SQL passed to `execute`, `fetch_all`, `fetch_one`, `fetch_val`, `iterate`,
`iterate_batches`, and `execute_many` normally uses the connected driver's
native placeholder style (`$1` for PostgreSQL, `%s` for MySQL, `?` for SQLite).
Pass `paramstyle=` to write portable SQL with one canonical style and have it
translated for the active dialect:

```python
# qmark (?) — bind from a sequence
await db.fetch_all("SELECT * FROM users WHERE score = ?", [10], paramstyle="qmark")

# named (:name) — bind from a mapping
await db.fetch_one("SELECT * FROM users WHERE id = :id", {"id": 1}, paramstyle="named")

# auto — named for mappings, qmark for sequences
await db.fetch_val(
    "SELECT COUNT(*) FROM users WHERE name = :n", {"n": "a"}, paramstyle="auto"
)
```

Translation is literal-aware (string literals, quoted identifiers, comments and
PostgreSQL dollar-quoting are left untouched). The default is `None` — no
translation — so existing native-placeholder SQL is unaffected.

### Upsert (PostgreSQL / CockroachDB)

```python
await user.insert(on_conflict="(email) DO UPDATE SET name = EXCLUDED.name")
await link.insert(on_conflict="DO NOTHING")
```

### Bulk Insert

Insert many rows in a single database round trip.  PostgreSQL uses
`unnest()` for a fixed-size query; MySQL and SQLite use multi-row `VALUES`.

```python
items = [ProjectMedia(project_id=1, media_id=m) for m in media_ids]
await ProjectMedia.bulk_insert(items, on_conflict="DO NOTHING")
```

### Distinct Values

Retrieve unique values of a single column as a flat list of scalars.

```python
labels = await Annotation.distinct("label")
# ["bike", "car", "person"]

types = await Annotation.distinct("annotation_type", order_by="-annotation_type")
# Supports equality filters
labels = await Annotation.distinct("label", media_id=42)
```

### Pydantic interop

aUniqORM models expose `.to_dict()` for easy conversion to/from pydantic schemas
— no adapter layer needed:

```python
from pydantic import BaseModel


class UserResponse(BaseModel):
    id: int
    name: str
    email: str


# aUniqORM → pydantic
user = await User.get(id=1)
response = UserResponse(**user.to_dict())

# pydantic → aUniqORM
payload = UserCreate(name="Alice", email="alice@example.com")
user = User(**payload.model_dump())
await user.insert()
```

## Native drivers

aUniqORM includes Rust implementations for SQLite, PostgreSQL, and MySQL.
When the native extension is installed, these drivers are selected by default
so applications get the fastest implementation without configuration. If the
extension is unavailable, aUniqORM uses the standard Python driver.

Set the backend-specific environment variable to `0` to opt out:

```bash
AUNIQORM_SQLITE_NATIVE=0
AUNIQORM_PG_NATIVE=0
AUNIQORM_MYSQL_NATIVE=0
```

The native package is built from the Rust crate in [`rust/`](rust/). Native
integration tests that require a running PostgreSQL or MySQL service remain
skipped unless those services are available.

## Benchmarking

A dedicated benchmark compares aUniqORM against well-known Python ORMs on the
same machine and dataset:

```bash
uv sync --extra benchmark
python benchmarks/orm_comparison.py
```

The benchmark currently compares:

- aUniqORM
- Yara ORM
- SQLAlchemy ORM (async)
- Tortoise ORM

It measures nine steady-state scenarios:

- inserting rows through ORM models inside a single transaction
- repeated primary-key lookups
- repeated indexed equality filters
- aggregate queries (COUNT + SUM with GROUP BY)
- F-expression updates (column arithmetic in WHERE + SET)
- get-or-create (lookup with defaults)
- partial column selects
- prefetch_related (forward FK batch loading)
- many-to-many relationship queries

### Multi-database support

The benchmark supports SQLite (default), PostgreSQL, and MySQL via the
`--database` flag. For PostgreSQL and MySQL, start the services first:

```bash
docker compose -f benchmarks/compose.yaml up -d --wait
python benchmarks/orm_comparison.py --database postgres
python benchmarks/orm_comparison.py --database mysql
docker compose -f benchmarks/compose.yaml down
```

Both services use tmpfs for storage, ensuring a clean state between runs and
avoiding disk I/O variance.

### CLI options

Default settings are tuned for local development rather than CI. You can adjust
the workload size, repeats, and warmups from the command line:

```bash
python benchmarks/orm_comparison.py --database sqlite --rows 10000 --lookups 10000 --filters 2000 --repeats 7
```

This is intended as a practical regression benchmark, not a universal ORM
shootout. Keep the Python version, hardware, database version, and dependency
versions fixed if you want meaningful before/after comparisons.

### Benchmark Results

The following run was collected on 2026-10-13 with Python 3.12, 5,000 seeded
rows, 5,000 primary-key lookups, 1,000 operations for the other scenarios,
five measured repetitions, and one warmup. The aggregate workload groups by
`score` and materializes `COUNT(id)` and `SUM(score)` results.

The aUniqORM rows use the native Rust engines (tokio-postgres, mysql_async, and
rusqlite) by default:

```bash
python benchmarks/orm_comparison.py --database sqlite
python benchmarks/orm_comparison.py --database postgres
python benchmarks/orm_comparison.py --database mysql
```

Set the corresponding `AUNIQORM_*_NATIVE=0` variable to benchmark the standard
Python drivers (asyncpg, aiomysql, or aiosqlite).

#### SQLite

| scenario | library | ops | median(s) | best(s) | ops/s | vs_best |
|---|---|---|---|---|---|---|
| **aggregate** | **auniqorm** | **1000** | **0.1645** | **0.1620** | **6080.8** | **1.00x** |
| aggregate | sqlalchemy | 1000 | 0.4236 | 0.3955 | 2360.5 | 2.58x |
| aggregate | tortoise | 1000 | 0.3685 | 0.3654 | 2713.7 | 2.24x |
| aggregate | yara | 1000 | 0.2540 | 0.2425 | 3937.7 | 1.54x |
| **filter** | **auniqorm** | **1000** | **0.0214** | **0.0207** | **46838.0** | **1.00x** |
| filter | sqlalchemy | 1000 | 0.2424 | 0.2396 | 4126.1 | 11.35x |
| filter | tortoise | 1000 | 0.1969 | 0.1717 | 5077.8 | 9.22x |
| filter | yara | 1000 | 0.0871 | 0.0818 | 11479.3 | 4.08x |
| **get** | **auniqorm** | **5000** | **0.0133** | **0.0129** | **376914.8** | **1.00x** |
| get | sqlalchemy | 5000 | 0.7052 | 0.6742 | 7089.9 | 53.16x |
| get | tortoise | 5000 | 0.2816 | 0.2635 | 17754.8 | 21.23x |
| get | yara | 5000 | 0.1521 | 0.1511 | 32877.8 | 11.46x |
| **get_or_create** | **auniqorm** | **1000** | **0.0037** | **0.0036** | **272289.8** | **1.00x** |
| get_or_create | sqlalchemy | 1000 | 0.1406 | 0.1360 | 7111.3 | 38.29x |
| get_or_create | tortoise | 1000 | 0.0610 | 0.0549 | 16402.4 | 16.60x |
| get_or_create | yara | 1000 | 0.0378 | 0.0325 | 26482.8 | 10.28x |
| **insert** | **auniqorm** | **5000** | **0.0071** | **0.0069** | **700596.5** | **1.00x** |
| insert | sqlalchemy | 5000 | 0.3076 | 0.2995 | 16254.0 | 43.10x |
| insert | tortoise | 5000 | 4.8877 | 4.7887 | 1023.0 | 684.86x |
| insert | yara | 5000 | 0.0134 | 0.0133 | 372848.7 | 1.88x |
| **m2m** | **auniqorm** | **1000** | **0.0531** | **0.0528** | **18843.6** | **1.00x** |
| m2m | sqlalchemy | 1000 | 1.0816 | 1.0690 | 924.6 | 20.38x |
| m2m | tortoise | 1000 | 0.5953 | 0.5841 | 1680.0 | 11.22x |
| m2m | yara | 1000 | 0.2020 | 0.1837 | 4951.2 | 3.81x |
| **partial_select** | **auniqorm** | **1000** | **0.0214** | **0.0213** | **46648.1** | **1.00x** |
| partial_select | sqlalchemy | 1000 | 0.1589 | 0.1542 | 6292.1 | 7.41x |
| partial_select | tortoise | 1000 | 0.1091 | 0.0928 | 9163.2 | 5.09x |
| partial_select | yara | 1000 | 0.0702 | 0.0673 | 14235.3 | 3.28x |
| **prefetch** | **auniqorm** | **1000** | **0.0363** | **0.0362** | **27550.8** | **1.00x** |
| prefetch | sqlalchemy | 1000 | 0.8771 | 0.8640 | 1140.1 | 24.16x |
| prefetch | tortoise | 1000 | 0.4332 | 0.4171 | 2308.3 | 11.94x |
| prefetch | yara | 1000 | 0.1561 | 0.1537 | 6406.4 | 4.30x |
| **update_expr** | **auniqorm** | **1000** | **0.1318** | **0.1297** | **7589.3** | **1.00x** |
| update_expr | sqlalchemy | 1000 | 0.5764 | 0.5690 | 1734.9 | 4.37x |
| update_expr | tortoise | 1000 | 0.2988 | 0.2933 | 3346.6 | 2.27x |
| update_expr | yara | 1000 | 0.1883 | 0.1744 | 5309.7 | 1.43x |

#### PostgreSQL

| scenario | library | ops | median(s) | best(s) | ops/s | vs_best |
|---|---|---|---|---|---|---|
| **aggregate** | **auniqorm** | **1000** | **0.4088** | **0.3969** | **2446.4** | **1.00x** |
| aggregate | sqlalchemy | 1000 | 0.4885 | 0.4795 | 2046.9 | 1.20x |
| aggregate | tortoise | 1000 | 0.5435 | 0.5394 | 1840.1 | 1.33x |
| aggregate | yara | 1000 | 0.4408 | 0.4357 | 2268.6 | 1.08x |
| **filter** | **auniqorm** | **1000** | **0.0792** | **0.0699** | **12622.4** | **1.00x** |
| filter | sqlalchemy | 1000 | 0.2209 | 0.2064 | 4527.5 | 2.79x |
| filter | tortoise | 1000 | 0.2457 | 0.2340 | 4070.7 | 3.10x |
| filter | yara | 1000 | 0.1176 | 0.1026 | 8500.5 | 1.48x |
| **get** | **auniqorm** | **5000** | **0.2738** | **0.2327** | **18259.2** | **1.00x** |
| get | sqlalchemy | 5000 | 0.6630 | 0.6276 | 7541.0 | 2.42x |
| get | tortoise | 5000 | 0.6709 | 0.6291 | 7452.8 | 2.45x |
| **get** | **yara** | **5000** | **0.2740** | **0.2557** | **18247.3** | **1.00x** |
| **get_or_create** | **auniqorm** | **1000** | **0.0512** | **0.0491** | **19528.7** | **1.00x** |
| get_or_create | sqlalchemy | 1000 | 0.1245 | 0.1142 | 8033.6 | 2.43x |
| get_or_create | tortoise | 1000 | 0.1340 | 0.1249 | 7460.1 | 2.62x |
| get_or_create | yara | 1000 | 0.0587 | 0.0503 | 17035.1 | 1.15x |
| **insert** | **auniqorm** | **5000** | **0.0156** | **0.0148** | **321101.0** | **1.00x** |
| insert | sqlalchemy | 5000 | 0.1003 | 0.0990 | 49844.4 | 6.44x |
| insert | tortoise | 5000 | 0.4320 | 0.4217 | 11573.1 | 27.75x |
| insert | yara | 5000 | 0.0296 | 0.0285 | 168808.7 | 1.90x |
| **m2m** | **auniqorm** | **1000** | **0.2576** | **0.2331** | **3882.4** | **1.00x** |
| m2m | sqlalchemy | 1000 | 1.7440 | 1.7245 | 573.4 | 6.77x |
| m2m | tortoise | 1000 | 1.1735 | 0.7882 | 852.1 | 4.56x |
| m2m | yara | 1000 | 0.2970 | 0.2783 | 3366.5 | 1.15x |
| **partial_select** | **auniqorm** | **1000** | **0.0812** | **0.0793** | **12321.1** | **1.00x** |
| partial_select | sqlalchemy | 1000 | 0.1437 | 0.1406 | 6958.9 | 1.77x |
| partial_select | tortoise | 1000 | 0.1699 | 0.1532 | 5885.9 | 2.09x |
| partial_select | yara | 1000 | 0.1010 | 0.0946 | 9898.0 | 1.24x |
| **prefetch** | **auniqorm** | **1000** | **0.1964** | **0.1918** | **5091.6** | **1.00x** |
| prefetch | sqlalchemy | 1000 | 0.9711 | 0.9201 | 1029.7 | 4.94x |
| prefetch | tortoise | 1000 | 0.6230 | 0.5731 | 1605.0 | 3.17x |
| prefetch | yara | 1000 | 0.3971 | 0.3888 | 2518.5 | 2.02x |
| update_expr | auniqorm | 1000 | 1.0896 | 1.0841 | 917.8 | 1.01x |
| update_expr | sqlalchemy | 1000 | 1.3208 | 1.3006 | 757.1 | 1.23x |
| update_expr | tortoise | 1000 | 2.0799 | 2.0731 | 480.8 | 1.93x |
| **update_expr** | **yara** | **1000** | **1.0775** | **1.0593** | **928.1** | **1.00x** |

#### MySQL

| scenario | library | ops | median(s) | best(s) | ops/s | vs_best |
|---|---|---|---|---|---|---|
| **aggregate** | **auniqorm** | **1000** | **0.3937** | **0.3915** | **2539.8** | **1.00x** |
| aggregate | sqlalchemy | 1000 | 0.7163 | 0.6903 | 1396.0 | 1.82x |
| aggregate | tortoise | 1000 | 0.6930 | 0.6818 | 1443.0 | 1.76x |
| aggregate | yara | 1000 | 0.4507 | 0.4429 | 2218.6 | 1.14x |
| **filter** | **auniqorm** | **1000** | **0.0920** | **0.0888** | **10870.7** | **1.00x** |
| filter | sqlalchemy | 1000 | 0.3812 | 0.3693 | 2623.5 | 4.14x |
| filter | tortoise | 1000 | 0.3365 | 0.3300 | 2972.1 | 3.66x |
| filter | yara | 1000 | 0.1268 | 0.1185 | 7885.9 | 1.38x |
| **get** | **auniqorm** | **5000** | **0.2330** | **0.2296** | **21456.1** | **1.00x** |
| get | sqlalchemy | 5000 | 0.8385 | 0.8260 | 5963.0 | 3.60x |
| get | tortoise | 5000 | 0.5536 | 0.5336 | 9032.3 | 2.38x |
| **get** | **yara** | **5000** | **0.2331** | **0.2218** | **21452.2** | **1.00x** |
| **get_or_create** | **auniqorm** | **1000** | **0.0489** | **0.0462** | **20469.9** | **1.00x** |
| get_or_create | sqlalchemy | 1000 | 0.1584 | 0.1492 | 6314.3 | 3.24x |
| get_or_create | tortoise | 1000 | 0.1128 | 0.1118 | 8865.5 | 2.31x |
| get_or_create | yara | 1000 | 0.0500 | 0.0483 | 19988.5 | 1.02x |
| **insert** | **auniqorm** | **5000** | **0.0238** | **0.0236** | **209665.6** | **1.00x** |
| insert | sqlalchemy | 5000 | 0.3250 | 0.3069 | 15384.4 | 13.63x |
| insert | tortoise | 5000 | 0.3848 | 0.3612 | 12994.5 | 16.14x |
| insert | yara | 5000 | 0.0275 | 0.0273 | 181730.1 | 1.15x |
| **m2m** | **auniqorm** | **1000** | **0.3506** | **0.3129** | **2852.0** | **1.00x** |
| m2m | sqlalchemy | 1000 | 2.2968 | 2.2736 | 435.4 | 6.55x |
| m2m | tortoise | 1000 | 1.7051 | 1.6882 | 586.5 | 4.86x |
| m2m | yara | 1000 | 0.9693 | 0.9528 | 1031.7 | 2.76x |
| **partial_select** | **auniqorm** | **1000** | **0.1031** | **0.0962** | **9700.7** | **1.00x** |
| partial_select | sqlalchemy | 1000 | 0.2724 | 0.2522 | 3671.0 | 2.64x |
| partial_select | tortoise | 1000 | 0.2390 | 0.2374 | 4183.6 | 2.32x |
| partial_select | yara | 1000 | 0.1201 | 0.1141 | 8329.2 | 1.16x |
| **prefetch** | **auniqorm** | **1000** | **0.1814** | **0.1809** | **5511.7** | **1.00x** |
| prefetch | sqlalchemy | 1000 | 1.1374 | 1.1344 | 879.2 | 6.27x |
| prefetch | tortoise | 1000 | 0.7356 | 0.7230 | 1359.4 | 4.05x |
| prefetch | yara | 1000 | 0.2534 | 0.2408 | 3946.1 | 1.40x |
| **update_expr** | **auniqorm** | **1000** | **1.0489** | **1.0363** | **953.4** | **1.00x** |
| update_expr | sqlalchemy | 1000 | 1.3165 | 1.2722 | 759.6 | 1.26x |
| update_expr | tortoise | 1000 | 1.0893 | 1.0680 | 918.0 | 1.04x |
| update_expr | yara | 1000 | 1.0618 | 1.0368 | 941.8 | 1.01x |

In this run, aUniqORM has the lowest median in 26 of 27 combinations, while
Yara has the lowest median in 1. aUniqORM is faster than SQLAlchemy and
Tortoise in every measured combination. These are local measurements rather
than general performance claims; rerun the benchmark on your target workload
and environment before drawing operational conclusions.

## Development

Clone the repository and install the development dependencies:

```bash
git clone https://github.com/AxisCommunications/auniqorm.git
cd auniqorm
uv sync --extra dev
```

Run the test suite and repository checks locally:

```bash
uv run python -m pytest
uv run pre-commit run --all-files
cargo fmt --manifest-path rust/Cargo.toml --check
cargo check --manifest-path rust/Cargo.toml --locked
```

The benchmark dependencies and database services are optional. See
[`benchmarks/compose.yaml`](benchmarks/compose.yaml) for the PostgreSQL and
MySQL development services, and [`RELEASING.md`](RELEASING.md) for the release
process.
