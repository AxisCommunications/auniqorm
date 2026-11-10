<p align="left">
  <img src="auniqorm.png" alt="auniqorm logo" width="100%">
</p>

# aUniqORM

**One Python data layer for SQL and documents.**

aUniqORM is a unified, typed Python data-mapping layer for relational and
document databases. It provides consistent connection management, async and
synchronous APIs, transactions, serialization, and database-native query
access without flattening every backend into the same lowest-common
denominator.

Use the relational ORM for SQL databases and the document API for MongoDB.
Rust-powered relational drivers provide the default database path for
performance-sensitive workloads.

## Why aUniqORM?

- **Async by default** with `asyncio`-native database access.
- **Typed models** built around Python annotations and [msgspec](https://github.com/msgspec/msgspec).
- **Multiple data models** through relational and document backends.
- **Practical ORM features** including relationships, migrations, transactions,
  pagination, bulk inserts, upserts, and prefetching.
- **Portable SQL** with literal-aware placeholder translation when raw SQL is
  needed.
- **Synchronous access** through a blocking adapter without maintaining a
  separate model layer.
- **Rust-powered drivers** for SQLite, PostgreSQL, and MySQL, with an explicit
  opt-out when the standard Python drivers are preferred.
- **Native query access** through SQL or MongoDB operation documents.

## Contents

- [Installation](#installation)
- [Supported databases](#supported-databases)
- [Quick start](#quick-start)
- [Synchronous usage](#synchronous-usage)
- [Transactions](#transactions)
- [Query features](#query-features)
- [Document databases](#document-databases)
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

## Transactions

```python
async with await db.transaction():
    await Account.update_where({"balance": F("balance") - amount}, id=src)
    await Account.update_where({"balance": F("balance") + amount}, id=dst)
```

A transaction, or a connection pinned with `async with await db.connection():`,
belongs to the asyncio task that opened it and to tasks that task creates inside
the block, for every driver. Concurrent tasks sharing one `Database`, such as
separate web requests, each get their own transaction; statements from other
tasks never run inside it, read its uncommitted changes, or commit or roll it
back. With `SyncDatabase`, a transaction belongs to the thread that opened it.

- Tasks created inside a transaction that are still running after it ends get
  `QueryError` instead of reusing the released connection.
- A transaction opened inside another one in the same task becomes a savepoint:
  leaving the inner block with an exception rolls back only its changes.
- Cancelling a task inside a transaction rolls it back and returns its connection.
- SQLite uses a single connection, so while one task's transaction is open,
  statements from other tasks wait until it ends. Keep SQLite transactions short,
  and do not await work in other tasks that needs the database from inside one.
- The aiomysql driver now runs pooled statements in autocommit mode (pass
  `autocommit=False` to opt out), so pooled reads no longer leave transactions
  open on returned connections.

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

### Identifier and pattern safety

Values are always bound as parameters; identifiers are validated instead:

- Table, column, `ForeignKey`, `ManyToMany` and `dynamic_model()` names must
  match `[A-Za-z_][A-Za-z0-9_]*` (tables may have one `schema.` prefix), checked
  when the model is defined.
- Names supplied at query time — filter/`Q` keys, `order_by`, `columns=`,
  `values()`/`group_by`, aggregate columns, update keys and `F()` columns — must
  be model fields (or aggregate aliases in `values(order_by=...)`), else
  `ValueError`. Validated SQL is cached, so this costs nothing on repeat calls.
- `F()` arithmetic accepts only `F` and finite `int`/`float` operands.
- `contains`/`startswith`/`endswith` and their `i` variants escape `%`, `_` and
  `\`, so the value matches literally; `iexact` is `LOWER(col) = LOWER(?)` on
  every dialect (no wildcards).
- `pandas_io.to_sql()` quotes table and column names like pandas does
  (case-sensitive on PostgreSQL).

`RawSQL`, `on_conflict`, `server_default`, `db_type` and raw query text are
still SQL: only pass trusted strings there.

## Document databases

MongoDB uses its native document API rather than relational model DDL. It shares
aUniqORM's connection, transaction, and async/sync foundations while keeping
MongoDB's native operation shapes.

### MongoDB

Install the optional backend:

```bash
uv add "auniqorm[mongodb]"
```

Use `db.collection()` for a keyword-based API. Filters use MongoDB query
syntax, and update operators are keyword arguments (`set` → `$set`, `inc` →
`$inc`, `unset`, `push`, `add_to_set`, `current_date`, …):

```python
from datetime import UTC, datetime

async with Database("mongodb://localhost/example?tz_aware=true") as db:
    users = db.collection("users")
    user_id = await users.insert_one(
        {"name": "Ada", "created": datetime.now(UTC)}
    )
    await users.update_one(
        {"_id": user_id},
        set={"profile.name": "Ada Lovelace"},
        inc={"login_count": 1},
    )
    recent = await users.find(
        {"created": {"$gte": datetime(2026, 1, 1, tzinfo=UTC)}},
        projection={"name": 1},
        sort={"created": -1},
        limit=50,
    )
```

Reads accept `hint`, `collation`, and `max_time_ms`; updates also accept
`array_filters`, and replace/delete accept `collation` and `hint`. The same
options are available on the `bulk_write` request classes:

```python
await users.find(
    {"name": "ada"},
    collation={"locale": "en", "strength": 2},
    hint="name_1",
    max_time_ms=500,
)
await users.update_many(
    {}, set={"scores.$[s]": 100}, array_filters=[{"s": {"$gte": 90}}]
)
```

Values keep their native BSON types (`ObjectId`, Date, `Decimal128`, UUID,
binary, …) because operations are encoded with PyMongo's BSON encoder and the
client's codec options. Set codec options such as `uuidRepresentation=standard`
or `tz_aware=true` in the URL exactly as with PyMongo. Pass `update=` for any
other update document or an aggregation pipeline. `SyncDatabase.collection()`
provides the same API without `await`.

Send many writes in as few round trips as possible with `bulk_write`.
Consecutive writes of the same kind are batched into one command:

```python
from auniqorm import DeleteMany, InsertOne, UpdateOne

result = await users.bulk_write(
    [
        InsertOne({"name": "Grace"}),
        UpdateOne({"name": "Ada"}, inc={"login_count": 1}),
        DeleteMany({"active": False}),
    ],
    ordered=True,
)
print(result.inserted_count, result.modified_count, result.deleted_count)
```

Failed writes raise `BulkWriteError`; its `details` match PyMongo's
`BulkWriteError.details`.

Stream large results with `iterate` (one document at a time) or
`iterate_batches` (lists of up to `batch_size`). The native driver fetches the
next batches while you process the current one, and stopping early closes the
server cursor:

```python
async for user in users.iterate({"active": True}, batch_size=1000):
    process(user)
```

Run aggregation pipelines and counts:

```python
totals = await users.aggregate(
    [{"$group": {"_id": "$country", "logins": {"$sum": "$login_count"}}}]
)
active = await users.count_documents({"active": True})
countries = await users.distinct("country")
```

Large `find` and `aggregate` results arrive in batches: the native driver
returns the first batch immediately and fetches the rest while Python decodes
it.

Manage indexes and collections:

```python
await users.create_index("email", unique=True)
await users.create_index("session_start", expire_after_seconds=3600)
await users.create_index([("bio", "text")])
await users.drop_index("email_1")
await users.drop()
```

Transactions require a replica set or sharded cluster. Every collection
operation inside the block joins the transaction:

```python
async with await db.transaction():
    await accounts.update_one({"_id": src}, inc={"balance": -amount})
    await accounts.update_one({"_id": dst}, inc={"balance": amount})
```

A transaction belongs to the asyncio task that started it and to tasks that
task creates inside the block. Concurrent tasks, such as separate web
requests, can each run their own transaction on the same `Database`, and
operations in other tasks never join it. With `SyncDatabase`, a transaction
belongs to the thread that started it. Using a transaction after it has ended
raises `QueryError`.

With the native driver, transactions run in Rust too. An operation the native
driver cannot run inside a transaction, such as `drop` or `create_index`, raises
`QueryError` instead of silently running outside it; set
`AUNIQORM_MONGODB_NATIVE=0` to run such transactions through PyMongo.

When the native extension is installed, reads, writes, bulk writes, aggregation,
and transactions use the Rust driver by default, with raw BSON passed straight
through. Index and drop operations, options the native path does not support,
and any connection URL the Rust driver rejects fall back to PyMongo
automatically. Set `AUNIQORM_MONGODB_NATIVE=0` to force the PyMongo
implementation.

#### Raw JSON operations

The collection API is the recommended MongoDB interface. As a raw escape hatch,
the generic `Database.execute`, `fetch_one`, `fetch_all`, `fetch_val`, and
`iterate` methods also accept MongoDB operations as JSON documents:

```python
await db.execute(
    '{"operation":"update_one","collection":"users",'
    '"filter":{"name":"Ada"},"update":{"$inc":{"login_count":1}}}'
)
active = await db.fetch_all(
    '{"operation":"find","collection":"users","filter":{"active":true}}'
)
```

The `operation` field names a collection method (`insert_one`, `find`,
`aggregate`, `create_index`, …) and the remaining fields are its arguments:
`filter` for the query, and otherwise the collection API's names
(`projection`, `sort`, `update`, `upsert`, `hint`, …). JSON operations run
through the same implementation as the collection API, but values are limited
to JSON types: there is no way to express an `ObjectId`, a date, or a
`Decimal128`, and `bulk_write` is not available.

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

### Native driver TLS and session safety

**TLS.** The native drivers honour the same TLS settings as the Python drivers;
a requested TLS mode is never silently downgraded.

- **PostgreSQL** follows libpq/asyncpg semantics. `sslmode` comes from the
  `ssl=` keyword, the URL, or `PGSSLMODE`, and defaults to `prefer` as in
  asyncpg:

  | `sslmode` | Behaviour |
  |-----------|-----------|
  | `disable` | Plaintext. |
  | `allow`, `prefer` | TLS without certificate verification; plaintext only if the server refuses TLS. |
  | `require` | TLS is mandatory. The chain is verified only if a root certificate is available (`sslrootcert`, `PGSSLROOTCERT` or `~/.postgresql/root.crt`), as in libpq. |
  | `verify-ca` | TLS; the chain must verify against the root certificate. |
  | `verify-full` | As `verify-ca`, and the host name must match. |

  `ssl=True` means `verify-full` against the system trust store
  (`ssl.create_default_context()` in asyncpg), and `ssl=False` means `disable`.
  `sslrootcert` may name a PEM file or `system`. Other TLS settings (client
  certificates, CRLs, protocol-version bounds) are not implemented natively:
  set through `PGSSLCERT`-style environment variables or the default
  `~/.postgresql` files, they select asyncpg, and as URL parameters they are
  rejected.
- **MySQL** connects without TLS unless asked, like aiomysql. `ssl=True`
  requires TLS and verifies the chain and host name against the system trust
  store. `ssl=` also accepts MySQL modes: `"disabled"`, `"preferred"`
  (plaintext only if the server lacks TLS), `"required"` (encrypted, not
  verified) and `"verify_identity"`. `"verify_ca"` raises an error because
  mysql_async cannot skip only the host-name check. mysql_async's
  `require_ssl`, `verify_ca` and `built_in_roots` URL parameters also work;
  unknown URL parameters (such as `ssl_ca`) are rejected.
- **Unsupported options.** A keyword option that a native driver cannot honour
  exactly, such as `ssl=ssl.SSLContext(...)` or a driver-specific keyword,
  selects asyncpg or aiomysql instead. If that package is not installed,
  `connect()` raises `DriverNotFoundError`. The native MongoDB client receives
  `Database` keyword options (`tls`, `tlsCAFile`, `authSource`, credentials,
  and so on) as connection-string options. If any option cannot be expressed
  that way (SSL contexts, event listeners, type registries, callables), the
  driver disables the native client for that instance and uses PyMongo.

```python
db = Database("postgresql://app@db.internal/app?sslrootcert=/etc/ssl/db-ca.pem",
              ssl="verify-full")
```

The default `prefer` encrypts when the server offers TLS but does not verify
the server, so it does not protect against an active attacker. Use
`verify-full` (or `ssl=True`) for connections that leave a trusted network.

**PostgreSQL values.** Integers are range-checked and parameters must match
the column type; nothing wraps or binds silently. Results decode to asyncpg's
types. Values the native driver cannot represent raise instead of becoming
`None`: `interval`, `timetz`, multi-dimensional arrays, `numeric` beyond 28
significant digits or non-finite, and infinite or out-of-range dates.

**Session state.** Only pinned connections and transactions are reset when
they return to the pool. The native drivers do not reset the session after a
standalone pooled statement, because that would add a round trip to every
query. A `SET`, `SET ROLE`, temporary table, user variable, advisory or
`GET_LOCK` lock, or `LISTEN` run as a standalone statement therefore stays on
that pooled connection and can affect later requests, including requests for a
different tenant or user when the pool is shared. Change session-level state
only inside a transaction (`async with await db.transaction():`) or on a pinned
connection (`async with await db.connection():`). Do not run `SET ROLE` or
row-level-security settings such as `SET app.tenant_id` outside one of those.

When such a connection returns to the pool, the native driver resets its
session:

- PostgreSQL runs `CLOSE ALL`, `SET SESSION AUTHORIZATION DEFAULT`,
  `RESET ALL`, `UNLISTEN *`, `pg_advisory_unlock_all()`, `DISCARD TEMP` and
  `DISCARD SEQUENCES` in the same round trip as the final `COMMIT` or
  `ROLLBACK`, and keeps prepared statements cached. In local measurements this
  added about 5 µs per transaction.
- MySQL runs `COM_RESET_CONNECTION` in the background after `COMMIT` or
  `ROLLBACK` returns, then prepares the transaction's statements again. The
  connection is not reused until both have finished, and closing the engine
  waits for them. In local measurements this added about 4 µs per transaction.

If the reset fails, the driver discards the connection instead of returning it
to the pool. If a transaction or pinned connection is abandoned, for example
because its task was cancelled, the driver rolls it back and resets it in the
background before reusing the connection.

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

### MongoDB

The relational benchmark does not apply to MongoDB, so three separate scripts
measure the document API against a live MongoDB service. They need the
`mongo-benchmark` extra and the MongoDB service from
[`benchmarks/compose.yaml`](benchmarks/compose.yaml), which runs as a
single-node replica set so transactions can be measured:

```bash
uv sync --extra mongo-benchmark
docker compose -f benchmarks/compose.yaml up -d --wait mongodb
python benchmarks/mongo_paired.py
python benchmarks/mongo_odm_comparison.py
python benchmarks/mongo_features.py
docker compose -f benchmarks/compose.yaml down
```

[`mongo_paired.py`](benchmarks/mongo_paired.py) is the main comparison with
PyMongo. Each scenario runs as alternating blocks of the same operations with
each library's natural API, swapping the order every round, and reports how
many rounds each library won, so host noise cannot pass for a win. Every
scenario runs back to back (`sequential`), with an idle gap before each
operation like a request handler waiting for work (`idle`; only operation
time counts), and with 32 operations in flight (`concurrent`). Tune it with
`AUNIQORM_PAIRED_ROUNDS`, `AUNIQORM_PAIRED_OPS`, `AUNIQORM_PAIRED_MODES`,
`AUNIQORM_PAIRED_SCENARIOS`, and `AUNIQORM_PAIRED_GAP_MS`.

[`mongo_odm_comparison.py`](benchmarks/mongo_odm_comparison.py) adds Beanie and
ODMantic, comparing the collection API (`auniqorm`) and raw JSON operations
(`auniqorm_json`) on insert-many, point reads, filtered reads, and atomic
updates. Tune it with `AUNIQORM_MONGO_BENCH_ITERATIONS`,
`AUNIQORM_MONGO_BENCH_ROUNDS`, and `AUNIQORM_MONGO_BENCH_LIBRARIES` (for
example `auniqorm,pymongo`).

[`mongo_features.py`](benchmarks/mongo_features.py) compares aUniqORM with the
equivalent PyMongo code for bulk writes, cursor streaming, aggregation,
transactions, JSON reads, and query options. Select scenarios with
`AUNIQORM_FEATURE_BENCH` (for example `bulk_update,stream`) and rounds with
`AUNIQORM_FEATURE_BENCH_ROUNDS`. `AUNIQORM_FEATURE_BENCH_OUT=before.json` saves
the medians and `AUNIQORM_FEATURE_BENCH_BASE=before.json` prints the change
against them, for before/after comparisons.

All scripts print whether the native MongoDB driver is active; it is used by
default when the native extension is installed.

#### MongoDB Results

The following runs were collected on 2026-10-15 with Python 3.12, MongoDB 7.0
as a single-node replica set, PyMongo 4.18, Beanie 2.2, and ODMantic 1.1, with
the native MongoDB driver enabled. Lower is better. Writes use the replica
set's default majority write concern and wait for the journal, which makes
every single-document update take about one millisecond regardless of the
client.

Paired comparison with PyMongo: microseconds per operation (aUniqORM vs
PyMongo) and aUniqORM's wins out of 10 paired rounds of 200 operations
(fewer for the larger scenarios), with a 2 ms idle gap in `idle` mode:

| scenario | sequential | idle | concurrent |
|---|---|---|---|
| point read | **84** vs 130 (10/10) | **180** vs 222 (8/10) | **19** vs 83 (10/10) |
| point read, 10 KB document | **117** vs 154 (10/10) | **220** vs 283 (10/10) | **42** vs 115 (10/10) |
| filtered read | **102** vs 146 (10/10) | **221** vs 280 (9/10) | **22** vs 84 (10/10) |
| sorted find, limit 5 | **121** vs 148 (9/10) | **228** vs 282 (10/10) | **22** vs 95 (10/10) |
| read 1,000 documents | **831** vs 1,073 (7/10) | **1,019** vs 1,186 (7/10) | **839** vs 934 (7/10) |
| `insert_one` | **161** vs 220 (10/10) | **385** vs 407 (7/10) | **75** vs 91 (10/10) |
| `insert_one`, 10 KB document | **240** vs 282 (10/10) | **416** vs 471 (10/10) | **81** vs 141 (10/10) |
| `update_one` | 987 vs 985 (5/10) | 915 vs 906 (4/10) | 100 vs 100 (6/10) |
| `count_documents` | **202** vs 231 (10/10) | **332** vs 405 (10/10) | **33** vs 86 (10/10) |
| `$group` aggregation | **544** vs 555 (7/10) | **639** vs 658 (8/10) | **81** vs 123 (10/10) |
| transaction, 10 updates | **1,626** vs 1,987 (10/10) | **2,054** vs 2,564 (8/10) | **344** vs 902 (10/10) |

aUniqORM won 292 of 330 paired rounds. The one tie is `update_one`: every
update waits for MongoDB to journal it under the majority write concern, so
the server sets the pace for both clients, and under concurrency both reach
the server's throughput.

API comparison, median of five rotated rounds in milliseconds, 1,000
documents and 1,000 operations per scenario (`insert_many` is a single call
inserting 1,000 documents):

| operation | aUniqORM | aUniqORM JSON | PyMongo | Beanie | ODMantic |
|---|---|---|---|---|---|
| insert_many | **6.20** | 7.49 | 7.36 | 20.67 | 377.33 |
| point_read | 82.81 | **78.33** | 122.27 | 186.73 | 365.34 |
| filtered_read | **262.97** | 270.31 | 302.92 | 490.13 | 2272.35 |
| atomic_update | 985.70 | 987.40 | 969.98 | **941.96** | 1213.62 |

ODMantic's update scenario is read-modify-save because its high-level API does
not expose MongoDB atomic update operators; Beanie and ODMantic also include
model validation and document conversion. Atomic updates are bound by the
journal wait described above, so their differences are within run-to-run
variation.

Features, median of 11 rotated rounds in milliseconds:

| scenario | aUniqORM | PyMongo |
|---|---|---|
| 1,000 updates in one bulk write | **19.85** | 25.70 |
| stream 50,000 documents, `iterate` | **50.34** | 70.20 |
| stream 50,000 documents, `iterate_batches` | **44.14** | 70.20 |
| `$group` aggregation ×200 | **136.02** | 137.76 |
| aggregation returning 25,000 documents ×10 | **242.51** | 298.79 |
| `count_documents` + `distinct` ×200 | **76.70** | 95.72 |
| 100 transactions × 10 updates | **181.16** | 205.35 |
| JSON `find_one` ×1,000 | **87.10** | 126.53 |
| JSON sorted `find` with limit ×1,000 | **129.43** | 154.84 |
| JSON `hint` + `max_time_ms` ×1,000 | **108.85** | 148.56 |
| JSON `collation` ×1,000 | **377.30** | 414.99 |
| JSON `array_filters` ×1,000 | **165.66** | 184.54 |

The same 1,000 updates sent one `update_one` at a time take 981.82 ms with
PyMongo.

aUniqORM moves cursor iteration, BSON transport, and the network round trip
into Rust, and resolves results through a completion bridge that wakes only
the event loop thread, so it spends far less time on the Python thread than
PyMongo. That shows most under concurrency, where the event loop is the
bottleneck, and in transactions, which aUniqORM scopes to the task that
started them so concurrent requests run independent transactions on one
`Database`. Like the relational results, these are local measurements; rerun
the scripts on your own workload and environment.

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
[`benchmarks/compose.yaml`](benchmarks/compose.yaml) for the PostgreSQL,
MySQL, and MongoDB development services, and [`RELEASING.md`](RELEASING.md) for
the release process.
