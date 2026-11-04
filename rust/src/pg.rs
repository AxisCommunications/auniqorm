//! Native PostgreSQL engine for aUniqORM.
//!
//! Uses tokio-postgres + deadpool so the async DB work runs off the Python
//! event loop in Rust. Prepared statements are cached per connection, and
//! result rows are decoded straight into Python objects under a single GIL
//! pass (or into a msgpack buffer off the GIL).
//!
//! Session state: pooled single statements run on whichever connection is
//! free and are never followed by a reset (that would cost a round trip per
//! statement). Connections pinned through [`PgEngine::begin`] or
//! [`PgEngine::acquire`] are reset (`RESET ALL`, `SET SESSION AUTHORIZATION
//! DEFAULT`, `DISCARD TEMP`, …) in the same round trip as their final
//! `COMMIT`/`ROLLBACK` before they return to the pool, so session-level state
//! (`SET`, temp tables, advisory locks, `LISTEN`) must be set inside a
//! transaction or pinned connection.

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
use crate::pg_types::{
    Param, encode_msgpack, encode_msgpack_row, extract_params, row_to_pydict, rows_to_pylist,
};
use crate::tls::{pg_tls, split_ssl_params};
use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, RecyclingMethod};
use futures_util::StreamExt;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::error::Error as StdError;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::RwLock;
use tokio_postgres::config::SslMode;
use tokio_postgres::types::ToSql;
use tokio_postgres::{NoTls, SimpleQueryMessage};

type Arg<'py> = Bound<'py, PyAny>;
type Fut<'py> = PyResult<Bound<'py, PyAny>>;

fn err<E: StdError>(e: E) -> PyErr {
    let mut msg = e.to_string();
    let mut source = e.source();
    while let Some(cause) = source {
        let cause_msg = cause.to_string();
        if !msg.ends_with(&cause_msg) {
            msg.push_str(": ");
            msg.push_str(&cause_msg);
        }
        source = cause.source();
    }
    PyRuntimeError::new_err(msg)
}

fn ended() -> PyErr {
    PyRuntimeError::new_err("native PostgreSQL transaction/connection handle already ended")
}

/// Statements that return a pinned connection's session to its defaults
/// (`DISCARD ALL` minus `DEALLOCATE ALL`/`DISCARD PLANS`, which would drop the
/// per-connection prepared-statement cache). All are valid inside the
/// implicit transaction of a multi-statement simple query.
const RESET_SQL: &str = "CLOSE ALL; SET SESSION AUTHORIZATION DEFAULT; RESET ALL; UNLISTEN *; \
                         SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES";

/// What a query method returns to Python.
#[derive(Clone, Copy)]
enum Fetch {
    /// Rows affected.
    Execute,
    All,
    One,
    AllMsgpack,
    OneMsgpack,
}

/// Run one prepared (and cached) statement, converting the result for Python
/// (msgpack is encoded off the GIL).
async fn run(client: &Object, sql: &str, params: &[Param], fetch: Fetch) -> PyResult<Py<PyAny>> {
    let stmt = client.prepare_cached(sql).await.map_err(err)?;
    let refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p as _).collect();
    let bytes = |buf: Result<Vec<u8>, String>| {
        let buf = buf.map_err(PyRuntimeError::new_err)?;
        Python::attach(|py| Ok(PyBytes::new(py, &buf).into_any().unbind()))
    };
    match fetch {
        Fetch::Execute => {
            let affected = client.execute(&stmt, &refs).await.map_err(err)? as i64;
            Python::attach(|py| Ok(affected.into_pyobject(py)?.into_any().unbind()))
        }
        Fetch::All | Fetch::AllMsgpack => {
            let rows = client.query(&stmt, &refs).await.map_err(err)?;
            match fetch {
                Fetch::AllMsgpack => bytes(encode_msgpack(&rows)),
                _ => Python::attach(|py| rows_to_pylist(py, &rows)),
            }
        }
        Fetch::One | Fetch::OneMsgpack => {
            match client.query_opt(&stmt, &refs).await.map_err(err)? {
                None => Python::attach(|py| Ok(py.None())),
                Some(row) if matches!(fetch, Fetch::OneMsgpack) => bytes(encode_msgpack_row(&row)),
                Some(row) => Python::attach(|py| Ok(row_to_pydict(py, &row)?.into_any().unbind())),
            }
        }
    }
}

/// Run a simple-query batch, returning how many statements completed before
/// any error.
async fn run_batch(client: &Object, sql: &str) -> Result<(), (usize, tokio_postgres::Error)> {
    let mut completed = 0usize;
    let stream = client.simple_query_raw(sql).await.map_err(|e| (0, e))?;
    futures_util::pin_mut!(stream);
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(SimpleQueryMessage::CommandComplete(_)) => completed += 1,
            Ok(_) => {}
            Err(e) => return Err((completed, e)),
        }
    }
    Ok(())
}

/// End the transaction with `end` (`COMMIT`/`ROLLBACK`), reset the session in
/// the same round trip, and return the connection to the pool. On failure the
/// reset is retried once, then the connection is discarded rather than
/// pooled. Errors from `end` itself are returned.
async fn finish(client: Object, end: &'static str) -> PyResult<()> {
    let Err((completed, e)) = run_batch(&client, &format!("{end}; {RESET_SQL}")).await else {
        return Ok(());
    };
    let cleanup = format!("ROLLBACK; {RESET_SQL}");
    if client.is_closed() || run_batch(&client, &cleanup).await.is_err() {
        drop(Object::take(client));
    }
    if completed == 0 { Err(err(e)) } else { Ok(()) }
}

/// A pooled connection pinned to one handle. Dropping it while still held
/// rolls back and resets the session on a Tokio task (no GIL needed) before
/// the connection returns to the pool.
struct Pinned {
    conn: RwLock<Option<Object>>,
    closed: AtomicBool,
}

impl Drop for Pinned {
    fn drop(&mut self) {
        if let Some(client) = self.conn.get_mut().take() {
            pyo3_async_runtimes::tokio::get_runtime().spawn(finish(client, "ROLLBACK"));
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Created by `begin()`: COMMIT/ROLLBACK end the transaction and release.
    Transaction,
    /// Created by `acquire()`: COMMIT/ROLLBACK keep the connection pinned.
    Pinned,
}

#[pyclass]
pub struct PgEngine {
    pool: Pool,
}

impl PgEngine {
    fn query<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>, fetch: Fetch) -> Fut<'py> {
        let pool = self.pool.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            run(&pool.get().await.map_err(err)?, &sql, &params, fetch).await
        })
    }

    fn pin<'py>(&self, py: Python<'py>, mode: Mode) -> Fut<'py> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Wrapped first, so a cancelled BEGIN still resets the connection.
            let inner = Arc::new(Pinned {
                conn: RwLock::new(Some(pool.get().await.map_err(err)?)),
                closed: AtomicBool::new(false),
            });
            if mode == Mode::Transaction {
                let guard = inner.conn.read().await;
                guard
                    .as_ref()
                    .ok_or_else(ended)?
                    .batch_execute("BEGIN")
                    .await
                    .map_err(err)?;
            }
            Python::attach(|py| Py::new(py, PgTransaction { inner, mode }).map(Py::into_any))
        })
    }
}

#[pymethods]
impl PgEngine {
    fn execute<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>) -> Fut<'py> {
        self.query(py, sql, params, Fetch::Execute)
    }

    fn fetch_all<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>) -> Fut<'py> {
        self.query(py, sql, params, Fetch::All)
    }

    fn fetch_one<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>) -> Fut<'py> {
        self.query(py, sql, params, Fetch::One)
    }

    fn fetch_all_msgpack<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>) -> Fut<'py> {
        self.query(py, sql, params, Fetch::AllMsgpack)
    }

    fn fetch_one_msgpack<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>) -> Fut<'py> {
        self.query(py, sql, params, Fetch::OneMsgpack)
    }

    /// Pin a pooled connection and issue `BEGIN` (transaction mode).
    fn begin<'py>(&self, py: Python<'py>) -> Fut<'py> {
        self.pin(py, Mode::Transaction)
    }

    /// Pin a pooled connection without starting a transaction (pinned mode).
    fn acquire<'py>(&self, py: Python<'py>) -> Fut<'py> {
        self.pin(py, Mode::Pinned)
    }

    fn close<'py>(&self, py: Python<'py>) -> Fut<'py> {
        self.pool.close();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(()) })
    }
}

/// A connection pinned by `PgEngine.begin()` (transaction mode) or
/// `PgEngine.acquire()` (pinned mode).
///
/// Queries from several tasks may run concurrently (they pipeline on the
/// connection); `begin`/`commit`/`rollback`/`release` wait for in-flight
/// queries and run exclusively.
#[pyclass]
pub struct PgTransaction {
    inner: Arc<Pinned>,
    mode: Mode,
}

impl PgTransaction {
    fn ensure_open(&self) -> PyResult<()> {
        match self.inner.closed.load(Ordering::Acquire) {
            true => Err(ended()),
            false => Ok(()),
        }
    }

    fn query<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>, fetch: Fetch) -> Fut<'py> {
        self.ensure_open()?;
        let inner = self.inner.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.conn.read().await;
            run(guard.as_ref().ok_or_else(ended)?, &sql, &params, fetch).await
        })
    }

    /// Run a control statement exclusively on the still-pinned connection.
    fn control<'py>(&self, py: Python<'py>, sql: &'static str) -> Fut<'py> {
        self.ensure_open()?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.conn.write().await;
            guard
                .as_ref()
                .ok_or_else(ended)?
                .batch_execute(sql)
                .await
                .map_err(err)
        })
    }

    /// Close the handle and hand the connection to `finish` on a detached
    /// task, so cancelling the awaiting Python task cannot interrupt it.
    fn close_with<'py>(&self, py: Python<'py>, end: &'static str, report: bool) -> Fut<'py> {
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            return match report {
                true => Err(ended()),
                false => pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(()) }),
            };
        }
        let inner = self.inner.clone();
        let task = pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
            match inner.conn.write().await.take() {
                Some(client) => finish(client, end).await,
                None => Ok(()),
            }
        });
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = task
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            if report { result } else { Ok(()) }
        })
    }
}

#[pymethods]
impl PgTransaction {
    /// True once the connection has been returned to the pool.
    #[getter]
    fn closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    fn execute<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>) -> Fut<'py> {
        self.query(py, sql, params, Fetch::Execute)
    }

    fn fetch_all<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>) -> Fut<'py> {
        self.query(py, sql, params, Fetch::All)
    }

    fn fetch_one<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>) -> Fut<'py> {
        self.query(py, sql, params, Fetch::One)
    }

    fn fetch_all_msgpack<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>) -> Fut<'py> {
        self.query(py, sql, params, Fetch::AllMsgpack)
    }

    fn fetch_one_msgpack<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>) -> Fut<'py> {
        self.query(py, sql, params, Fetch::OneMsgpack)
    }

    /// Pinned mode only: start a transaction on the pinned connection.
    fn begin<'py>(&self, py: Python<'py>) -> Fut<'py> {
        if self.mode == Mode::Transaction && !self.closed() {
            return Err(PyRuntimeError::new_err(
                "begin() is only valid on a pinned connection from acquire(); \
                 this handle already is a transaction",
            ));
        }
        self.control(py, "BEGIN")
    }

    /// Transaction mode: commit, reset the session and return the connection
    /// to the pool (closing the handle). Pinned mode: stays pinned.
    fn commit<'py>(&self, py: Python<'py>) -> Fut<'py> {
        match self.mode {
            Mode::Pinned => self.control(py, "COMMIT"),
            Mode::Transaction => self.close_with(py, "COMMIT", true),
        }
    }

    /// As `commit()`, with ROLLBACK.
    fn rollback<'py>(&self, py: Python<'py>) -> Fut<'py> {
        match self.mode {
            Mode::Pinned => self.control(py, "ROLLBACK"),
            Mode::Transaction => self.close_with(py, "ROLLBACK", true),
        }
    }

    /// Roll back any open transaction, reset the session and return the
    /// connection to the pool. Idempotent; a connection that cannot be reset
    /// is discarded instead.
    fn release<'py>(&self, py: Python<'py>) -> Fut<'py> {
        self.close_with(py, "ROLLBACK", false)
    }
}

/// Connect to a Postgres database, returning a `PgEngine`.
///
/// `sslmode`/`sslrootcert` (the `ssl=` keyword of `Database`) override the
/// URL's values; see [`crate::tls`]. Other URL options are parsed by
/// tokio-postgres, which rejects unknown ones.
#[pyfunction]
#[pyo3(signature = (url, sslmode=None, sslrootcert=None, max_size=10, min_size=1))]
pub fn connect_pg(
    py: Python<'_>,
    url: String,
    sslmode: Option<String>,
    sslrootcert: Option<String>,
    max_size: usize,
    min_size: usize,
) -> PyResult<Bound<'_, PyAny>> {
    if max_size == 0 || min_size > max_size {
        return Err(PyValueError::new_err(
            "pool sizes must satisfy 0 <= min_size <= max_size and max_size > 0",
        ));
    }
    let (url, url_sslmode, url_sslrootcert) = split_ssl_params(&url);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let mut pg_config: tokio_postgres::Config = url.parse().map_err(err)?;
        let tls = pg_tls(sslmode.or(url_sslmode), sslrootcert.or(url_sslrootcert))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = match tls {
            None => {
                pg_config.ssl_mode(SslMode::Disable);
                Manager::from_config(pg_config, NoTls, manager_config)
            }
            Some((mode, connector)) => {
                pg_config.ssl_mode(mode);
                Manager::from_config(pg_config, connector, manager_config)
            }
        };
        let pool = Pool::builder(manager)
            .max_size(max_size)
            .build()
            .map_err(err)?;
        // Surface connection errors eagerly and pre-open min_size connections.
        let mut warm = Vec::with_capacity(min_size.max(1));
        for _ in 0..min_size.max(1) {
            warm.push(pool.get().await.map_err(err)?);
        }
        drop(warm);
        Python::attach(|py| Py::new(py, PgEngine { pool }).map(Py::into_any))
    })
}
