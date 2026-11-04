//! Native MySQL engine for aUniqORM.
//!
//! Uses mysql_async so async DB work runs off the Python event loop. The
//! Python driver rewrites the ORM's `%s` placeholders to `?`. Prepared
//! statements are cached per connection by mysql_async; rows decode straight
//! into Python objects (or a msgpack buffer).
//!
//! Session state: pooled single statements are not followed by a reset (that
//! would cost a round trip and drop the statement cache). Connections pinned
//! by [`MySqlEngine::begin`]/[`MySqlEngine::acquire`] are reset with
//! `COM_RESET_CONNECTION` (rolling back, and clearing user and session
//! variables, temporary tables and `GET_LOCK` locks) on a background task
//! after COMMIT/ROLLBACK has resolved; the connection stays checked out until
//! it is reset and the handle's statements are prepared again.
//!
//! TLS: `ssl_mode` (from `Database(..., ssl=...)`) is `disabled`,
//! `preferred`, `required` or `verify_identity`; mysql_async's own
//! `require_ssl`/`verify_ca`/`built_in_roots` URL parameters also work.

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
use crate::tls::system_roots;
use mysql_async::prelude::*;
use mysql_async::{Conn, DriverError, OptsBuilder, Pool, PoolConstraints, Row, SslOpts, Value};
use pyo3::exceptions::{PyOverflowError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyInt, PyList, PyString};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Mutex, Notify, oneshot};

type Arg<'py> = Bound<'py, PyAny>;
type Fut<'py> = PyResult<Bound<'py, PyAny>>;

fn err<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

fn ended() -> PyErr {
    PyRuntimeError::new_err("native MySQL transaction/connection handle already ended")
}

// ── Parameters and results ──────────────────────────────────────────────────

fn py_to_value(item: &Bound<'_, PyAny>) -> PyResult<Value> {
    if item.is_none() {
        Ok(Value::NULL)
    } else if let Ok(b) = item.cast_exact::<PyBool>() {
        Ok(Value::Int(i64::from(b.is_true())))
    } else if item.is_instance_of::<PyInt>() {
        // Never fall through to float: out-of-range ints must not round.
        item.extract::<i64>()
            .map(Value::Int)
            .or_else(|_| item.extract::<u64>().map(Value::UInt))
            .map_err(|_| {
                PyOverflowError::new_err(
                    "int bind parameter does not fit in a 64-bit integer; \
                     pass a decimal.Decimal for DECIMAL columns",
                )
            })
    } else if let Ok(v) = item.extract::<f64>() {
        Ok(Value::Double(v))
    } else if let Ok(v) = item.extract::<String>() {
        Ok(Value::Bytes(v.into_bytes()))
    } else {
        Err(PyRuntimeError::new_err("unsupported bind parameter type"))
    }
}

fn extract_params(params: &Bound<'_, PyAny>) -> PyResult<mysql_async::Params> {
    if params.is_none() {
        return Ok(mysql_async::Params::Empty);
    }
    let values = params
        .try_iter()?
        .map(|item| py_to_value(&item?))
        .collect::<PyResult<Vec<_>>>()?;
    Ok(if values.is_empty() {
        mysql_async::Params::Empty
    } else {
        mysql_async::Params::Positional(values)
    })
}

fn temporal_to_string(v: &Value) -> String {
    let frac = |us: u32| {
        if us == 0 {
            String::new()
        } else {
            format!(".{us:06}")
        }
    };
    match v {
        Value::Date(y, mo, d, h, mi, s, us) => {
            format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}{}", frac(*us))
        }
        Value::Time(neg, days, h, mi, s, us) => {
            let sign = if *neg { "-" } else { "" };
            let hours = days * 24 + u32::from(*h);
            format!("{sign}{hours:02}:{mi:02}:{s:02}{}", frac(*us))
        }
        _ => String::new(),
    }
}

fn value_py<'py>(py: Python<'py>, v: &Value) -> PyResult<Bound<'py, PyAny>> {
    Ok(match v {
        Value::NULL => py.None().into_bound(py),
        Value::Int(i) => i.into_pyobject(py)?.into_any(),
        Value::UInt(u) => u.into_pyobject(py)?.into_any(),
        Value::Float(f) => f64::from(*f).into_pyobject(py)?.into_any(),
        Value::Double(d) => d.into_pyobject(py)?.into_any(),
        Value::Bytes(b) => match std::str::from_utf8(b) {
            Ok(s) => PyString::new(py, s).into_any(),
            Err(_) => PyBytes::new(py, b).into_any(),
        },
        other => PyString::new(py, &temporal_to_string(other)).into_any(),
    })
}

fn row_dict<'py>(
    py: Python<'py>,
    row: &Row,
    keys: &[Bound<'py, PyString>],
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (i, key) in keys.iter().enumerate() {
        dict.set_item(key, value_py(py, row.as_ref(i).unwrap_or(&Value::NULL))?)?;
    }
    Ok(dict)
}

fn keys<'py>(py: Python<'py>, row: &Row) -> Vec<Bound<'py, PyString>> {
    row.columns_ref()
        .iter()
        .map(|c| PyString::intern(py, &c.name_str()))
        .collect()
}

fn write_row_mp(buf: &mut Vec<u8>, row: &Row) -> Result<(), String> {
    use rmp::encode as mp;
    fn e(err: impl std::fmt::Display) -> String {
        err.to_string()
    }
    mp::write_map_len(buf, row.columns_ref().len() as u32).map_err(e)?;
    for (i, col) in row.columns_ref().iter().enumerate() {
        mp::write_str(buf, &col.name_str()).map_err(e)?;
        match row.as_ref(i).unwrap_or(&Value::NULL) {
            Value::NULL => mp::write_nil(buf).map_err(e)?,
            Value::Int(i) => drop(mp::write_sint(buf, *i).map_err(e)?),
            Value::UInt(u) => drop(mp::write_uint(buf, *u).map_err(e)?),
            Value::Float(f) => mp::write_f64(buf, f64::from(*f)).map_err(e)?,
            Value::Double(d) => mp::write_f64(buf, *d).map_err(e)?,
            Value::Bytes(b) => match std::str::from_utf8(b) {
                Ok(s) => mp::write_str(buf, s).map_err(e)?,
                Err(_) => mp::write_bin(buf, b).map_err(e)?,
            },
            other => mp::write_str(buf, &temporal_to_string(other)).map_err(e)?,
        }
    }
    Ok(())
}

// ── Statement execution shared by the pool engine and pinned handles ────────

/// Transaction control (`BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT x`,
/// `RELEASE SAVEPOINT x`, `ROLLBACK TO [SAVEPOINT] x`) runs through the text
/// protocol: MySQL cannot prepare savepoints, and there is nothing worth
/// caching. The strict grammar keeps stacked statements impossible.
fn is_text_statement(sql: &str) -> bool {
    let words: Vec<String> = sql
        .split_ascii_whitespace()
        .map(|w| w.to_ascii_uppercase())
        .collect();
    let words: Vec<&str> = words.iter().map(String::as_str).collect();
    let name_ok = |name: &str| {
        name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    };
    match words.as_slice() {
        ["BEGIN" | "COMMIT" | "ROLLBACK"] => true,
        ["SAVEPOINT", name]
        | ["RELEASE", "SAVEPOINT", name]
        | ["ROLLBACK", "TO", name]
        | ["ROLLBACK", "TO", "SAVEPOINT", name] => name_ok(name),
        _ => false,
    }
}

/// What a query method returns to Python.
#[derive(Clone, Copy)]
enum Fetch {
    /// The last insert id.
    Execute,
    All,
    One,
    AllMsgpack,
    OneMsgpack,
}

enum Output {
    Id(i64),
    Rows(Vec<Row>),
    Row(Option<Row>),
}

/// Whether `sql` runs through the prepared-statement protocol.
fn is_prepared(sql: &str, params: &mysql_async::Params) -> bool {
    !(matches!(params, mysql_async::Params::Empty) && is_text_statement(sql))
}

async fn run(
    conn: &mut Conn,
    sql: &str,
    params: mysql_async::Params,
    fetch: Fetch,
) -> PyResult<Output> {
    Ok(match fetch {
        Fetch::Execute => {
            if is_prepared(sql, &params) {
                conn.exec_drop(sql, params).await.map_err(err)?;
            } else {
                conn.query_drop(sql).await.map_err(err)?;
            }
            Output::Id(conn.last_insert_id().unwrap_or(0) as i64)
        }
        Fetch::All | Fetch::AllMsgpack => Output::Rows(conn.exec(sql, params).await.map_err(err)?),
        Fetch::One | Fetch::OneMsgpack => {
            Output::Row(conn.exec_first(sql, params).await.map_err(err)?)
        }
    })
}

fn msgpack(rows: &[Row], single: bool) -> PyResult<Py<PyAny>> {
    let mut buf = Vec::new();
    if !single {
        rmp::encode::write_array_len(&mut buf, rows.len() as u32).map_err(err)?;
    }
    for row in rows {
        write_row_mp(&mut buf, row).map_err(PyRuntimeError::new_err)?;
    }
    Python::attach(|py| Ok(PyBytes::new(py, &buf).into_any().unbind()))
}

/// Convert a query's output for Python; msgpack is encoded off the GIL.
fn into_py(output: Output, fetch: Fetch) -> PyResult<Py<PyAny>> {
    match (output, fetch) {
        (Output::Rows(rows), Fetch::AllMsgpack) => msgpack(&rows, false),
        (Output::Row(Some(row)), Fetch::OneMsgpack) => msgpack(&[row], true),
        (output, _) => Python::attach(|py| {
            Ok(match output {
                Output::Id(id) => id.into_pyobject(py)?.into_any().unbind(),
                Output::Row(None) => py.None(),
                Output::Row(Some(row)) => row_dict(py, &row, &keys(py, &row))?.into_any().unbind(),
                Output::Rows(rows) => {
                    let list = PyList::empty(py);
                    if let Some(first) = rows.first() {
                        let keys = keys(py, first);
                        for row in &rows {
                            list.append(row_dict(py, row, &keys)?)?;
                        }
                    }
                    list.into_any().unbind()
                }
            })
        }),
    }
}

// ── Returning pinned connections to the pool ────────────────────────────────

/// Tracks background resets so `MySqlEngine.close()` can wait for them.
#[derive(Default)]
struct Background {
    running: AtomicUsize,
    idle: Notify,
}

impl Background {
    fn spawn<F: Future<Output = ()> + Send + 'static>(self: &Arc<Self>, task: F) {
        self.running.fetch_add(1, Ordering::AcqRel);
        let this = self.clone();
        pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
            task.await;
            if this.running.fetch_sub(1, Ordering::AcqRel) == 1 {
                this.idle.notify_waiters();
            }
        });
    }

    async fn wait_idle(&self) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.running.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// A pinned connection and the distinct prepared statements executed through
/// it (most recent last, bounded by the statement-cache size).
struct Held {
    conn: Conn,
    statements: Vec<String>,
}

impl Held {
    fn remember(&mut self, sql: &str) {
        if let Some(pos) = self.statements.iter().position(|s| s == sql) {
            let s = self.statements.remove(pos);
            self.statements.push(s);
            return;
        }
        let cap = self.conn.opts().stmt_cache_size();
        if cap > 0 {
            if self.statements.len() >= cap {
                self.statements.remove(0);
            }
            self.statements.push(sql.to_owned());
        }
    }
}

/// Reset `held` and return it to the pool, re-preparing its statements (the
/// reset deallocates them), or disconnect it if it cannot be reset or an
/// operation on it was cancelled midway.
async fn recycle(held: Held, poisoned: bool) {
    let Held {
        mut conn,
        statements,
    } = held;
    if poisoned || !matches!(conn.reset().await, Ok(true)) {
        let _ = conn.disconnect().await;
        return;
    }
    for sql in statements {
        match conn.prep(sql.as_str()).await {
            // A server error (e.g. a temporary table the reset dropped) is fine.
            Ok(_) | Err(mysql_async::Error::Server(_)) => {}
            Err(_) => {
                let _ = conn.disconnect().await;
                return;
            }
        }
    }
}

/// A pooled connection pinned to one handle. Dropping it while still held
/// resets it on a background task (no GIL needed) before it is pooled again.
struct Pinned {
    held: Mutex<Option<Held>>,
    closed: AtomicBool,
    /// Set when an operation's future was dropped midway (task cancelled).
    poisoned: AtomicBool,
    background: Arc<Background>,
}

impl Pinned {
    fn new(conn: Conn, background: Arc<Background>) -> Arc<Self> {
        Arc::new(Self {
            held: Mutex::new(Some(Held {
                conn,
                statements: Vec::new(),
            })),
            closed: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            background,
        })
    }

    /// Run `fetch` on the pinned connection. The connection is poisoned if
    /// the future is dropped before the statement completes.
    async fn run(&self, sql: &str, params: mysql_async::Params, fetch: Fetch) -> PyResult<Output> {
        let mut guard = self.held.lock().await;
        let held = guard.as_mut().ok_or_else(ended)?;
        if self.poisoned.load(Ordering::Acquire) {
            return Err(PyRuntimeError::new_err(
                "the pinned MySQL connection is unusable after a cancelled operation",
            ));
        }
        let prepared = is_prepared(sql, &params);
        let complete = Poison(Some(&self.poisoned));
        let output = run(&mut held.conn, sql, params, fetch).await;
        std::mem::forget(complete);
        if prepared && output.is_ok() {
            held.remember(sql);
        }
        output
    }
}

impl Drop for Pinned {
    fn drop(&mut self) {
        if let Some(held) = self.held.get_mut().take() {
            let poisoned = self.poisoned.load(Ordering::Acquire);
            self.background.spawn(recycle(held, poisoned));
        }
    }
}

/// Sets the flag when dropped; forgotten once the operation completes.
struct Poison<'a>(Option<&'a AtomicBool>);

impl Drop for Poison<'_> {
    fn drop(&mut self) {
        if let Some(flag) = self.0 {
            flag.store(true, Ordering::Release);
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

// ── Pooled engine ───────────────────────────────────────────────────────────

#[pyclass]
pub struct MySqlEngine {
    pool: Pool,
    background: Arc<Background>,
}

impl MySqlEngine {
    fn query<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>, fetch: Fetch) -> Fut<'py> {
        let pool = self.pool.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut conn = pool.get_conn().await.map_err(err)?;
            let output = run(&mut conn, &sql, params, fetch).await?;
            drop(conn);
            into_py(output, fetch)
        })
    }

    fn pin<'py>(&self, py: Python<'py>, mode: Mode) -> Fut<'py> {
        let pool = self.pool.clone();
        let background = self.background.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = Pinned::new(pool.get_conn().await.map_err(err)?, background);
            if mode == Mode::Transaction {
                inner
                    .run("BEGIN", mysql_async::Params::Empty, Fetch::Execute)
                    .await?;
            }
            Python::attach(|py| Py::new(py, MySqlTransaction { inner, mode }).map(Py::into_any))
        })
    }
}

#[pymethods]
impl MySqlEngine {
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

    /// Wait for background resets, then close the pool (which also waits for
    /// connections still checked out).
    fn close<'py>(&self, py: Python<'py>) -> Fut<'py> {
        let pool = self.pool.clone();
        let background = self.background.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            background.wait_idle().await;
            pool.disconnect().await.map_err(err)
        })
    }
}

// ── Pinned connection / transaction handle ──────────────────────────────────

/// A connection pinned by `MySqlEngine.begin()` (transaction mode) or
/// `MySqlEngine.acquire()` (pinned mode). Operations from several tasks are
/// serialised on the connection.
#[pyclass]
pub struct MySqlTransaction {
    inner: Arc<Pinned>,
    mode: Mode,
}

impl MySqlTransaction {
    fn query<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<Arg<'py>>,
        fetch: Fetch,
    ) -> Fut<'py> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(ended());
        }
        let params = match params {
            Some(params) => extract_params(&params)?,
            None => mysql_async::Params::Empty,
        };
        let (inner, sql) = (self.inner.clone(), sql.to_owned());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let output = inner.run(&sql, params, fetch).await?;
            into_py(output, fetch)
        })
    }

    /// Transaction mode: run `end` on a background task that owns the
    /// connection, resolve with its result, then reset and pool the
    /// connection on that task (cancelling the caller cannot interrupt it).
    fn end_transaction<'py>(&self, py: Python<'py>, end: &'static str) -> Fut<'py> {
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            return Err(ended());
        }
        let inner = self.inner.clone();
        let (tx, rx) = oneshot::channel::<PyResult<()>>();
        self.inner.background.spawn(async move {
            let Some(mut held) = inner.held.lock().await.take() else {
                let _ = tx.send(Ok(()));
                return;
            };
            let poisoned = inner.poisoned.load(Ordering::Acquire);
            let result = if poisoned {
                Err(PyRuntimeError::new_err(
                    "the connection was discarded after a cancelled operation; \
                     the transaction was rolled back",
                ))
            } else {
                held.conn.query_drop(end).await.map_err(err)
            };
            let _ = tx.send(result);
            recycle(held, poisoned).await;
        });
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            rx.await
                .map_err(|_| PyRuntimeError::new_err("transaction end was interrupted"))?
        })
    }
}

#[pymethods]
impl MySqlTransaction {
    /// True once the connection has been handed back for release.
    #[getter]
    fn closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    fn execute<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>) -> Fut<'py> {
        self.query(py, &sql, Some(params), Fetch::Execute)
    }

    fn fetch_all<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>) -> Fut<'py> {
        self.query(py, &sql, Some(params), Fetch::All)
    }

    fn fetch_one<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>) -> Fut<'py> {
        self.query(py, &sql, Some(params), Fetch::One)
    }

    fn fetch_all_msgpack<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>) -> Fut<'py> {
        self.query(py, &sql, Some(params), Fetch::AllMsgpack)
    }

    fn fetch_one_msgpack<'py>(&self, py: Python<'py>, sql: String, params: Arg<'py>) -> Fut<'py> {
        self.query(py, &sql, Some(params), Fetch::OneMsgpack)
    }

    /// Pinned mode only: start a transaction on the pinned connection.
    fn begin<'py>(&self, py: Python<'py>) -> Fut<'py> {
        if self.mode == Mode::Transaction && !self.closed() {
            return Err(PyRuntimeError::new_err(
                "begin() is only valid on a pinned connection from acquire(); \
                 this handle already is a transaction",
            ));
        }
        self.query(py, "BEGIN", None, Fetch::Execute)
    }

    /// Transaction mode: resolves once COMMIT completes; the reset and return
    /// to the pool follow in the background. Pinned mode: stays pinned.
    fn commit<'py>(&self, py: Python<'py>) -> Fut<'py> {
        match self.mode {
            Mode::Pinned => self.query(py, "COMMIT", None, Fetch::Execute),
            Mode::Transaction => self.end_transaction(py, "COMMIT"),
        }
    }

    /// As `commit()`, with ROLLBACK.
    fn rollback<'py>(&self, py: Python<'py>) -> Fut<'py> {
        match self.mode {
            Mode::Pinned => self.query(py, "ROLLBACK", None, Fetch::Execute),
            Mode::Transaction => self.end_transaction(py, "ROLLBACK"),
        }
    }

    /// Hand the connection back; the reset (which rolls back any open
    /// transaction) and the return to the pool run in the background.
    /// Idempotent.
    fn release<'py>(&self, py: Python<'py>) -> Fut<'py> {
        if !self.inner.closed.swap(true, Ordering::AcqRel) {
            let inner = self.inner.clone();
            self.inner.background.spawn(async move {
                if let Some(held) = inner.held.lock().await.take() {
                    recycle(held, inner.poisoned.load(Ordering::Acquire)).await;
                }
            });
        }
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(()) })
    }
}

// ── Connecting ──────────────────────────────────────────────────────────────

/// TLS options for an ssl mode (`None` for `disabled`).
fn ssl_opts(mode: &str) -> PyResult<Option<SslOpts>> {
    let opts = SslOpts::default();
    Ok(Some(match mode {
        "disabled" => return Ok(None),
        "preferred" | "required" => opts
            .with_danger_accept_invalid_certs(true)
            .with_danger_skip_domain_validation(true),
        // Trust the system store (like ssl.create_default_context()) as well
        // as mysql_async's bundled Mozilla roots.
        "verify_identity" => opts.with_root_certs(
            system_roots()
                .unwrap_or_default()
                .into_iter()
                .map(|c| c.as_ref().to_vec().into())
                .collect(),
        ),
        // mysql_async skips only the host-name check when rustls reports a
        // legacy error variant current rustls never produces, so VERIFY_CA
        // would silently behave as VERIFY_IDENTITY.
        "verify_ca" => {
            return Err(PyValueError::new_err(
                "ssl mode VERIFY_CA is not supported by the native MySQL driver; use \
                 verify_identity, required, or AUNIQORM_MYSQL_NATIVE=0 with an \
                 ssl.SSLContext",
            ));
        }
        other => {
            return Err(PyValueError::new_err(format!(
                "invalid MySQL ssl mode {other:?}: expected disabled, preferred, \
                 required or verify_identity"
            )));
        }
    }))
}

/// Connect to a MySQL database, returning a `MySqlEngine`.
///
/// `ssl_mode` overrides the URL's TLS settings; `max_size`/`min_size` bound
/// the pool.
#[pyfunction]
#[pyo3(signature = (url, ssl_mode=None, max_size=None, min_size=None))]
pub fn connect_mysql(
    py: Python<'_>,
    url: String,
    ssl_mode: Option<String>,
    max_size: Option<usize>,
    min_size: Option<usize>,
) -> PyResult<Bound<'_, PyAny>> {
    let tls = ssl_mode.as_deref().map(ssl_opts).transpose()?;
    let preferred = ssl_mode.as_deref() == Some("preferred");
    let constraints = match (max_size, min_size) {
        (None, None) => None,
        (max, min) => {
            let max = max.unwrap_or(10);
            Some(
                PoolConstraints::new(min.unwrap_or(1).min(max), max)
                    .ok_or_else(|| PyValueError::new_err("invalid pool sizes"))?,
            )
        }
    };
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let opts = mysql_async::Opts::from_url(&url).map_err(err)?;
        // Skip the per-checkout COM_RESET_CONNECTION round trip: only pinned
        // connections carry session state, and they are reset on release.
        let mut pool_opts = opts.pool_opts().clone().with_reset_connection(false);
        if let Some(constraints) = constraints {
            pool_opts = pool_opts.with_constraints(constraints);
        }
        let mut builder = OptsBuilder::from_opts(opts).pool_opts(pool_opts);
        if let Some(tls) = tls {
            builder = builder.ssl_opts(tls);
        }
        let mut pool = Pool::new(builder.clone());
        // Surface connection errors eagerly. PREFERRED falls back to
        // plaintext only when the server does not offer TLS.
        match pool.get_conn().await {
            Ok(_) => {}
            Err(mysql_async::Error::Driver(DriverError::NoClientSslFlagFromServer))
                if preferred =>
            {
                let _ = pool.disconnect().await;
                pool = Pool::new(builder.ssl_opts(None));
                drop(pool.get_conn().await.map_err(err)?);
            }
            Err(e) => {
                let _ = pool.disconnect().await;
                return Err(err(e));
            }
        }
        let engine = MySqlEngine {
            pool,
            background: Arc::default(),
        };
        Python::attach(|py| Py::new(py, engine).map(Py::into_any))
    })
}

#[cfg(test)]
mod tests {
    use super::is_text_statement;

    #[test]
    fn text_statement_grammar_is_strict() {
        for ok in [
            "BEGIN",
            "commit",
            "SAVEPOINT a_1",
            "release savepoint sp",
            "ROLLBACK TO SAVEPOINT _x9",
            "ROLLBACK TO sp",
        ] {
            assert!(is_text_statement(ok), "{ok}");
        }
        for bad in [
            "SAVEPOINT a; DROP TABLE t",
            "SAVEPOINT a;",
            "SAVEPOINT `a`",
            "SAVEPOINT 1a",
            "BEGIN WORK; X",
            "SELECT 1",
            "",
        ] {
            assert!(!is_text_statement(bad), "{bad}");
        }
    }
}
