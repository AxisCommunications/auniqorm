//! Native SQLite engine for aUniqORM.
//!
//! aiosqlite offloads every statement to a background thread and hops the result
//! back to the event loop; that per-query thread hop is its dominant cost. Here
//! we run SQLite synchronously with the GIL released (SQLite is in-process and
//! fast), returning results directly — the Python driver's async methods simply
//! return the value, so `await` resolves without a thread hop. A single
//! connection behind a mutex gives natural transaction pinning.

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString};
use rusqlite::Connection;
use rusqlite::types::{Value, ValueRef};
use std::sync::{Arc, Mutex};

fn err<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

// ── Parameter binding ───────────────────────────────────────────────────────

fn extract_params(params: &Bound<'_, PyAny>) -> PyResult<Vec<Value>> {
    if params.is_none() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for item in params.try_iter()? {
        let item = item?;
        if item.is_none() {
            out.push(Value::Null);
        } else if let Ok(v) = item.extract::<i64>() {
            // bool is an int subclass in Python, so True/False land here as 1/0.
            out.push(Value::Integer(v));
        } else if let Ok(v) = item.extract::<f64>() {
            out.push(Value::Real(v));
        } else if let Ok(v) = item.extract::<String>() {
            out.push(Value::Text(v));
        } else if let Ok(v) = item.extract::<Vec<u8>>() {
            out.push(Value::Blob(v));
        } else {
            return Err(PyRuntimeError::new_err("unsupported bind parameter type"));
        }
    }
    Ok(out)
}

// ── Result decoding ─────────────────────────────────────────────────────────

enum Cell {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

struct ResultSet {
    columns: Vec<String>,
    rows: Vec<Vec<Cell>>,
}

/// Run a query and collect the whole result set. Off-GIL (no Python touched).
fn run_query(conn: &Connection, sql: &str, params: &[Value]) -> Result<ResultSet, PyErr> {
    let mut stmt = conn.prepare_cached(sql).map_err(err)?;
    let columns: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let ncol = columns.len();
    let mut out = Vec::new();
    let mut rows = stmt
        .query(rusqlite::params_from_iter(params.iter()))
        .map_err(err)?;
    while let Some(row) = rows.next().map_err(err)? {
        let mut cells = Vec::with_capacity(ncol);
        for i in 0..ncol {
            cells.push(match row.get_ref(i).map_err(err)? {
                ValueRef::Null => Cell::Null,
                ValueRef::Integer(x) => Cell::Int(x),
                ValueRef::Real(x) => Cell::Real(x),
                ValueRef::Text(b) => Cell::Text(String::from_utf8_lossy(b).into_owned()),
                ValueRef::Blob(b) => Cell::Blob(b.to_vec()),
            });
        }
        out.push(cells);
    }
    Ok(ResultSet { columns, rows: out })
}

fn set_cell(dict: &Bound<'_, PyDict>, key: &Bound<'_, PyString>, cell: &Cell) -> PyResult<()> {
    match cell {
        Cell::Null => dict.set_item(key, dict.py().None()),
        Cell::Int(i) => dict.set_item(key, *i),
        Cell::Real(f) => dict.set_item(key, *f),
        Cell::Text(s) => dict.set_item(key, s),
        Cell::Blob(b) => dict.set_item(key, PyBytes::new(dict.py(), b)),
    }
}

fn resultset_to_pylist(py: Python<'_>, rs: &ResultSet) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    let keys: Vec<Py<PyString>> = rs
        .columns
        .iter()
        .map(|c| PyString::intern(py, c).unbind())
        .collect();
    for row in &rs.rows {
        let dict = PyDict::new(py);
        for (key, cell) in keys.iter().zip(row) {
            set_cell(&dict, key.bind(py), cell)?;
        }
        list.append(dict)?;
    }
    Ok(list.into_any().unbind())
}

fn row_to_pydict<'py>(
    py: Python<'py>,
    rs: &ResultSet,
    cells: &[Cell],
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (name, cell) in rs.columns.iter().zip(cells) {
        let key = PyString::intern(py, name);
        set_cell(&dict, &key, cell)?;
    }
    Ok(dict)
}

// ── msgpack encoding (off-GIL): array of {column: value} maps ───────────────

fn write_cell_mp(buf: &mut Vec<u8>, cell: &Cell) -> Result<(), String> {
    match cell {
        Cell::Null => rmp::encode::write_nil(buf).map_err(|e| e.to_string()),
        Cell::Int(i) => rmp::encode::write_sint(buf, *i)
            .map(|_| ())
            .map_err(|e| e.to_string()),
        Cell::Real(f) => rmp::encode::write_f64(buf, *f).map_err(|e| e.to_string()),
        Cell::Text(s) => rmp::encode::write_str(buf, s).map_err(|e| e.to_string()),
        Cell::Blob(b) => rmp::encode::write_bin(buf, b).map_err(|e| e.to_string()),
    }
}

fn encode_msgpack(rs: &ResultSet) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    rmp::encode::write_array_len(&mut buf, rs.rows.len() as u32).map_err(|e| e.to_string())?;
    let ncol = rs.columns.len();
    for row in &rs.rows {
        rmp::encode::write_map_len(&mut buf, ncol as u32).map_err(|e| e.to_string())?;
        for (name, cell) in rs.columns.iter().zip(row) {
            rmp::encode::write_str(&mut buf, name).map_err(|e| e.to_string())?;
            write_cell_mp(&mut buf, cell)?;
        }
    }
    Ok(buf)
}

fn encode_msgpack_row(columns: &[String], cells: &[Cell]) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    rmp::encode::write_map_len(&mut buf, columns.len() as u32).map_err(|e| e.to_string())?;
    for (name, cell) in columns.iter().zip(cells) {
        rmp::encode::write_str(&mut buf, name).map_err(|e| e.to_string())?;
        write_cell_mp(&mut buf, cell)?;
    }
    Ok(buf)
}

// ── Engine ──────────────────────────────────────────────────────────────────

#[pyclass]
pub struct SqliteEngine {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteEngine {
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, PyErr> {
        self.conn
            .lock()
            .map_err(|_| err("sqlite connection poisoned"))
    }
}

#[pymethods]
impl SqliteEngine {
    /// Execute a statement, returning the last inserted rowid.
    fn execute(&self, py: Python<'_>, sql: String, params: Bound<'_, PyAny>) -> PyResult<i64> {
        let values = extract_params(&params)?;
        let conn = self.conn.clone();
        py.detach(move || {
            let guard = conn.lock().map_err(|_| err("sqlite connection poisoned"))?;
            let mut stmt = guard.prepare_cached(&sql).map_err(err)?;
            stmt.execute(rusqlite::params_from_iter(values.iter()))
                .map_err(err)?;
            drop(stmt);
            Ok(guard.last_insert_rowid())
        })
    }

    fn fetch_all(
        &self,
        py: Python<'_>,
        sql: String,
        params: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let values = extract_params(&params)?;
        let conn = self.conn.clone();
        let rs = py.detach(move || {
            let guard = conn.lock().map_err(|_| err("sqlite connection poisoned"))?;
            run_query(&guard, &sql, &values)
        })?;
        resultset_to_pylist(py, &rs)
    }

    fn fetch_one(
        &self,
        py: Python<'_>,
        sql: String,
        params: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let values = extract_params(&params)?;
        let conn = self.conn.clone();
        let rs = py.detach(move || {
            let guard = conn.lock().map_err(|_| err("sqlite connection poisoned"))?;
            run_query(&guard, &sql, &values)
        })?;
        match rs.rows.first() {
            Some(cells) => Ok(row_to_pydict(py, &rs, cells)?.into_any().unbind()),
            None => Ok(py.None()),
        }
    }

    fn fetch_all_msgpack(
        &self,
        py: Python<'_>,
        sql: String,
        params: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let values = extract_params(&params)?;
        let conn = self.conn.clone();
        let buf = py.detach(move || {
            let guard = conn.lock().map_err(|_| err("sqlite connection poisoned"))?;
            let rs = run_query(&guard, &sql, &values)?;
            encode_msgpack(&rs).map_err(PyRuntimeError::new_err)
        })?;
        Ok(PyBytes::new(py, &buf).into_any().unbind())
    }

    fn fetch_one_msgpack(
        &self,
        py: Python<'_>,
        sql: String,
        params: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let values = extract_params(&params)?;
        let conn = self.conn.clone();
        let buf = py.detach(move || {
            let guard = conn.lock().map_err(|_| err("sqlite connection poisoned"))?;
            let rs = run_query(&guard, &sql, &values)?;
            match rs.rows.first() {
                Some(cells) => encode_msgpack_row(&rs.columns, cells)
                    .map(Some)
                    .map_err(PyRuntimeError::new_err),
                None => Ok(None),
            }
        })?;
        match buf {
            Some(b) => Ok(PyBytes::new(py, &b).into_any().unbind()),
            None => Ok(py.None()),
        }
    }

    fn begin(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.lock()?.execute_batch("BEGIN").map_err(err))
    }

    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.lock()?.execute_batch("COMMIT").map_err(err))
    }

    fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.lock()?.execute_batch("ROLLBACK").map_err(err))
    }

    fn close(&self) -> PyResult<()> {
        // The connection closes when the last reference is dropped.
        Ok(())
    }
}

/// Open a SQLite database at *path* (or ``:memory:``), returning a `SqliteEngine`.
#[pyfunction]
pub fn connect_sqlite(path: String) -> PyResult<SqliteEngine> {
    let conn = Connection::open(&path).map_err(err)?;
    // WAL + NORMAL: writers don't block readers and commits skip the per-txn
    // fsync (durable at checkpoints; safe against corruption, only risks the
    // last commits on OS/power loss) — the standard high-throughput pairing.
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL")
        .map_err(err)?;
    Ok(SqliteEngine {
        conn: Arc::new(Mutex::new(conn)),
    })
}
