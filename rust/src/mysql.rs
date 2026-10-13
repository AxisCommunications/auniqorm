//! Native MySQL engine for aUniqORM.
//!
//! Uses mysql_async (the stack Yara ships) so async DB work runs off the Python
//! event loop. The ORM emits `%s` placeholders for the MySQL dialect; the Python
//! driver rewrites them to mysql_async's `?` before handing SQL here. Prepared
//! statements are cached per connection by mysql_async, and rows are decoded
//! straight into Python objects (or a msgpack buffer) under a single GIL pass.

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
use mysql_async::prelude::*;
use mysql_async::{Conn, Pool, Row, Value};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString};
use std::sync::Arc;
use tokio::sync::Mutex;

fn err<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

// ── Parameter binding ───────────────────────────────────────────────────────

fn py_to_value(item: &Bound<'_, PyAny>) -> PyResult<Value> {
    if item.is_none() {
        Ok(Value::NULL)
    } else if let Ok(v) = item.extract::<i64>() {
        // bool is an int subclass in Python, so True/False land here as 1/0.
        Ok(Value::Int(v))
    } else if let Ok(v) = item.extract::<f64>() {
        Ok(Value::Double(v))
    } else if let Ok(v) = item.extract::<String>() {
        Ok(Value::Bytes(v.into_bytes()))
    } else {
        Err(PyRuntimeError::new_err("unsupported bind parameter type"))
    }
}

fn extract_params(params: &Bound<'_, PyAny>) -> PyResult<Vec<Value>> {
    if params.is_none() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for item in params.try_iter()? {
        out.push(py_to_value(&item?)?);
    }
    Ok(out)
}

fn params_of(values: Vec<Value>) -> mysql_async::Params {
    if values.is_empty() {
        mysql_async::Params::Empty
    } else {
        mysql_async::Params::Positional(values)
    }
}

// ── Value decoding ──────────────────────────────────────────────────────────

fn temporal_to_string(v: &Value) -> String {
    match v {
        Value::Date(y, mo, d, h, mi, s, us) => {
            if *us == 0 {
                format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}")
            } else {
                format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}")
            }
        }
        Value::Time(neg, days, h, mi, s, us) => {
            let sign = if *neg { "-" } else { "" };
            let hours = days * 24 + u32::from(*h);
            if *us == 0 {
                format!("{sign}{hours:02}:{mi:02}:{s:02}")
            } else {
                format!("{sign}{hours:02}:{mi:02}:{s:02}.{us:06}")
            }
        }
        _ => String::new(),
    }
}

fn set_value(dict: &Bound<'_, PyDict>, key: &Bound<'_, PyString>, v: &Value) -> PyResult<()> {
    match v {
        Value::NULL => dict.set_item(key, dict.py().None()),
        Value::Int(i) => dict.set_item(key, *i),
        Value::UInt(u) => dict.set_item(key, *u as i64),
        Value::Float(f) => dict.set_item(key, f64::from(*f)),
        Value::Double(d) => dict.set_item(key, *d),
        Value::Bytes(b) => match std::str::from_utf8(b) {
            Ok(s) => dict.set_item(key, s),
            Err(_) => dict.set_item(key, PyBytes::new(dict.py(), b)),
        },
        other => dict.set_item(key, temporal_to_string(other)),
    }
}

fn row_to_pydict<'py>(py: Python<'py>, row: &Row) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (i, col) in row.columns_ref().iter().enumerate() {
        let key = PyString::intern(py, &col.name_str());
        set_value(&dict, &key, row.as_ref(i).unwrap_or(&Value::NULL))?;
    }
    Ok(dict)
}

/// Decode a result set, interning the column-name keys once so they are shared
/// across every row.
fn rows_to_pylist(py: Python<'_>, rows: &[Row]) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    if let Some(first) = rows.first() {
        let plan: Vec<Py<PyString>> = first
            .columns_ref()
            .iter()
            .map(|c| PyString::intern(py, &c.name_str()).unbind())
            .collect();
        for row in rows {
            let dict = PyDict::new(py);
            for (i, key) in plan.iter().enumerate() {
                set_value(&dict, key.bind(py), row.as_ref(i).unwrap_or(&Value::NULL))?;
            }
            list.append(dict)?;
        }
    }
    Ok(list.into_any().unbind())
}

// ── msgpack encoding (off-GIL): array of {column: value} maps ───────────────

fn write_value_mp(buf: &mut Vec<u8>, v: &Value) -> Result<(), String> {
    match v {
        Value::NULL => rmp::encode::write_nil(buf).map_err(|e| e.to_string()),
        Value::Int(i) => rmp::encode::write_sint(buf, *i)
            .map(|_| ())
            .map_err(|e| e.to_string()),
        Value::UInt(u) => rmp::encode::write_uint(buf, *u)
            .map(|_| ())
            .map_err(|e| e.to_string()),
        Value::Float(f) => rmp::encode::write_f64(buf, f64::from(*f)).map_err(|e| e.to_string()),
        Value::Double(d) => rmp::encode::write_f64(buf, *d).map_err(|e| e.to_string()),
        Value::Bytes(b) => match std::str::from_utf8(b) {
            Ok(s) => rmp::encode::write_str(buf, s).map_err(|e| e.to_string()),
            Err(_) => rmp::encode::write_bin(buf, b).map_err(|e| e.to_string()),
        },
        other => rmp::encode::write_str(buf, &temporal_to_string(other)).map_err(|e| e.to_string()),
    }
}

fn encode_msgpack(rows: &[Row]) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    rmp::encode::write_array_len(&mut buf, rows.len() as u32).map_err(|e| e.to_string())?;
    let Some(first) = rows.first() else {
        return Ok(buf);
    };
    let names: Vec<String> = first
        .columns_ref()
        .iter()
        .map(|c| c.name_str().into_owned())
        .collect();
    for row in rows {
        rmp::encode::write_map_len(&mut buf, names.len() as u32).map_err(|e| e.to_string())?;
        for (i, name) in names.iter().enumerate() {
            rmp::encode::write_str(&mut buf, name).map_err(|e| e.to_string())?;
            write_value_mp(&mut buf, row.as_ref(i).unwrap_or(&Value::NULL))?;
        }
    }
    Ok(buf)
}

fn encode_msgpack_row(row: &Row) -> Result<Vec<u8>, String> {
    let cols = row.columns_ref();
    let mut buf = Vec::new();
    rmp::encode::write_map_len(&mut buf, cols.len() as u32).map_err(|e| e.to_string())?;
    for (i, col) in cols.iter().enumerate() {
        rmp::encode::write_str(&mut buf, &col.name_str()).map_err(|e| e.to_string())?;
        write_value_mp(&mut buf, row.as_ref(i).unwrap_or(&Value::NULL))?;
    }
    Ok(buf)
}

// ── Query helpers shared by the pool engine and pinned transactions ─────────

async fn q_execute(conn: &mut Conn, sql: &str, params: Vec<Value>) -> Result<i64, PyErr> {
    conn.exec_drop(sql, params_of(params)).await.map_err(err)?;
    Ok(conn.last_insert_id().unwrap_or(0) as i64)
}

async fn q_query(conn: &mut Conn, sql: &str, params: Vec<Value>) -> Result<Vec<Row>, PyErr> {
    conn.exec(sql, params_of(params)).await.map_err(err)
}

async fn q_query_opt(conn: &mut Conn, sql: &str, params: Vec<Value>) -> Result<Option<Row>, PyErr> {
    conn.exec_first(sql, params_of(params)).await.map_err(err)
}

// ── Pooled engine ───────────────────────────────────────────────────────────

#[pyclass]
pub struct MySqlEngine {
    pool: Pool,
}

#[pymethods]
impl MySqlEngine {
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut conn = pool.get_conn().await.map_err(err)?;
            q_execute(&mut conn, &sql, params).await
        })
    }

    fn fetch_all<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut conn = pool.get_conn().await.map_err(err)?;
            let rows = q_query(&mut conn, &sql, params).await?;
            Python::attach(|py| rows_to_pylist(py, &rows))
        })
    }

    fn fetch_one<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut conn = pool.get_conn().await.map_err(err)?;
            let row = q_query_opt(&mut conn, &sql, params).await?;
            Python::attach(|py| match row {
                Some(ref r) => Ok(row_to_pydict(py, r)?.into_any().unbind()),
                None => Ok(py.None()),
            })
        })
    }

    fn fetch_all_msgpack<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut conn = pool.get_conn().await.map_err(err)?;
            let rows = q_query(&mut conn, &sql, params).await?;
            let buf = encode_msgpack(&rows).map_err(PyRuntimeError::new_err)?;
            Python::attach(|py| Ok(PyBytes::new(py, &buf).unbind().into_any()))
        })
    }

    fn fetch_one_msgpack<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut conn = pool.get_conn().await.map_err(err)?;
            let row = q_query_opt(&mut conn, &sql, params).await?;
            Python::attach(|py| match row {
                Some(ref r) => {
                    let buf = encode_msgpack_row(r).map_err(PyRuntimeError::new_err)?;
                    Ok(PyBytes::new(py, &buf).unbind().into_any())
                }
                None => Ok(py.None()),
            })
        })
    }

    /// Begin a transaction on a pinned connection, returning a `MySqlTransaction`.
    fn begin<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut conn = pool.get_conn().await.map_err(err)?;
            conn.query_drop("BEGIN").await.map_err(err)?;
            Python::attach(|py| {
                Py::new(
                    py,
                    MySqlTransaction {
                        conn: Arc::new(Mutex::new(conn)),
                    },
                )
                .map(|t| t.into_any())
            })
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            pool.disconnect().await.map_err(err)?;
            Ok(())
        })
    }
}

// ── Pinned transaction ──────────────────────────────────────────────────────

/// A transaction pinned to one connection (BEGIN issued; COMMIT/ROLLBACK close).
#[pyclass]
pub struct MySqlTransaction {
    conn: Arc<Mutex<Conn>>,
}

#[pymethods]
impl MySqlTransaction {
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.conn.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = conn.lock().await;
            q_execute(&mut guard, &sql, params).await
        })
    }

    fn fetch_all<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.conn.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = conn.lock().await;
            let rows = q_query(&mut guard, &sql, params).await?;
            Python::attach(|py| rows_to_pylist(py, &rows))
        })
    }

    fn fetch_one<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.conn.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = conn.lock().await;
            let row = q_query_opt(&mut guard, &sql, params).await?;
            Python::attach(|py| match row {
                Some(ref r) => Ok(row_to_pydict(py, r)?.into_any().unbind()),
                None => Ok(py.None()),
            })
        })
    }

    fn fetch_all_msgpack<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.conn.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = conn.lock().await;
            let rows = q_query(&mut guard, &sql, params).await?;
            let buf = encode_msgpack(&rows).map_err(PyRuntimeError::new_err)?;
            Python::attach(|py| Ok(PyBytes::new(py, &buf).unbind().into_any()))
        })
    }

    fn fetch_one_msgpack<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.conn.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = conn.lock().await;
            let row = q_query_opt(&mut guard, &sql, params).await?;
            Python::attach(|py| match row {
                Some(ref r) => {
                    let buf = encode_msgpack_row(r).map_err(PyRuntimeError::new_err)?;
                    Ok(PyBytes::new(py, &buf).unbind().into_any())
                }
                None => Ok(py.None()),
            })
        })
    }

    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.conn.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = conn.lock().await;
            guard.query_drop("COMMIT").await.map_err(err)?;
            Ok(())
        })
    }

    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn = self.conn.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = conn.lock().await;
            guard.query_drop("ROLLBACK").await.map_err(err)?;
            Ok(())
        })
    }
}

/// Connect to a MySQL database, returning a `MySqlEngine`.
#[pyfunction]
pub fn connect_mysql(py: Python<'_>, url: String) -> PyResult<Bound<'_, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let opts = mysql_async::Opts::from_url(&url).map_err(err)?;
        // Skip the per-checkout COM_RESET_CONNECTION round-trip: we manage
        // transaction state explicitly, so resetting only adds latency.
        let pool_opts = opts.pool_opts().clone().with_reset_connection(false);
        let opts = mysql_async::OptsBuilder::from_opts(opts).pool_opts(pool_opts);
        let pool = Pool::new(opts);
        // Surface connection errors eagerly.
        let conn = pool.get_conn().await.map_err(err)?;
        drop(conn);
        Python::attach(|py| Py::new(py, MySqlEngine { pool }).map(|engine| engine.into_any()))
    })
}
