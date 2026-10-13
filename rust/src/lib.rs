//! Native Postgres engine for aUniqORM (prototype).
//!
//! Uses tokio-postgres + deadpool (the stack Yara ships) so the async DB work
//! runs off the Python event loop in Rust. Prepared statements are cached per
//! connection, and result rows are decoded straight into Python objects under a
//! single GIL pass. Single-backend prototype to measure headroom before a full
//! engine.

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
use bytes::BytesMut;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use postgres_types::to_sql_checked;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString};
use std::error::Error as StdError;
use std::sync::Arc;
use tokio_postgres::types::{IsNull, ToSql, Type};
use tokio_postgres::{NoTls, Row};

mod mysql;
mod sqlite;

fn err<E: StdError>(e: E) -> PyErr {
    let mut msg = e.to_string();
    let mut source = e.source();
    while let Some(cause) = source {
        msg.push_str(": ");
        msg.push_str(&cause.to_string());
        source = cause.source();
    }
    PyRuntimeError::new_err(msg)
}

/// A bind parameter extracted from Python before crossing into async Rust.
#[derive(Debug)]
enum Param {
    Int(i64),
    Float(f64),
    Text(String),
    Null,
    IntArray(Vec<i64>),
    FloatArray(Vec<f64>),
    TextArray(Vec<String>),
}

fn extract_array(seq: &Bound<'_, PyAny>) -> PyResult<Param> {
    let items: Vec<Bound<'_, PyAny>> = seq.try_iter()?.collect::<PyResult<_>>()?;
    let Some(first) = items.first() else {
        return Ok(Param::IntArray(Vec::new()));
    };
    if first.extract::<i64>().is_ok() {
        let mut v = Vec::with_capacity(items.len());
        for it in &items {
            v.push(it.extract::<i64>()?);
        }
        Ok(Param::IntArray(v))
    } else if first.extract::<f64>().is_ok() {
        let mut v = Vec::with_capacity(items.len());
        for it in &items {
            v.push(it.extract::<f64>()?);
        }
        Ok(Param::FloatArray(v))
    } else {
        let mut v = Vec::with_capacity(items.len());
        for it in &items {
            v.push(it.extract::<String>()?);
        }
        Ok(Param::TextArray(v))
    }
}

fn extract_params(params: &Bound<'_, PyAny>) -> PyResult<Vec<Param>> {
    if params.is_none() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for item in params.try_iter()? {
        let item = item?;
        if item.is_none() {
            out.push(Param::Null);
        } else if let Ok(v) = item.extract::<i64>() {
            out.push(Param::Int(v));
        } else if let Ok(v) = item.extract::<f64>() {
            out.push(Param::Float(v));
        } else if let Ok(v) = item.extract::<String>() {
            out.push(Param::Text(v));
        } else if item.is_instance_of::<pyo3::types::PyList>()
            || item.is_instance_of::<pyo3::types::PyTuple>()
        {
            out.push(extract_array(&item)?);
        } else {
            return Err(PyRuntimeError::new_err("unsupported bind parameter type"));
        }
    }
    Ok(out)
}

impl ToSql for Param {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn StdError + Sync + Send>> {
        match self {
            // Bind integers/floats at the column's actual width; the prepared
            // statement infers the param type (e.g. `id = $1` -> int4).
            Param::Int(v) => {
                if *ty == Type::INT2 {
                    (*v as i16).to_sql(ty, out)
                } else if *ty == Type::INT4 {
                    (*v as i32).to_sql(ty, out)
                } else if *ty == Type::FLOAT4 {
                    (*v as f32).to_sql(ty, out)
                } else if *ty == Type::FLOAT8 {
                    (*v as f64).to_sql(ty, out)
                } else {
                    v.to_sql(ty, out)
                }
            }
            Param::Float(v) => {
                if *ty == Type::FLOAT4 {
                    (*v as f32).to_sql(ty, out)
                } else {
                    v.to_sql(ty, out)
                }
            }
            Param::Text(v) => v.to_sql(ty, out),
            Param::Null => Ok(IsNull::Yes),
            // Arrays for unnest bulk-insert; match the element width to the cast.
            Param::IntArray(v) => {
                if *ty == Type::INT4_ARRAY {
                    let a: Vec<i32> = v.iter().map(|x| *x as i32).collect();
                    a.to_sql(ty, out)
                } else {
                    v.to_sql(ty, out)
                }
            }
            Param::FloatArray(v) => {
                if *ty == Type::FLOAT4_ARRAY {
                    let a: Vec<f32> = v.iter().map(|x| *x as f32).collect();
                    a.to_sql(ty, out)
                } else {
                    v.to_sql(ty, out)
                }
            }
            Param::TextArray(v) => v.to_sql(ty, out),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

/// How to decode a Postgres column, resolved once per result set.
#[derive(Clone, Copy)]
enum ColKind {
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Bool,
    Text,
    Other,
}

fn kind_of(ty: &Type) -> ColKind {
    if *ty == Type::INT2 {
        ColKind::Int2
    } else if *ty == Type::INT4 {
        ColKind::Int4
    } else if *ty == Type::INT8 {
        ColKind::Int8
    } else if *ty == Type::FLOAT4 {
        ColKind::Float4
    } else if *ty == Type::FLOAT8 {
        ColKind::Float8
    } else if *ty == Type::BOOL {
        ColKind::Bool
    } else if *ty == Type::TEXT || *ty == Type::VARCHAR || *ty == Type::BPCHAR || *ty == Type::NAME
    {
        ColKind::Text
    } else {
        ColKind::Other
    }
}

fn set_opt<'py, T>(
    dict: &Bound<'py, PyDict>,
    key: &Bound<'py, PyString>,
    value: Option<T>,
) -> PyResult<()>
where
    T: IntoPyObject<'py>,
{
    match value {
        Some(v) => dict.set_item(key, v),
        None => dict.set_item(key, dict.py().None()),
    }
}

fn set_cell(
    dict: &Bound<'_, PyDict>,
    key: &Bound<'_, PyString>,
    row: &Row,
    i: usize,
    kind: ColKind,
) -> PyResult<()> {
    match kind {
        ColKind::Int2 => set_opt(
            dict,
            key,
            row.try_get::<usize, Option<i16>>(i)
                .map_err(err)?
                .map(i64::from),
        ),
        ColKind::Int4 => set_opt(
            dict,
            key,
            row.try_get::<usize, Option<i32>>(i)
                .map_err(err)?
                .map(i64::from),
        ),
        ColKind::Int8 => set_opt(
            dict,
            key,
            row.try_get::<usize, Option<i64>>(i).map_err(err)?,
        ),
        ColKind::Float4 => set_opt(
            dict,
            key,
            row.try_get::<usize, Option<f32>>(i)
                .map_err(err)?
                .map(f64::from),
        ),
        ColKind::Float8 => set_opt(
            dict,
            key,
            row.try_get::<usize, Option<f64>>(i).map_err(err)?,
        ),
        ColKind::Bool => set_opt(
            dict,
            key,
            row.try_get::<usize, Option<bool>>(i).map_err(err)?,
        ),
        ColKind::Text => set_opt(
            dict,
            key,
            row.try_get::<usize, Option<String>>(i).map_err(err)?,
        ),
        ColKind::Other => set_opt(
            dict,
            key,
            row.try_get::<usize, Option<String>>(i).ok().flatten(),
        ),
    }
}

/// Decode a result set, resolving the per-column plan once and interning the
/// column-name keys so they are shared across every row.
fn rows_to_pylist(py: Python<'_>, rows: &[Row]) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    if let Some(first) = rows.first() {
        let plan: Vec<(Py<PyString>, ColKind)> = first
            .columns()
            .iter()
            .map(|col| {
                (
                    PyString::intern(py, col.name()).unbind(),
                    kind_of(col.type_()),
                )
            })
            .collect();
        for row in rows {
            let dict = PyDict::new(py);
            for (i, (key, kind)) in plan.iter().enumerate() {
                set_cell(&dict, key.bind(py), row, i, *kind)?;
            }
            list.append(dict)?;
        }
    }
    Ok(list.into_any().unbind())
}

fn row_to_pydict<'py>(py: Python<'py>, row: &Row) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (i, col) in row.columns().iter().enumerate() {
        let key = PyString::intern(py, col.name());
        set_cell(&dict, &key, row, i, kind_of(col.type_()))?;
    }
    Ok(dict)
}

// ── msgpack encoding (off-GIL): array of {column: value} maps ───────────────

fn mp_i(buf: &mut Vec<u8>, v: Option<i64>) -> Result<(), String> {
    match v {
        Some(x) => rmp::encode::write_sint(buf, x)
            .map(|_| ())
            .map_err(|e| e.to_string()),
        None => rmp::encode::write_nil(buf).map_err(|e| e.to_string()),
    }
}

fn mp_f(buf: &mut Vec<u8>, v: Option<f64>) -> Result<(), String> {
    match v {
        Some(x) => rmp::encode::write_f64(buf, x).map_err(|e| e.to_string()),
        None => rmp::encode::write_nil(buf).map_err(|e| e.to_string()),
    }
}

fn mp_b(buf: &mut Vec<u8>, v: Option<bool>) -> Result<(), String> {
    match v {
        Some(x) => rmp::encode::write_bool(buf, x).map_err(|e| e.to_string()),
        None => rmp::encode::write_nil(buf).map_err(|e| e.to_string()),
    }
}

fn mp_s(buf: &mut Vec<u8>, v: Option<String>) -> Result<(), String> {
    match v {
        Some(s) => rmp::encode::write_str(buf, &s).map_err(|e| e.to_string()),
        None => rmp::encode::write_nil(buf).map_err(|e| e.to_string()),
    }
}

fn encode_msgpack(rows: &[Row]) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    rmp::encode::write_array_len(&mut buf, rows.len() as u32).map_err(|e| e.to_string())?;
    let Some(first) = rows.first() else {
        return Ok(buf);
    };
    let plan: Vec<(String, ColKind)> = first
        .columns()
        .iter()
        .map(|c| (c.name().to_string(), kind_of(c.type_())))
        .collect();
    for row in rows {
        rmp::encode::write_map_len(&mut buf, plan.len() as u32).map_err(|e| e.to_string())?;
        for (i, (name, kind)) in plan.iter().enumerate() {
            rmp::encode::write_str(&mut buf, name).map_err(|e| e.to_string())?;
            match kind {
                ColKind::Int2 => mp_i(
                    &mut buf,
                    row.try_get::<usize, Option<i16>>(i)
                        .map_err(|e| e.to_string())?
                        .map(i64::from),
                )?,
                ColKind::Int4 => mp_i(
                    &mut buf,
                    row.try_get::<usize, Option<i32>>(i)
                        .map_err(|e| e.to_string())?
                        .map(i64::from),
                )?,
                ColKind::Int8 => mp_i(
                    &mut buf,
                    row.try_get::<usize, Option<i64>>(i)
                        .map_err(|e| e.to_string())?,
                )?,
                ColKind::Float4 => mp_f(
                    &mut buf,
                    row.try_get::<usize, Option<f32>>(i)
                        .map_err(|e| e.to_string())?
                        .map(f64::from),
                )?,
                ColKind::Float8 => mp_f(
                    &mut buf,
                    row.try_get::<usize, Option<f64>>(i)
                        .map_err(|e| e.to_string())?,
                )?,
                ColKind::Bool => mp_b(
                    &mut buf,
                    row.try_get::<usize, Option<bool>>(i)
                        .map_err(|e| e.to_string())?,
                )?,
                ColKind::Text => mp_s(
                    &mut buf,
                    row.try_get::<usize, Option<String>>(i)
                        .map_err(|e| e.to_string())?,
                )?,
                ColKind::Other => mp_s(
                    &mut buf,
                    row.try_get::<usize, Option<String>>(i).ok().flatten(),
                )?,
            }
        }
    }
    Ok(buf)
}

fn write_cell_mp(buf: &mut Vec<u8>, row: &Row, i: usize, kind: ColKind) -> Result<(), String> {
    match kind {
        ColKind::Int2 => mp_i(
            buf,
            row.try_get::<usize, Option<i16>>(i)
                .map_err(|e| e.to_string())?
                .map(i64::from),
        ),
        ColKind::Int4 => mp_i(
            buf,
            row.try_get::<usize, Option<i32>>(i)
                .map_err(|e| e.to_string())?
                .map(i64::from),
        ),
        ColKind::Int8 => mp_i(
            buf,
            row.try_get::<usize, Option<i64>>(i)
                .map_err(|e| e.to_string())?,
        ),
        ColKind::Float4 => mp_f(
            buf,
            row.try_get::<usize, Option<f32>>(i)
                .map_err(|e| e.to_string())?
                .map(f64::from),
        ),
        ColKind::Float8 => mp_f(
            buf,
            row.try_get::<usize, Option<f64>>(i)
                .map_err(|e| e.to_string())?,
        ),
        ColKind::Bool => mp_b(
            buf,
            row.try_get::<usize, Option<bool>>(i)
                .map_err(|e| e.to_string())?,
        ),
        ColKind::Text => mp_s(
            buf,
            row.try_get::<usize, Option<String>>(i)
                .map_err(|e| e.to_string())?,
        ),
        ColKind::Other => mp_s(buf, row.try_get::<usize, Option<String>>(i).ok().flatten()),
    }
}

fn encode_msgpack_row(row: &Row) -> Result<Vec<u8>, String> {
    let cols = row.columns();
    let mut buf = Vec::new();
    rmp::encode::write_map_len(&mut buf, cols.len() as u32).map_err(|e| e.to_string())?;
    for (i, col) in cols.iter().enumerate() {
        rmp::encode::write_str(&mut buf, col.name()).map_err(|e| e.to_string())?;
        write_cell_mp(&mut buf, row, i, kind_of(col.type_()))?;
    }
    Ok(buf)
}

// ── Query helpers shared by the pool engine and pinned transactions ────────

async fn q_execute(
    client: &deadpool_postgres::Object,
    sql: &str,
    params: &[Param],
) -> Result<u64, PyErr> {
    let stmt = client.prepare_cached(sql).await.map_err(err)?;
    let refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
    client.execute(&stmt, &refs).await.map_err(err)
}

async fn q_query(
    client: &deadpool_postgres::Object,
    sql: &str,
    params: &[Param],
) -> Result<Vec<Row>, PyErr> {
    let stmt = client.prepare_cached(sql).await.map_err(err)?;
    let refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
    client.query(&stmt, &refs).await.map_err(err)
}

async fn q_query_opt(
    client: &deadpool_postgres::Object,
    sql: &str,
    params: &[Param],
) -> Result<Option<Row>, PyErr> {
    let stmt = client.prepare_cached(sql).await.map_err(err)?;
    let refs: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
    client.query_opt(&stmt, &refs).await.map_err(err)
}

#[pyclass]
struct PgEngine {
    pool: Pool,
}

#[pymethods]
impl PgEngine {
    /// Run a statement, returning rows affected.
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = pool.get().await.map_err(err)?;
            let stmt = client.prepare_cached(&sql).await.map_err(err)?;
            let refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
            let affected = client.execute(&stmt, &refs).await.map_err(err)?;
            Ok(affected as i64)
        })
    }

    /// Fetch all rows as a list of dicts.
    fn fetch_all<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = pool.get().await.map_err(err)?;
            let stmt = client.prepare_cached(&sql).await.map_err(err)?;
            let refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
            let rows = client.query(&stmt, &refs).await.map_err(err)?;
            Python::attach(|py| rows_to_pylist(py, &rows))
        })
    }

    /// Fetch all rows as a msgpack buffer (array of {column: value} maps),
    /// encoded off-GIL for `msgspec.msgpack` to decode straight into Structs.
    fn fetch_all_msgpack<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = pool.get().await.map_err(err)?;
            let stmt = client.prepare_cached(&sql).await.map_err(err)?;
            let refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
            let rows = client.query(&stmt, &refs).await.map_err(err)?;
            let buf = encode_msgpack(&rows).map_err(PyRuntimeError::new_err)?;
            Python::attach(|py| Ok(PyBytes::new(py, &buf).unbind().into_any()))
        })
    }

    /// Fetch a single row as a msgpack map buffer, or None.
    fn fetch_one_msgpack<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = pool.get().await.map_err(err)?;
            let stmt = client.prepare_cached(&sql).await.map_err(err)?;
            let refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
            let row = client.query_opt(&stmt, &refs).await.map_err(err)?;
            Python::attach(|py| match row {
                Some(ref r) => {
                    let buf = encode_msgpack_row(r).map_err(PyRuntimeError::new_err)?;
                    Ok(PyBytes::new(py, &buf).unbind().into_any())
                }
                None => Ok(py.None()),
            })
        })
    }

    /// Fetch a single row as a dict, or None.
    fn fetch_one<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = pool.get().await.map_err(err)?;
            let stmt = client.prepare_cached(&sql).await.map_err(err)?;
            let refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
            let row = client.query_opt(&stmt, &refs).await.map_err(err)?;
            Python::attach(|py| match row {
                Some(ref r) => Ok(row_to_pydict(py, r)?.into_any().unbind()),
                None => Ok(py.None()),
            })
        })
    }

    /// Begin a transaction on a pinned connection, returning a `PgTransaction`.
    fn begin<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let client = pool.get().await.map_err(err)?;
            client.batch_execute("BEGIN").await.map_err(err)?;
            Python::attach(|py| {
                Py::new(
                    py,
                    PgTransaction {
                        client: Arc::new(client),
                    },
                )
                .map(|t| t.into_any())
            })
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = self.pool.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            pool.close();
            Ok(())
        })
    }
}

/// A transaction pinned to one connection (BEGIN issued; COMMIT/ROLLBACK close).
#[pyclass]
struct PgTransaction {
    client: Arc<deadpool_postgres::Object>,
}

#[pymethods]
impl PgTransaction {
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let affected = q_execute(&client, &sql, &params).await?;
            Ok(affected as i64)
        })
    }

    fn fetch_all<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows = q_query(&client, &sql, &params).await?;
            Python::attach(|py| rows_to_pylist(py, &rows))
        })
    }

    fn fetch_one<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let row = q_query_opt(&client, &sql, &params).await?;
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
        let client = self.client.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows = q_query(&client, &sql, &params).await?;
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
        let client = self.client.clone();
        let params = extract_params(&params)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let row = q_query_opt(&client, &sql, &params).await?;
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
        let client = self.client.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.batch_execute("COMMIT").await.map_err(err)?;
            Ok(())
        })
    }

    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client.batch_execute("ROLLBACK").await.map_err(err)?;
            Ok(())
        })
    }
}

/// Connect to a Postgres database, returning a `PgEngine`.
#[pyfunction]
fn connect_pg(py: Python<'_>, url: String) -> PyResult<Bound<'_, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let pg_config: tokio_postgres::Config = url.parse().map_err(err)?;
        let manager = Manager::from_config(
            pg_config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(manager).max_size(10).build().map_err(err)?;
        // Surface connection errors eagerly.
        let _ = pool.get().await.map_err(err)?;
        Python::attach(|py| Py::new(py, PgEngine { pool }).map(|engine| engine.into_any()))
    })
}

#[pymodule]
fn auniqorm_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PgEngine>()?;
    m.add_class::<PgTransaction>()?;
    m.add_function(wrap_pyfunction!(connect_pg, m)?)?;
    m.add_class::<mysql::MySqlEngine>()?;
    m.add_class::<mysql::MySqlTransaction>()?;
    m.add_function(wrap_pyfunction!(mysql::connect_mysql, m)?)?;
    m.add_class::<sqlite::SqliteEngine>()?;
    m.add_function(wrap_pyfunction!(sqlite::connect_sqlite, m)?)?;
    Ok(())
}
