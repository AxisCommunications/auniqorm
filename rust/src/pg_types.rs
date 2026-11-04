//! PostgreSQL bind parameters and result decoding for the native engine.
//!
//! Parameters are extracted from Python into [`Param`] under the GIL and
//! encoded for the type the server inferred for each placeholder. Conversions
//! are checked (as in asyncpg): out-of-range integers and mismatched Python
//! types raise instead of being truncated or reinterpreted. Results decode to
//! the Python types asyncpg returns (`str` for json/jsonb); unsupported types
//! raise instead of decoding to `None`.

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
use bytes::{BufMut, BytesMut};
use postgres_types::{FromSql, IsNull, Kind, ToSql, Type, to_sql_checked};
use pyo3::exceptions::{PyOverflowError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDate, PyDateTime, PyDict, PyFloat, PyInt, PyList, PyString,
    PyTime, PyTuple, PyType, PyTzInfo, PyTzInfoAccess,
};
use rust_decimal::Decimal;
use std::error::Error as StdError;
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};
use tokio_postgres::{Column, Row};
use uuid::Uuid;

type BoxErr = Box<dyn StdError + Sync + Send>;

const BOOL: u32 = 16;
const BYTEA: u32 = 17;
const CHAR: u32 = 18;
const NAME: u32 = 19;
const INT8: u32 = 20;
const INT2: u32 = 21;
const INT4: u32 = 23;
const TEXT: u32 = 25;
const OID: u32 = 26;
const JSON: u32 = 114;
const XML: u32 = 142;
const FLOAT4: u32 = 700;
const FLOAT8: u32 = 701;
const UNKNOWN: u32 = 705;
const BPCHAR: u32 = 1042;
const VARCHAR: u32 = 1043;
const DATE: u32 = 1082;
const TIME: u32 = 1083;
const TIMESTAMP: u32 = 1114;
const TIMESTAMPTZ: u32 = 1184;
const NUMERIC: u32 = 1700;
const VOID: u32 = 2278;
const UUID: u32 = 2950;
const JSONB: u32 = 3802;

/// Resolve domains to their base type (the wire format is the base type's).
fn base_type(ty: &Type) -> &Type {
    match ty.kind() {
        Kind::Domain(inner) => base_type(inner),
        _ => ty,
    }
}

/// Types whose binary format is their UTF-8 text.
fn is_textual(ty: &Type) -> bool {
    matches!(
        ty.oid(),
        TEXT | VARCHAR | BPCHAR | NAME | UNKNOWN | XML | CHAR
    ) || matches!(ty.kind(), Kind::Enum(_))
        || matches!(ty.name(), "citext" | "ltree" | "lquery" | "ltxtquery")
}

// ── Bind parameters ─────────────────────────────────────────────────────────

/// A bind parameter extracted from Python before crossing into async Rust.
#[derive(Debug)]
pub enum Param {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    /// A `dict`, serialised with `json.dumps`.
    Json(String),
    Uuid(Uuid),
    Decimal(Decimal),
    /// The instant (naive values are local time, as in asyncpg) and, for
    /// naive values, the wall clock bound to `timestamp` columns.
    DateTime {
        utc: OffsetDateTime,
        naive: Option<PrimitiveDateTime>,
    },
    Date(Date),
    Time(Time),
    Array(Vec<Param>),
}

impl Param {
    fn py_type_name(&self) -> &'static str {
        match self {
            Param::Null => "None",
            Param::Bool(_) => "bool",
            Param::Int(_) => "int",
            Param::Float(_) => "float",
            Param::Text(_) => "str",
            Param::Bytes(_) => "bytes",
            Param::Json(_) => "dict",
            Param::Uuid(_) => "uuid.UUID",
            Param::Decimal(_) => "decimal.Decimal",
            Param::DateTime { .. } => "datetime.datetime",
            Param::Date(_) => "datetime.date",
            Param::Time(_) => "datetime.time",
            Param::Array(_) => "list",
        }
    }
}

static UUID_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static DECIMAL_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
static JSON_DUMPS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// Parse a decimal string exactly; rust_decimal holds 28 significant digits
/// and would otherwise round.
fn exact_decimal(s: &str) -> Result<Decimal, String> {
    Decimal::from_str_exact(s).map_err(|e| {
        format!("numeric value {s:?} is not supported by the native driver ({e}; max 28 digits)")
    })
}

/// Extract one Python value. Checks run cheapest/most common first.
fn extract_param(item: &Bound<'_, PyAny>, depth: usize) -> PyResult<Param> {
    if item.is_none() {
        return Ok(Param::Null);
    }
    // bool before int: bool is an int subclass in Python.
    if let Ok(b) = item.cast_exact::<PyBool>() {
        return Ok(Param::Bool(b.is_true()));
    }
    if item.is_instance_of::<PyInt>() {
        return item.extract::<i64>().map(Param::Int).map_err(|_| {
            PyOverflowError::new_err(
                "int bind parameter does not fit in a 64-bit integer; \
                 pass a decimal.Decimal for numeric columns",
            )
        });
    }
    if let Ok(s) = item.cast::<PyString>() {
        return Ok(Param::Text(s.to_str()?.to_owned()));
    }
    if let Ok(f) = item.cast::<PyFloat>() {
        return Ok(Param::Float(f.value()));
    }
    if let Ok(b) = item.cast::<PyBytes>() {
        return Ok(Param::Bytes(b.as_bytes().to_vec()));
    }
    let py = item.py();
    // datetime before date: datetime is a date subclass.
    if let Ok(dt) = item.cast::<PyDateTime>() {
        let naive = match dt.get_tzinfo() {
            None => Some(item.extract()?),
            Some(_) => None,
        };
        let utc = item.call_method1("astimezone", (PyTzInfo::utc(py)?,))?;
        return Ok(Param::DateTime {
            utc: utc.extract()?,
            naive,
        });
    }
    if item.is_instance_of::<PyDate>() {
        return Ok(Param::Date(item.extract()?));
    }
    if item.is_instance_of::<PyTime>() {
        return Ok(Param::Time(item.extract()?));
    }
    if item.is_instance(UUID_TYPE.import(py, "uuid", "UUID")?)? {
        return Ok(Param::Uuid(item.extract()?));
    }
    if item.is_instance(DECIMAL_TYPE.import(py, "decimal", "Decimal")?)? {
        let text = item.call_method1("__format__", ("f",))?;
        return exact_decimal(text.cast::<PyString>()?.to_str()?)
            .map(Param::Decimal)
            .map_err(PyValueError::new_err);
    }
    if item.is_instance_of::<PyDict>() {
        let dumps = JSON_DUMPS.get_or_try_init(py, || {
            Ok::<_, PyErr>(py.import("json")?.getattr("dumps")?.unbind())
        })?;
        let text = dumps.bind(py).call1((item,))?;
        return Ok(Param::Json(text.cast::<PyString>()?.to_str()?.to_owned()));
    }
    if item.is_instance_of::<PyList>() || item.is_instance_of::<PyTuple>() {
        if depth > 64 {
            return Err(PyValueError::new_err("bind parameter nested too deeply"));
        }
        let items = item.try_iter()?;
        return items
            .map(|i| extract_param(&i?, depth + 1))
            .collect::<PyResult<_>>()
            .map(Param::Array);
    }
    if let Ok(b) = item.cast::<PyByteArray>() {
        return Ok(Param::Bytes(b.to_vec()));
    }
    Err(PyTypeError::new_err(format!(
        "unsupported bind parameter type: {}",
        item.get_type().name()?
    )))
}

pub fn extract_params(params: &Bound<'_, PyAny>) -> PyResult<Vec<Param>> {
    if params.is_none() {
        return Ok(Vec::new());
    }
    params
        .try_iter()?
        .map(|item| extract_param(&item?, 0))
        .collect()
}

fn mismatch(param: &Param, ty: &Type) -> BoxErr {
    format!(
        "cannot bind Python {} to a PostgreSQL parameter of type {}",
        param.py_type_name(),
        ty.name()
    )
    .into()
}

fn checked<T: TryFrom<i64>>(v: i64, ty: &Type) -> Result<T, BoxErr> {
    T::try_from(v).map_err(|_| {
        format!(
            "value {v} is out of range for PostgreSQL type {}",
            ty.name()
        )
        .into()
    })
}

/// A list parameter as JSON (for `json`/`jsonb` targets).
fn json_value(param: &Param) -> Result<serde_json::Value, BoxErr> {
    Ok(match param {
        Param::Null => serde_json::Value::Null,
        Param::Bool(b) => (*b).into(),
        Param::Int(v) => (*v).into(),
        Param::Float(v) => serde_json::Number::from_f64(*v)
            .ok_or("non-finite floats are not valid JSON")?
            .into(),
        Param::Text(s) => s.as_str().into(),
        Param::Json(raw) => serde_json::from_str(raw)?,
        Param::Array(items) => items.iter().map(json_value).collect::<Result<_, _>>()?,
        other => return Err(format!("{} is not JSON serializable", other.py_type_name()).into()),
    })
}

fn put_json(oid: u32, text: &[u8], out: &mut BytesMut) {
    if oid == JSONB {
        out.put_u8(1);
    }
    out.put_slice(text);
}

/// Encode one parameter in binary format for type `ty`.
fn encode_value(param: &Param, ty: &Type, out: &mut BytesMut) -> Result<IsNull, BoxErr> {
    let ty = base_type(ty);
    let oid = ty.oid();
    match (param, ty.kind()) {
        (Param::Null, _) => return Ok(IsNull::Yes),
        (Param::Array(items), Kind::Array(_)) => return items.to_sql(ty, out),
        (_, Kind::Array(_)) => return Err(mismatch(param, ty)),
        _ => {}
    }
    match (param, oid) {
        (Param::Int(v), INT8) => out.put_i64(*v),
        (Param::Int(v), INT4) => out.put_i32(checked(*v, ty)?),
        (Param::Int(v), INT2) => out.put_i16(checked(*v, ty)?),
        (Param::Int(v), OID) => out.put_u32(checked(*v, ty)?),
        (Param::Int(v), FLOAT8) => out.put_f64(*v as f64),
        (Param::Int(v), FLOAT4) => out.put_f32(*v as f32),
        (Param::Int(v), NUMERIC) => return Decimal::from(*v).to_sql(ty, out),
        (Param::Bool(b), BOOL) => out.put_u8(u8::from(*b)),
        (Param::Bool(b), INT2 | INT4 | INT8 | NUMERIC) => {
            return encode_value(&Param::Int(i64::from(*b)), ty, out);
        }
        (Param::Float(v), FLOAT8) => out.put_f64(*v),
        (Param::Float(v), FLOAT4) => {
            if v.is_finite() && (*v as f32).is_infinite() {
                return Err(format!("value {v} is out of range for float4").into());
            }
            out.put_f32(*v as f32);
        }
        (Param::Float(v), NUMERIC) => return exact_decimal(&v.to_string())?.to_sql(ty, out),
        (Param::Text(s) | Param::Json(s), JSON | JSONB) => put_json(oid, s.as_bytes(), out),
        (Param::Text(s), UUID) => return Uuid::parse_str(s)?.to_sql(ty, out),
        (Param::Text(s), NUMERIC) => return exact_decimal(s)?.to_sql(ty, out),
        (Param::Text(s), _) if is_textual(ty) => out.put_slice(s.as_bytes()),
        (Param::Bytes(b), BYTEA) => out.put_slice(b),
        (Param::Uuid(u), UUID) => return u.to_sql(ty, out),
        (Param::Decimal(d), NUMERIC) => return d.to_sql(ty, out),
        (Param::Decimal(d), FLOAT8 | FLOAT4) => {
            return encode_value(&Param::Float(f64::try_from(*d)?), ty, out);
        }
        (Param::DateTime { utc, .. }, TIMESTAMPTZ) => return utc.to_sql(ty, out),
        (Param::DateTime { naive, .. }, TIMESTAMP) => {
            return naive
                .ok_or("cannot bind a timezone-aware datetime to a timestamp (without time zone) parameter; pass a naive datetime")?
                .to_sql(ty, out);
        }
        (Param::Date(d), DATE) => return d.to_sql(ty, out),
        (Param::Time(t), TIME) => return t.to_sql(ty, out),
        (Param::Array(_), JSON | JSONB) => {
            put_json(oid, &serde_json::to_vec(&json_value(param)?)?, out);
        }
        _ => return Err(mismatch(param, ty)),
    }
    Ok(IsNull::No)
}

impl ToSql for Param {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, BoxErr> {
        encode_value(self, ty, out)
    }

    /// Types at least one [`Param`] variant can be encoded as.
    fn accepts(ty: &Type) -> bool {
        let ty = base_type(ty);
        match ty.kind() {
            Kind::Array(member) => Self::accepts(member),
            _ => {
                is_textual(ty)
                    || matches!(
                        ty.oid(),
                        BOOL | BYTEA
                            | INT2
                            | INT4
                            | INT8
                            | OID
                            | FLOAT4
                            | FLOAT8
                            | NUMERIC
                            | JSON
                            | JSONB
                            | UUID
                            | DATE
                            | TIME
                            | TIMESTAMP
                            | TIMESTAMPTZ
                    )
            }
        }
    }

    to_sql_checked!();
}

// ── Result decoding ─────────────────────────────────────────────────────────

/// Borrow a column's raw binary value; decoding happens per type below.
struct Raw<'a>(&'a [u8]);

impl<'a> FromSql<'a> for Raw<'a> {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, BoxErr> {
        Ok(Raw(raw))
    }

    fn accepts(_: &Type) -> bool {
        true
    }
}

/// A decoded cell, written either to a Python object or to msgpack.
enum Cell<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(&'a str),
    Bytes(&'a [u8]),
    Utc(OffsetDateTime),
    Naive(PrimitiveDateTime),
    Date(Date),
    Time(Time),
    Uuid(Uuid),
    Decimal(Decimal),
    List(Vec<Cell<'a>>),
}

/// Decode `numeric` exactly or fail: rust_decimal would round values with
/// more than 28 significant digits.
fn numeric(raw: &[u8]) -> Result<Decimal, BoxErr> {
    let field = |i: usize| {
        raw.get(i..i + 2)
            .map(|b| i64::from(i16::from_be_bytes([b[0], b[1]])))
    };
    let (Some(weight), Some(dscale)) = (field(2), field(6)) else {
        return Err("invalid numeric value".into());
    };
    if (weight + 1).max(0) * 4 + dscale > 28 {
        return Err("numeric value has more than 28 significant digits, which the native driver cannot represent exactly".into());
    }
    Decimal::from_sql(&Type::NUMERIC, raw)
}

/// Python dates span years 1-9999 (this also rejects `infinity`).
fn python_range(date: Date) -> Result<Date, BoxErr> {
    match date.year() {
        1..=9999 => Ok(date),
        year => Err(format!("year {year} is out of range for Python dates").into()),
    }
}

fn decode<'a>(ty: &Type, raw: &'a [u8]) -> Result<Cell<'a>, BoxErr> {
    let ty = base_type(ty);
    if let Kind::Array(member) = ty.kind() {
        // One-dimensional arrays only (postgres-types rejects more).
        let items = Vec::<Option<Raw>>::from_sql(ty, raw)?;
        return items
            .into_iter()
            .map(|item| item.map_or(Ok(Cell::Null), |Raw(raw)| decode(member, raw)))
            .collect::<Result<_, _>>()
            .map(Cell::List);
    }
    Ok(match ty.oid() {
        BOOL => Cell::Bool(bool::from_sql(ty, raw)?),
        INT2 => Cell::Int(i16::from_sql(ty, raw)?.into()),
        INT4 => Cell::Int(i32::from_sql(ty, raw)?.into()),
        INT8 => Cell::Int(i64::from_sql(ty, raw)?),
        OID => Cell::Int(u32::from_sql(ty, raw)?.into()),
        FLOAT4 => Cell::Float(f32::from_sql(ty, raw)?.into()),
        FLOAT8 => Cell::Float(f64::from_sql(ty, raw)?),
        BYTEA => Cell::Bytes(raw),
        JSONB => match raw.split_first() {
            Some((1, text)) => Cell::Str(std::str::from_utf8(text)?),
            _ => return Err("unsupported jsonb version".into()),
        },
        UUID => Cell::Uuid(Uuid::from_sql(ty, raw)?),
        NUMERIC => Cell::Decimal(numeric(raw)?),
        DATE => Cell::Date(python_range(Date::from_sql(ty, raw)?)?),
        // `time` would wrap PostgreSQL's 24:00:00 to midnight.
        TIME if i64::from_sql(&Type::INT8, raw)? >= 86_400_000_000 => {
            return Err("time 24:00:00 is out of range for datetime.time".into());
        }
        TIME => Cell::Time(Time::from_sql(ty, raw)?),
        TIMESTAMP => {
            let ts = PrimitiveDateTime::from_sql(ty, raw)?;
            python_range(ts.date())?;
            Cell::Naive(ts)
        }
        TIMESTAMPTZ => {
            let ts = OffsetDateTime::from_sql(ty, raw)?;
            python_range(ts.date())?;
            Cell::Utc(ts)
        }
        VOID => Cell::Null,
        JSON => Cell::Str(std::str::from_utf8(raw)?),
        _ if is_textual(ty) => Cell::Str(std::str::from_utf8(raw)?),
        _ => {
            return Err(format!(
                "unsupported PostgreSQL result type {}; cast it to a supported type (e.g. ::text)",
                ty.name()
            )
            .into());
        }
    })
}

fn cell<'a>(row: &'a Row, i: usize, col: &Column) -> Result<Cell<'a>, String> {
    match row.try_get::<usize, Option<Raw>>(i) {
        Ok(Some(Raw(raw))) => decode(col.type_(), raw),
        Ok(None) => Ok(Cell::Null),
        Err(e) => Err(e.into()),
    }
    .map_err(|e| format!("column {:?}: {e}", col.name()))
}

impl Cell<'_> {
    fn into_py(self, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        Ok(match self {
            Cell::Null => py.None().into_bound(py),
            Cell::Bool(v) => PyBool::new(py, v).to_owned().into_any(),
            Cell::Int(v) => v.into_pyobject(py)?.into_any(),
            Cell::Float(v) => v.into_pyobject(py)?.into_any(),
            Cell::Str(v) => PyString::new(py, v).into_any(),
            Cell::Bytes(v) => PyBytes::new(py, v).into_any(),
            Cell::Utc(v) => v.into_pyobject(py)?.into_any(),
            Cell::Naive(v) => v.into_pyobject(py)?.into_any(),
            Cell::Date(v) => v.into_pyobject(py)?.into_any(),
            Cell::Time(v) => v.into_pyobject(py)?.into_any(),
            Cell::Uuid(v) => v.into_pyobject(py)?.into_any(),
            Cell::Decimal(v) => v.into_pyobject(py)?.into_any(),
            Cell::List(items) => {
                let list = PyList::empty(py);
                for item in items {
                    list.append(item.into_py(py)?)?;
                }
                list.into_any()
            }
        })
    }

    /// Lay the value out the way `msgspec.msgpack` decodes it into Struct
    /// fields: `timestamptz` as the msgpack timestamp extension (an aware UTC
    /// datetime); naive timestamps, dates and times as ISO strings; UUID and
    /// Decimal as strings; bytea as bin.
    fn write_mp(&self, buf: &mut Vec<u8>) -> Result<(), String> {
        use rmp::encode as mp;
        fn e(err: impl std::fmt::Display) -> String {
            err.to_string()
        }
        let ymd = |d: Date| format!("{:04}-{:02}-{:02}", d.year(), d.month() as u8, d.day());
        let hms = |t: Time| {
            let (h, m, s, us) = t.as_hms_micro();
            format!("{h:02}:{m:02}:{s:02}.{us:06}")
        };
        match self {
            Cell::Null => mp::write_nil(buf).map_err(e)?,
            Cell::Bool(v) => mp::write_bool(buf, *v).map_err(e)?,
            Cell::Int(v) => drop(mp::write_sint(buf, *v).map_err(e)?),
            Cell::Float(v) => mp::write_f64(buf, *v).map_err(e)?,
            Cell::Str(v) => mp::write_str(buf, v).map_err(e)?,
            Cell::Bytes(v) => mp::write_bin(buf, v).map_err(e)?,
            Cell::Utc(v) => {
                mp::write_ext_meta(buf, 12, -1).map_err(e)?;
                buf.extend_from_slice(&v.nanosecond().to_be_bytes());
                buf.extend_from_slice(&v.unix_timestamp().to_be_bytes());
            }
            Cell::Naive(v) => {
                mp::write_str(buf, &format!("{}T{}", ymd(v.date()), hms(v.time()))).map_err(e)?;
            }
            Cell::Date(v) => mp::write_str(buf, &ymd(*v)).map_err(e)?,
            Cell::Time(v) => mp::write_str(buf, &hms(*v)).map_err(e)?,
            Cell::Uuid(v) => mp::write_str(buf, &v.to_string()).map_err(e)?,
            Cell::Decimal(v) => mp::write_str(buf, &v.to_string()).map_err(e)?,
            Cell::List(items) => {
                mp::write_array_len(buf, items.len() as u32).map_err(e)?;
                for item in items {
                    item.write_mp(buf)?;
                }
            }
        }
        Ok(())
    }
}

fn row_dict<'py>(
    py: Python<'py>,
    row: &Row,
    keys: &[Bound<'py, PyString>],
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (i, (key, col)) in keys.iter().zip(row.columns()).enumerate() {
        let value = cell(row, i, col).map_err(PyRuntimeError::new_err)?;
        dict.set_item(key, value.into_py(py)?)?;
    }
    Ok(dict)
}

fn keys<'py>(py: Python<'py>, row: &Row) -> Vec<Bound<'py, PyString>> {
    row.columns()
        .iter()
        .map(|col| PyString::intern(py, col.name()))
        .collect()
}

/// Decode a result set into a list of dicts, interning the column-name keys
/// once so they are shared across every row.
pub fn rows_to_pylist(py: Python<'_>, rows: &[Row]) -> PyResult<Py<PyAny>> {
    let list = PyList::empty(py);
    if let Some(first) = rows.first() {
        let keys = keys(py, first);
        for row in rows {
            list.append(row_dict(py, row, &keys)?)?;
        }
    }
    Ok(list.into_any().unbind())
}

pub fn row_to_pydict<'py>(py: Python<'py>, row: &Row) -> PyResult<Bound<'py, PyDict>> {
    row_dict(py, row, &keys(py, row))
}

fn write_row_mp(buf: &mut Vec<u8>, row: &Row) -> Result<(), String> {
    let cols = row.columns();
    rmp::encode::write_map_len(buf, cols.len() as u32).map_err(|e| e.to_string())?;
    for (i, col) in cols.iter().enumerate() {
        rmp::encode::write_str(buf, col.name()).map_err(|e| e.to_string())?;
        cell(row, i, col)?.write_mp(buf)?;
    }
    Ok(())
}

/// Encode rows off the GIL as a msgpack array of `{column: value}` maps.
pub fn encode_msgpack(rows: &[Row]) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    rmp::encode::write_array_len(&mut buf, rows.len() as u32).map_err(|e| e.to_string())?;
    for row in rows {
        write_row_mp(&mut buf, row)?;
    }
    Ok(buf)
}

pub fn encode_msgpack_row(row: &Row) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    write_row_mp(&mut buf, row)?;
    Ok(buf)
}
