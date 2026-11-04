//! Native MongoDB engine.
//!
//! Python encodes operations once with PyMongo's BSON encoder; the engine runs
//! them with the Rust driver and returns raw BSON for PyMongo's decoder. Reads
//! stream through prefetching cursors, and transactions run on Rust client
//! sessions. The Python driver falls back to PyMongo for operations not covered
//! here.

mod bulk;
mod options;
mod read;
mod session;
mod write;

pub use read::MongoCursor;
pub use session::MongoSession;

/// Lock an optional transaction session for one operation, starting it on
/// first use.
async fn lock_session(
    session: &Option<std::sync::Arc<session::SessionState>>,
) -> PyResult<Option<tokio::sync::MappedMutexGuard<'_, mongodb::ClientSession>>> {
    match session {
        Some(session) => session.acquire().await.map(Some),
        None => Ok(None),
    }
}

use std::sync::Arc;

use mongodb::Client;
use mongodb::bson::{Bson, Document, RawBsonRef, RawDocument, RawDocumentBuf, doc};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::bridge::{self, BridgeCore, Bytes};

fn error(message: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(message.to_string())
}

fn optional_document(command: &RawDocument, key: &str) -> PyResult<Option<Document>> {
    match command.get(key).map_err(error)? {
        None | Some(RawBsonRef::Null) => Ok(None),
        Some(RawBsonRef::Document(document)) => {
            Document::try_from(document).map(Some).map_err(error)
        }
        Some(_) => Err(error(format!("'{key}' must be a document"))),
    }
}

fn optional_int(command: &RawDocument, key: &str) -> PyResult<Option<i64>> {
    match command.get(key).map_err(error)? {
        None | Some(RawBsonRef::Null) => Ok(None),
        Some(RawBsonRef::Int32(value)) => Ok(Some(i64::from(value))),
        Some(RawBsonRef::Int64(value)) => Ok(Some(value)),
        Some(_) => Err(error(format!("'{key}' must be an integer"))),
    }
}

fn encode_result(value: Bson) -> PyResult<Vec<u8>> {
    mongodb::bson::to_vec(&doc! { "result": value }).map_err(error)
}

/// Result of a write: counts become Python integers directly, other values
/// keep their BSON types by travelling as an encoded `{"result": value}`.
enum WriteResult {
    Count(i64),
    Encoded(Bytes),
}

impl WriteResult {
    fn new(value: Bson) -> PyResult<Self> {
        Ok(match value {
            Bson::Int64(count) => WriteResult::Count(count),
            Bson::Int32(count) => WriteResult::Count(i64::from(count)),
            value => WriteResult::Encoded(Bytes(encode_result(value)?)),
        })
    }
}

impl<'py> IntoPyObject<'py> for WriteResult {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(match self {
            WriteResult::Count(count) => count.into_pyobject(py)?.into_any(),
            WriteResult::Encoded(bytes) => bytes.into_pyobject(py)?.into_any(),
        })
    }
}

#[pyclass]
pub struct MongoEngine {
    client: Client,
    database: String,
    bridge: Option<Arc<BridgeCore>>,
}

#[pymethods]
impl MongoEngine {
    #[pyo3(signature = (command, session=None))]
    fn execute_bson<'py>(
        &self,
        py: Python<'py>,
        command: Vec<u8>,
        session: Option<PyRef<'py, MongoSession>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        let database = self.database.clone();
        let session = session.map(|session| session.state.clone());
        bridge::spawn(py, self.bridge.as_ref(), async move {
            let mut guard = lock_session(&session).await?;
            let session = guard.as_deref_mut();
            let command = RawDocumentBuf::from_bytes(command).map_err(error)?;
            let result = write::execute(&client.database(&database), &command, session).await?;
            WriteResult::new(result)
        })
    }

    /// Run a BSON-encoded bulk write built by `Collection.bulk_write`.
    ///
    /// Resolves to BSON bytes for `{"result": summary}` where the summary has
    /// the layout of PyMongo's `BulkWriteResult.bulk_api_result`.
    #[pyo3(signature = (command, session=None))]
    fn bulk_write_bson<'py>(
        &self,
        py: Python<'py>,
        command: Vec<u8>,
        session: Option<PyRef<'py, MongoSession>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        let database = self.database.clone();
        let session = session.map(|session| session.state.clone());
        bridge::spawn(py, self.bridge.as_ref(), async move {
            let mut guard = lock_session(&session).await?;
            let command = RawDocumentBuf::from_bytes(command).map_err(error)?;
            let summary =
                bulk::bulk_write(&client.database(&database), &command, guard.as_deref_mut())
                    .await
                    .map_err(error)?;
            encode_result(Bson::Document(summary)).map(Bytes)
        })
    }

    /// Run a BSON-encoded `find`, `find_one`, or `aggregate` command.
    ///
    /// Resolves to `(first_batch, cursor)`: the first batch as concatenated
    /// raw BSON, and a prefetching [`MongoCursor`] for the remaining
    /// documents, or `None` when the first batch holds the whole result.
    #[pyo3(signature = (command, session=None))]
    fn fetch_bson<'py>(
        &self,
        py: Python<'py>,
        command: Vec<u8>,
        session: Option<PyRef<'py, MongoSession>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        let database = self.database.clone();
        let session = session.map(|session| session.state.clone());
        let cursor_bridge = self.bridge.clone();
        bridge::spawn(py, self.bridge.as_ref(), async move {
            let command = RawDocumentBuf::from_bytes(command).map_err(error)?;
            let collection = client
                .database(&database)
                .collection::<RawDocumentBuf>(command.get_str("collection").map_err(error)?);
            if let Some(mut session) = lock_session(&session).await? {
                let encoded = read::fetch_in_session(&collection, &command, &mut session).await?;
                return Ok((Bytes(encoded), None::<MongoCursor>));
            }
            let (encoded, rest) = match command.get_str("operation").map_err(error)? {
                "find" => {
                    let mut spec = read::FindSpec::parse(&command)?;
                    let batch_size = spec.batch_size_or(read::FIRST_BATCH);
                    let cursor = spec.find(&collection).await.map_err(error)?;
                    read::first_batch(cursor, batch_size, cursor_bridge)
                        .await
                        .map_err(error)?
                }
                "aggregate" => {
                    let mut spec = read::AggregateSpec::parse(&command)?;
                    let batch_size = spec.batch_size_or(read::FIRST_BATCH);
                    let cursor = spec.run(&collection).await.map_err(error)?;
                    read::first_batch(cursor, batch_size, cursor_bridge)
                        .await
                        .map_err(error)?
                }
                "find_one" => {
                    let spec = read::FindSpec::parse(&command)?;
                    let document = spec.find_one(&collection).await.map_err(error)?;
                    let encoded = document.map(|d| d.into_bytes()).unwrap_or_default();
                    (encoded, None)
                }
                other => return Err(error(format!("Unsupported MongoDB fetch: {other}"))),
            };
            Ok((Bytes(encoded), rest))
        })
    }

    /// Open a streaming cursor for a BSON-encoded `find` command.
    ///
    /// Resolves to a [`MongoCursor`] once the first batch is requested from
    /// the server, so query errors surface here.
    fn open_cursor_bson<'py>(
        &self,
        py: Python<'py>,
        command: Vec<u8>,
        batch_size: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        let database = self.database.clone();
        let cursor_bridge = self.bridge.clone();
        bridge::spawn(py, self.bridge.as_ref(), async move {
            let command = RawDocumentBuf::from_bytes(command).map_err(error)?;
            let collection = client
                .database(&database)
                .collection::<RawDocumentBuf>(command.get_str("collection").map_err(error)?);
            let cursor = read::FindSpec::parse(&command)?
                .find(&collection)
                .await
                .map_err(error)?;
            Ok(MongoCursor::spawn(cursor, batch_size.max(1), cursor_bridge))
        })
    }

    /// Create a transaction that starts with its first operation.
    ///
    /// Synchronous and free of server round trips; the session and
    /// transaction start inside the first operation that uses it.
    fn begin_transaction(&self) -> MongoSession {
        MongoSession::new(self.client.clone(), self.bridge.clone())
    }

    /// Resolve this engine's futures through `bridge` instead of
    /// `pyo3_async_runtimes`; see [`crate::bridge`].
    #[cfg(unix)]
    fn use_bridge(&mut self, bridge: PyRef<'_, bridge::Bridge>) {
        self.bridge = Some(bridge.core.clone());
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client.clone();
        bridge::spawn(py, self.bridge.as_ref(), async move {
            client.shutdown().await;
            Ok(())
        })
    }
}

#[pyfunction]
pub fn connect_mongo<'py>(
    py: Python<'py>,
    url: String,
    database: String,
) -> PyResult<Bound<'py, PyAny>> {
    bridge::spawn(py, None, async move {
        let client = Client::with_uri_str(url).await.map_err(error)?;
        client
            .database(&database)
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(error)?;
        Ok(MongoEngine {
            client,
            database,
            bridge: None,
        })
    })
}
