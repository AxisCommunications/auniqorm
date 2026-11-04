//! Native MongoDB reads: `find` option parsing and streaming cursors.

use std::sync::Arc;

use mongodb::action::{Action, Find};
use mongodb::bson::{Document, RawBsonRef, RawDocument, RawDocumentBuf};
use mongodb::{ClientSession, Collection};
use pyo3::prelude::*;
use tokio::sync::{Mutex, mpsc};
use tokio::task::AbortHandle;

use std::time::Duration;

use mongodb::options::{Collation, Hint};

use super::{error, optional_document, optional_int, options};
use crate::bridge::{self, BridgeCore, Bytes};

/// Batches buffered ahead of Python, so the next fetch overlaps decoding.
const PREFETCH_BATCHES: usize = 2;

/// Documents returned in the first batch of a fetch. Larger results continue
/// on a prefetching cursor so Python decodes while the rest is fetched.
pub const FIRST_BATCH: u32 = 1000;

/// Options of a BSON-encoded `find`/`find_one` command.
pub struct FindSpec {
    pub filter: Document,
    projection: Option<Document>,
    sort: Option<Document>,
    skip: Option<u64>,
    limit: Option<i64>,
    batch_size: Option<u32>,
    hint: Option<Hint>,
    collation: Option<Collation>,
    max_time: Option<Duration>,
}

impl FindSpec {
    pub fn parse(command: &RawDocument) -> PyResult<Self> {
        let unsigned = |key: &str| -> PyResult<Option<u64>> {
            optional_int(command, key)?
                .map(u64::try_from)
                .transpose()
                .map_err(|_| error(format!("'{key}' must not be negative")))
        };
        Ok(FindSpec {
            filter: optional_document(command, "filter")?.unwrap_or_default(),
            projection: optional_document(command, "projection")?,
            sort: optional_document(command, "sort")?,
            skip: unsigned("skip")?,
            limit: optional_int(command, "limit")?,
            batch_size: unsigned("batch_size")?
                .map(u32::try_from)
                .transpose()
                .map_err(|_| error("'batch_size' is too large"))?,
            hint: options::hint(command)?,
            collation: options::collation(command)?,
            max_time: options::max_time(command)?,
        })
    }

    /// The batch size in effect, defaulting to `default` when unset.
    pub fn batch_size_or(&mut self, default: u32) -> u32 {
        *self.batch_size.get_or_insert(default)
    }

    pub fn find(self, collection: &Collection<RawDocumentBuf>) -> Find<'_, RawDocumentBuf> {
        collection
            .find(self.filter)
            .optional(self.projection, |find, value| find.projection(value))
            .optional(self.sort, |find, value| find.sort(value))
            .optional(self.skip, |find, value| find.skip(value))
            .optional(self.limit, |find, value| find.limit(value))
            .optional(self.batch_size, |find, value| find.batch_size(value))
            .optional(self.hint, |find, value| find.hint(value))
            .optional(self.collation, |find, value| find.collation(value))
            .optional(self.max_time, |find, value| find.max_time(value))
    }

    pub async fn find_one(
        self,
        collection: &Collection<RawDocumentBuf>,
    ) -> mongodb::error::Result<Option<RawDocumentBuf>> {
        collection
            .find_one(self.filter)
            .optional(self.projection, |find, value| find.projection(value))
            .optional(self.sort, |find, value| find.sort(value))
            .optional(self.skip, |find, value| find.skip(value))
            .optional(self.hint, |find, value| find.hint(value))
            .optional(self.collation, |find, value| find.collation(value))
            .optional(self.max_time, |find, value| find.max_time(value))
            .await
    }
}

/// Pipeline and options of a BSON-encoded `aggregate` command.
pub struct AggregateSpec {
    pipeline: Vec<Document>,
    allow_disk_use: Option<bool>,
    batch_size: Option<u32>,
    hint: Option<Hint>,
    collation: Option<Collation>,
    max_time: Option<Duration>,
}

impl AggregateSpec {
    pub fn parse(command: &RawDocument) -> PyResult<Self> {
        let pipeline = command
            .get_array("pipeline")
            .map_err(error)?
            .into_iter()
            .map(|stage| match stage.map_err(error)? {
                RawBsonRef::Document(stage) => Document::try_from(stage).map_err(error),
                _ => Err(error("pipeline stages must be documents")),
            })
            .collect::<PyResult<Vec<_>>>()?;
        let allow_disk_use = match command.get("allow_disk_use").map_err(error)? {
            None | Some(RawBsonRef::Null) => None,
            Some(RawBsonRef::Boolean(value)) => Some(value),
            Some(_) => return Err(error("'allow_disk_use' must be a boolean")),
        };
        let batch_size = optional_int(command, "batch_size")?
            .map(u32::try_from)
            .transpose()
            .map_err(|_| error("'batch_size' must be a non-negative 32-bit integer"))?;
        Ok(AggregateSpec {
            pipeline,
            allow_disk_use,
            batch_size,
            hint: options::hint(command)?,
            collation: options::collation(command)?,
            max_time: options::max_time(command)?,
        })
    }

    /// The batch size in effect, defaulting to `default` when unset.
    pub fn batch_size_or(&mut self, default: u32) -> u32 {
        *self.batch_size.get_or_insert(default)
    }

    pub async fn run(
        self,
        collection: &Collection<RawDocumentBuf>,
    ) -> mongodb::error::Result<mongodb::Cursor<RawDocumentBuf>> {
        collection
            .aggregate(self.pipeline)
            .optional(self.allow_disk_use, |aggregate, value| {
                aggregate.allow_disk_use(value)
            })
            .optional(self.batch_size, |aggregate, value| {
                aggregate.batch_size(value)
            })
            .optional(self.hint, |aggregate, value| aggregate.hint(value))
            .optional(self.collation, |aggregate, value| {
                aggregate.collation(value)
            })
            .optional(self.max_time, |aggregate, value| aggregate.max_time(value))
            .with_type::<RawDocumentBuf>()
            .await
    }
}

/// Read up to `batch_size` documents, and hand any remainder to a cursor.
pub async fn first_batch(
    mut cursor: mongodb::Cursor<RawDocumentBuf>,
    batch_size: u32,
    bridge: Option<Arc<BridgeCore>>,
) -> mongodb::error::Result<(Vec<u8>, Option<MongoCursor>)> {
    let mut encoded = Vec::new();
    let mut count = 0;
    while count < batch_size && cursor.advance().await? {
        encoded.extend_from_slice(cursor.current().as_bytes());
        count += 1;
    }
    let rest = (count == batch_size && cursor.has_next())
        .then(|| MongoCursor::spawn(cursor, batch_size as usize, bridge));
    Ok((encoded, rest))
}

/// Run a `find`, `find_one`, or `aggregate` inside a transaction session.
///
/// Session cursors borrow the session for every batch, so the whole result
/// is read here rather than streamed.
pub async fn fetch_in_session(
    collection: &Collection<RawDocumentBuf>,
    command: &RawDocument,
    session: &mut ClientSession,
) -> PyResult<Vec<u8>> {
    let mut encoded = Vec::new();
    let mut cursor = match command.get_str("operation").map_err(error)? {
        "find_one" => {
            let spec = FindSpec::parse(command)?;
            let document = collection
                .find_one(spec.filter)
                .optional(spec.projection, |find, value| find.projection(value))
                .optional(spec.sort, |find, value| find.sort(value))
                .optional(spec.skip, |find, value| find.skip(value))
                .optional(spec.hint, |find, value| find.hint(value))
                .optional(spec.collation, |find, value| find.collation(value))
                .optional(spec.max_time, |find, value| find.max_time(value))
                .session(&mut *session)
                .await
                .map_err(error)?;
            return Ok(document.map(RawDocumentBuf::into_bytes).unwrap_or_default());
        }
        "find" => FindSpec::parse(command)?
            .find(collection)
            .session(&mut *session)
            .await
            .map_err(error)?,
        "aggregate" => {
            let spec = AggregateSpec::parse(command)?;
            collection
                .aggregate(spec.pipeline)
                .optional(spec.allow_disk_use, |aggregate, value| {
                    aggregate.allow_disk_use(value)
                })
                .optional(spec.batch_size, |aggregate, value| {
                    aggregate.batch_size(value)
                })
                .optional(spec.hint, |aggregate, value| aggregate.hint(value))
                .optional(spec.collation, |aggregate, value| {
                    aggregate.collation(value)
                })
                .optional(spec.max_time, |aggregate, value| aggregate.max_time(value))
                .with_type::<RawDocumentBuf>()
                .session(&mut *session)
                .await
                .map_err(error)?
        }
        other => return Err(error(format!("Unsupported MongoDB fetch: {other}"))),
    };
    while cursor.advance(session).await.map_err(error)? {
        encoded.extend_from_slice(cursor.current().as_bytes());
    }
    Ok(encoded)
}

type Batches = mpsc::Receiver<Result<Vec<u8>, String>>;

/// Streaming cursor that prefetches batches of raw BSON in the background.
#[pyclass]
pub struct MongoCursor {
    batches: Arc<Mutex<Batches>>,
    task: AbortHandle,
    bridge: Option<Arc<BridgeCore>>,
}

impl MongoCursor {
    /// Drain `cursor` on a background task in batches of `batch_size`.
    pub fn spawn(
        mut cursor: mongodb::Cursor<RawDocumentBuf>,
        batch_size: usize,
        bridge: Option<Arc<BridgeCore>>,
    ) -> Self {
        let (sender, batches) = mpsc::channel(PREFETCH_BATCHES);
        let task = tokio::spawn(async move {
            let mut batch = Vec::new();
            let mut count = 0;
            loop {
                match cursor.advance().await {
                    Ok(true) => {
                        batch.extend_from_slice(cursor.current().as_bytes());
                        count += 1;
                        if count == batch_size {
                            let capacity = batch.len();
                            let full = std::mem::replace(&mut batch, Vec::with_capacity(capacity));
                            if sender.send(Ok(full)).await.is_err() {
                                return;
                            }
                            count = 0;
                        }
                    }
                    Ok(false) => {
                        if count > 0 {
                            let _ = sender.send(Ok(batch)).await;
                        }
                        return;
                    }
                    Err(failure) => {
                        let _ = sender.send(Err(failure.to_string())).await;
                        return;
                    }
                }
            }
        })
        .abort_handle();
        MongoCursor {
            batches: Arc::new(Mutex::new(batches)),
            task,
            bridge,
        }
    }
}

#[pymethods]
impl MongoCursor {
    /// Resolve to the next batch as concatenated BSON, or `None` when done.
    fn next_batch<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let batches = self.batches.clone();
        bridge::spawn(py, self.bridge.as_ref(), async move {
            match batches.lock().await.recv().await {
                None => Ok(None),
                Some(Ok(batch)) => Ok(Some(Bytes(batch))),
                Some(Err(failure)) => Err(error(failure)),
            }
        })
    }

    fn close(&self) {
        self.task.abort();
    }
}

impl Drop for MongoCursor {
    fn drop(&mut self) {
        self.task.abort();
    }
}
