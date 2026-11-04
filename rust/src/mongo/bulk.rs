//! Native MongoDB bulk writes.
//!
//! Requests are grouped into `insert`, `update`, and `delete` commands and
//! split into batches that fit MongoDB's limits, mirroring PyMongo's
//! `bulk_write`. Results use the same summary layout as PyMongo's
//! `BulkWriteResult.bulk_api_result`, with indexes relative to the original
//! request list.

use mongodb::bson::{
    Bson, Document, RawArrayBuf, RawBsonRef, RawDocument, RawDocumentBuf, doc, oid::ObjectId,
};
use mongodb::{ClientSession, Database};

const MAX_BATCH_COUNT: usize = 100_000;
// Leave headroom below the 16 MiB document limit for the command envelope.
const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024 - 16 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Insert,
    Update,
    Delete,
}

impl Kind {
    fn command(self) -> (&'static str, &'static str) {
        match self {
            Kind::Insert => ("insert", "documents"),
            Kind::Update => ("update", "updates"),
            Kind::Delete => ("delete", "deletes"),
        }
    }
}

struct Request<'a> {
    kind: Kind,
    index: usize,
    statement: &'a RawDocument,
}

struct Batch {
    kind: Kind,
    indexes: Vec<usize>,
    statements: RawArrayBuf,
    bytes: usize,
}

impl Batch {
    fn new(kind: Kind) -> Self {
        Batch {
            kind,
            indexes: Vec::new(),
            statements: RawArrayBuf::new(),
            bytes: 0,
        }
    }

    fn fits(&self, kind: Kind, bytes: usize) -> bool {
        self.indexes.is_empty()
            || (self.kind == kind
                && self.indexes.len() < MAX_BATCH_COUNT
                && self.bytes + bytes <= MAX_BATCH_BYTES)
    }

    fn push(&mut self, request: &Request<'_>) {
        let statement = match request.kind {
            Kind::Insert => with_id(request.statement),
            _ => request.statement.to_raw_document_buf(),
        };
        self.bytes += statement.as_bytes().len() + 8;
        self.indexes.push(request.index);
        self.statements.push(statement);
    }
}

/// PyMongo generates `_id` client-side for inserted documents; do the same.
fn with_id(document: &RawDocument) -> RawDocumentBuf {
    if matches!(document.get("_id"), Ok(Some(_))) {
        return document.to_raw_document_buf();
    }
    let mut buf = RawDocumentBuf::new();
    buf.append("_id", ObjectId::new());
    for (key, value) in document.iter().flatten() {
        buf.append_ref(key, value);
    }
    buf
}

fn parse_requests(command: &RawDocument) -> Result<Vec<Request<'_>>, String> {
    let requests = command
        .get_array("requests")
        .map_err(|error| error.to_string())?;
    requests
        .into_iter()
        .enumerate()
        .map(|(index, request)| {
            let request = match request.map_err(|error| error.to_string())? {
                RawBsonRef::Document(request) => request,
                _ => return Err("bulk requests must be documents".to_owned()),
            };
            let (op, statement) = request
                .iter()
                .next()
                .ok_or("empty bulk request")?
                .map_err(|error| error.to_string())?;
            let kind = match op {
                "insert" => Kind::Insert,
                "update" | "replace" => Kind::Update,
                "delete" => Kind::Delete,
                other => return Err(format!("unsupported bulk operation: {other}")),
            };
            match statement {
                RawBsonRef::Document(statement) => Ok(Request {
                    kind,
                    index,
                    statement,
                }),
                _ => Err("bulk statements must be documents".to_owned()),
            }
        })
        .collect()
}

fn batches(requests: &[Request<'_>], ordered: bool) -> Vec<Batch> {
    let groups: Vec<Vec<&Request<'_>>> = if ordered {
        vec![requests.iter().collect()]
    } else {
        [Kind::Insert, Kind::Update, Kind::Delete]
            .into_iter()
            .map(|kind| requests.iter().filter(|r| r.kind == kind).collect())
            .collect()
    };
    let mut batches = Vec::new();
    for group in groups {
        let mut current: Option<Batch> = None;
        for request in group {
            let bytes = request.statement.as_bytes().len() + 8;
            if !current
                .as_ref()
                .is_some_and(|batch| batch.fits(request.kind, bytes))
            {
                batches.extend(current.take());
                current = Some(Batch::new(request.kind));
            }
            if let Some(batch) = current.as_mut() {
                batch.push(request);
            }
        }
        batches.extend(current);
    }
    batches
}

fn count(response: &Document, key: &str) -> i64 {
    match response.get(key) {
        Some(Bson::Int32(value)) => i64::from(*value),
        Some(Bson::Int64(value)) => *value,
        Some(Bson::Double(value)) => *value as i64,
        _ => 0,
    }
}

/// Map batch-relative `index` fields to original request indexes.
fn remap(entries: Option<&Bson>, indexes: &[usize]) -> Vec<Bson> {
    let Some(Bson::Array(entries)) = entries else {
        return Vec::new();
    };
    entries
        .iter()
        .map(|entry| match entry {
            Bson::Document(entry) => {
                let mut entry = entry.clone();
                let local = count(&entry, "index");
                let global = usize::try_from(local)
                    .ok()
                    .and_then(|local| indexes.get(local))
                    .map_or(local, |global| *global as i64);
                entry.insert("index", global);
                Bson::Document(entry)
            }
            other => other.clone(),
        })
        .collect()
}

#[derive(Default)]
struct Totals {
    inserted: i64,
    matched: i64,
    modified: i64,
    removed: i64,
    upserted: Vec<Bson>,
    write_errors: Vec<Bson>,
    write_concern_errors: Vec<Bson>,
}

impl Totals {
    fn add(&mut self, kind: Kind, response: &Document, indexes: &[usize]) {
        let n = count(response, "n");
        match kind {
            Kind::Insert => self.inserted += n,
            Kind::Update => {
                let upserted = remap(response.get("upserted"), indexes);
                self.matched += n - upserted.len() as i64;
                self.modified += count(response, "nModified");
                self.upserted.extend(upserted);
            }
            Kind::Delete => self.removed += n,
        }
        self.write_errors
            .extend(remap(response.get("writeErrors"), indexes));
        if let Some(error) = response.get("writeConcernError") {
            self.write_concern_errors.push(error.clone());
        }
    }

    fn into_document(self) -> Document {
        doc! {
            "writeErrors": self.write_errors,
            "writeConcernErrors": self.write_concern_errors,
            "nInserted": self.inserted,
            "nUpserted": self.upserted.len() as i64,
            "nMatched": self.matched,
            "nModified": self.modified,
            "nRemoved": self.removed,
            "upserted": self.upserted,
        }
    }
}

/// Run the bulk write described by `command` and return PyMongo's summary.
pub async fn bulk_write(
    database: &Database,
    command: &RawDocument,
    mut session: Option<&mut ClientSession>,
) -> Result<Document, String> {
    let collection = command
        .get_str("collection")
        .map_err(|error| error.to_string())?;
    let ordered = command.get_bool("ordered").unwrap_or(true);
    let requests = parse_requests(command)?;
    let mut totals = Totals::default();
    for batch in batches(&requests, ordered) {
        let (name, list) = batch.kind.command();
        let mut raw = RawDocumentBuf::new();
        raw.append(name, collection);
        raw.append(list, batch.statements);
        raw.append("ordered", ordered);
        let mut run = database.run_raw_command(raw);
        if let Some(session) = session.as_deref_mut() {
            run = run.session(session);
        }
        let response = run.await.map_err(|error| error.to_string())?;
        totals.add(batch.kind, &response, &batch.indexes);
        if ordered && !totals.write_errors.is_empty() {
            break;
        }
    }
    Ok(totals.into_document())
}
