//! Native MongoDB writes and single-value reads (`count_documents`,
//! `distinct`) from BSON-encoded commands.

use mongodb::action::Action;
use mongodb::bson::{Bson, Document, RawBsonRef, RawDocument, RawDocumentBuf};
use mongodb::options::UpdateModifications;
use mongodb::{ClientSession, Database};
use pyo3::prelude::*;

use super::{error, optional_document, optional_int, options};

fn raw_document(command: &RawDocument, key: &str) -> PyResult<RawDocumentBuf> {
    command
        .get_document(key)
        .map(RawDocument::to_raw_document_buf)
        .map_err(error)
}

fn raw_documents(command: &RawDocument, key: &str) -> PyResult<Vec<RawDocumentBuf>> {
    command
        .get_array(key)
        .map_err(error)?
        .into_iter()
        .map(|value| match value.map_err(error)? {
            RawBsonRef::Document(document) => Ok(document.to_raw_document_buf()),
            _ => Err(error(format!("'{key}' must contain only documents"))),
        })
        .collect()
}

fn update(command: &RawDocument) -> PyResult<UpdateModifications> {
    match command.get("update").map_err(error)? {
        Some(RawBsonRef::Document(document)) => Document::try_from(document)
            .map(UpdateModifications::Document)
            .map_err(error),
        Some(RawBsonRef::Array(stages)) => stages
            .into_iter()
            .map(|stage| match stage.map_err(error)? {
                RawBsonRef::Document(document) => Document::try_from(document).map_err(error),
                _ => Err(error("update pipeline stages must be documents")),
            })
            .collect::<PyResult<Vec<_>>>()
            .map(UpdateModifications::Pipeline),
        _ => Err(error("'update' must be a document or pipeline")),
    }
}

fn unsigned(command: &RawDocument, key: &str) -> PyResult<Option<u64>> {
    optional_int(command, key)?
        .map(u64::try_from)
        .transpose()
        .map_err(|_| error(format!("'{key}' must not be negative")))
}

/// Run the write or single-value read described by `command`.
pub async fn execute(
    database: &Database,
    command: &RawDocument,
    session: Option<&mut ClientSession>,
) -> PyResult<Bson> {
    let raw = database.collection::<RawDocumentBuf>(command.get_str("collection").map_err(error)?);
    let collection = raw.clone_with_type::<Document>();
    let filter = || optional_document(command, "filter").map(Option::unwrap_or_default);
    let upsert = || command.get_bool("upsert").unwrap_or(false);
    let collation = options::collation(command)?;
    let hint = options::hint(command)?;
    let count = |value: u64| Bson::Int64(value as i64);
    Ok(match command.get_str("operation").map_err(error)? {
        "insert_one" => {
            raw.insert_one(raw_document(command, "document")?)
                .optional(session, |action, s| action.session(s))
                .await
                .map_err(error)?
                .inserted_id
        }
        "insert_many" => {
            let inserted = raw
                .insert_many(raw_documents(command, "documents")?)
                .optional(session, |action, s| action.session(s))
                .await
                .map_err(error)?;
            let mut ids = inserted.inserted_ids.into_iter().collect::<Vec<_>>();
            ids.sort_unstable_by_key(|(index, _)| *index);
            Bson::Array(ids.into_iter().map(|(_, id)| id).collect())
        }
        operation @ ("update_one" | "update_many") => {
            let filters = options::array_filters(command)?;
            let action = if operation == "update_one" {
                collection.update_one(filter()?, update(command)?)
            } else {
                collection.update_many(filter()?, update(command)?)
            };
            let result = action
                .upsert(upsert())
                .optional(filters, |action, value| action.array_filters(value))
                .optional(collation, |action, value| action.collation(value))
                .optional(hint, |action, value| action.hint(value))
                .optional(session, |action, s| action.session(s))
                .await
                .map_err(error)?;
            count(result.modified_count)
        }
        "replace_one" => {
            let result = raw
                .replace_one(filter()?, raw_document(command, "replacement")?)
                .upsert(upsert())
                .optional(collation, |action, value| action.collation(value))
                .optional(hint, |action, value| action.hint(value))
                .optional(session, |action, s| action.session(s))
                .await
                .map_err(error)?;
            count(result.modified_count)
        }
        operation @ ("delete_one" | "delete_many") => {
            let action = if operation == "delete_one" {
                collection.delete_one(filter()?)
            } else {
                collection.delete_many(filter()?)
            };
            let result = action
                .optional(collation, |action, value| action.collation(value))
                .optional(hint, |action, value| action.hint(value))
                .optional(session, |action, s| action.session(s))
                .await
                .map_err(error)?;
            count(result.deleted_count)
        }
        "count_documents" => {
            let total = collection
                .count_documents(filter()?)
                .optional(unsigned(command, "skip")?, |action, value| {
                    action.skip(value)
                })
                .optional(unsigned(command, "limit")?, |action, value| {
                    action.limit(value)
                })
                .optional(collation, |action, value| action.collation(value))
                .optional(hint, |action, value| action.hint(value))
                .optional(options::max_time(command)?, |action, value| {
                    action.max_time(value)
                })
                .optional(session, |action, s| action.session(s))
                .await
                .map_err(error)?;
            count(total)
        }
        "distinct" => Bson::Array(
            collection
                .distinct(command.get_str("key").map_err(error)?, filter()?)
                .optional(collation, |action, value| action.collation(value))
                .optional(options::max_time(command)?, |action, value| {
                    action.max_time(value)
                })
                .optional(session, |action, s| action.session(s))
                .await
                .map_err(error)?,
        ),
        other => return Err(error(format!("Unsupported MongoDB operation: {other}"))),
    })
}
