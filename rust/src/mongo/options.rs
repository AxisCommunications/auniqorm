//! Parsing of MongoDB operation options from BSON-encoded commands.

use std::time::Duration;

use mongodb::bson::{Bson, Document, RawBsonRef, RawDocument};
use mongodb::options::{Collation, Hint};
use pyo3::prelude::*;

use super::{error, optional_document, optional_int};

fn direction(value: RawBsonRef<'_>) -> PyResult<Bson> {
    match value {
        RawBsonRef::Int32(value) => Ok(Bson::Int32(value)),
        RawBsonRef::Int64(value) => Ok(Bson::Int64(value)),
        RawBsonRef::Double(value) => Ok(Bson::Double(value)),
        RawBsonRef::String(value) => Ok(Bson::String(value.to_owned())),
        _ => Err(error("index directions must be numbers or strings")),
    }
}

/// `hint` as an index name, a key document, or PyMongo's list of pairs.
pub fn hint(command: &RawDocument) -> PyResult<Option<Hint>> {
    match command.get("hint").map_err(error)? {
        None | Some(RawBsonRef::Null) => Ok(None),
        Some(RawBsonRef::String(name)) => Ok(Some(Hint::Name(name.to_owned()))),
        Some(RawBsonRef::Document(keys)) => Document::try_from(keys)
            .map(|keys| Some(Hint::Keys(keys)))
            .map_err(error),
        Some(RawBsonRef::Array(pairs)) => {
            let mut keys = Document::new();
            for pair in pairs {
                match pair.map_err(error)? {
                    RawBsonRef::String(name) => {
                        keys.insert(name, 1);
                    }
                    RawBsonRef::Array(pair) => {
                        let mut items = pair.into_iter();
                        let (Some(Ok(RawBsonRef::String(name))), Some(Ok(value)), None) =
                            (items.next(), items.next(), items.next())
                        else {
                            return Err(error("hint pairs must be [key, direction]"));
                        };
                        keys.insert(name, direction(value)?);
                    }
                    _ => return Err(error("hint pairs must be [key, direction]")),
                }
            }
            Ok(Some(Hint::Keys(keys)))
        }
        Some(_) => Err(error("'hint' must be an index name or key specification")),
    }
}

pub fn collation(command: &RawDocument) -> PyResult<Option<Collation>> {
    optional_document(command, "collation")?
        .map(mongodb::bson::from_document::<Collation>)
        .transpose()
        .map_err(error)
}

pub fn max_time(command: &RawDocument) -> PyResult<Option<Duration>> {
    optional_int(command, "max_time_ms")?
        .map(|value| {
            u64::try_from(value)
                .map(Duration::from_millis)
                .map_err(|_| error("'max_time_ms' must not be negative"))
        })
        .transpose()
}

pub fn array_filters(command: &RawDocument) -> PyResult<Option<Vec<Document>>> {
    match command.get("array_filters").map_err(error)? {
        None | Some(RawBsonRef::Null) => Ok(None),
        Some(RawBsonRef::Array(filters)) => filters
            .into_iter()
            .map(|filter| match filter.map_err(error)? {
                RawBsonRef::Document(filter) => Document::try_from(filter).map_err(error),
                _ => Err(error("'array_filters' must contain only documents")),
            })
            .collect::<PyResult<Vec<_>>>()
            .map(Some),
        Some(_) => Err(error("'array_filters' must be a list of documents")),
    }
}
