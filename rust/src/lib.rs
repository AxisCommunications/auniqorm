//! Native database engines for aUniqORM (PostgreSQL, MySQL, SQLite, MongoDB).
//!
//! Each engine runs its async database work off the Python event loop in
//! Rust; see the individual modules.

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
use pyo3::prelude::*;

mod bridge;
mod mongo;
mod mysql;
mod pg;
mod pg_types;
mod sqlite;
mod tls;

#[pymodule]
fn auniqorm_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // rustls needs one process-wide crypto provider; mongodb and mysql_async
    // pull in both ring and aws-lc-rs, so pick one explicitly.
    tls::install_default_provider();
    m.add_class::<pg::PgEngine>()?;
    m.add_class::<pg::PgTransaction>()?;
    m.add_function(wrap_pyfunction!(pg::connect_pg, m)?)?;
    m.add_class::<mysql::MySqlEngine>()?;
    m.add_class::<mysql::MySqlTransaction>()?;
    m.add_function(wrap_pyfunction!(mysql::connect_mysql, m)?)?;
    #[cfg(unix)]
    m.add_class::<bridge::Bridge>()?;
    m.add_class::<mongo::MongoEngine>()?;
    m.add_class::<mongo::MongoCursor>()?;
    m.add_class::<mongo::MongoSession>()?;
    m.add_function(wrap_pyfunction!(mongo::connect_mongo, m)?)?;
    m.add_class::<sqlite::SqliteEngine>()?;
    m.add_function(wrap_pyfunction!(sqlite::connect_sqlite, m)?)?;
    Ok(())
}
