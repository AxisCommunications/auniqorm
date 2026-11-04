//! Native MongoDB transactions.

use std::sync::Arc;

use mongodb::{Client, ClientSession};
use pyo3::prelude::*;
use tokio::sync::{MappedMutexGuard, Mutex, MutexGuard};

use super::error;
use crate::bridge::{self, BridgeCore};

/// Lifecycle of a [`MongoSession`]'s client session.
enum Slot {
    /// No operation has run yet; nothing exists on the server.
    Unused,
    Active(Box<ClientSession>),
    /// Committed or aborted; the client session has been returned.
    Ended,
}

/// Shared state of a [`MongoSession`].
pub struct SessionState {
    client: Client,
    slot: Mutex<Slot>,
}

impl SessionState {
    /// Lock the session for one operation, starting it on first use.
    ///
    /// Starting lazily lets `begin()` return without a round trip into the
    /// runtime; the session and transaction start inside the first
    /// operation's call instead. The lock serialises operations, as MongoDB
    /// requires for a single session.
    pub async fn acquire(&self) -> PyResult<MappedMutexGuard<'_, ClientSession>> {
        let mut guard = self.slot.lock().await;
        match &*guard {
            Slot::Ended => return Err(error("MongoDB transaction has already ended")),
            Slot::Unused => {
                let mut session = self.client.start_session().await.map_err(error)?;
                session.start_transaction().await.map_err(error)?;
                *guard = Slot::Active(Box::new(session));
            }
            Slot::Active(_) => {}
        }
        Ok(MutexGuard::map(guard, |slot| match slot {
            Slot::Active(session) => &mut **session,
            _ => unreachable!("session started above"),
        }))
    }

    /// Commit or abort, then return the client session to the pool.
    ///
    /// Ending the session explicitly instead of on drop matters because
    /// Python references to the handle can outlive the transaction, for
    /// example in contexts copied by tasks started inside it, and the client
    /// cannot shut down while a session is alive.
    async fn end(&self, commit: bool) -> PyResult<()> {
        let mut guard = self.slot.lock().await;
        let Slot::Active(mut session) = std::mem::replace(&mut *guard, Slot::Ended) else {
            return Ok(());
        };
        let result = if commit {
            session.commit_transaction().await
        } else {
            session.abort_transaction().await
        };
        result.map_err(error)
    }
}

/// A transaction on a client session that starts with its first operation.
#[pyclass]
pub struct MongoSession {
    pub(super) state: Arc<SessionState>,
    bridge: Option<Arc<BridgeCore>>,
}

impl MongoSession {
    pub fn new(client: Client, bridge: Option<Arc<BridgeCore>>) -> Self {
        MongoSession {
            state: Arc::new(SessionState {
                client,
                slot: Mutex::new(Slot::Unused),
            }),
            bridge,
        }
    }
}

#[pymethods]
impl MongoSession {
    /// End the transaction now if no operation ever used it.
    ///
    /// Returns `True` when there was nothing to commit or abort, so callers
    /// can skip the asynchronous `commit()`/`abort()` round trip.
    fn end_if_unused(&self) -> bool {
        let Ok(mut guard) = self.state.slot.try_lock() else {
            return false;
        };
        match &*guard {
            Slot::Unused => {
                *guard = Slot::Ended;
                true
            }
            Slot::Ended => true,
            Slot::Active(_) => false,
        }
    }

    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        bridge::spawn(
            py,
            self.bridge.as_ref(),
            async move { state.end(true).await },
        )
    }

    fn abort<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        bridge::spawn(
            py,
            self.bridge.as_ref(),
            async move { state.end(false).await },
        )
    }
}
