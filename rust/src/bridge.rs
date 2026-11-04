//! Lightweight completion bridge from Tokio tasks to an asyncio event loop.
//!
//! `pyo3_async_runtimes::tokio::future_into_py` resolves every future by
//! hopping to Tokio's blocking pool, acquiring the GIL there, and scheduling
//! the result with `call_soon_threadsafe`. Each operation therefore wakes
//! several threads and makes runtime threads wait for the GIL.
//!
//! The bridge instead registers each asyncio future under an id. A finished
//! Tokio task pushes its Rust-side outcome onto a queue and writes one byte to
//! a socket pair whose read end the event loop watches with `add_reader`. The
//! loop's reader callback, [`Bridge::drain`], converts outcomes to Python
//! objects and resolves the futures. Runtime threads never take the GIL, a
//! completion wakes only the event loop thread, and completions that finish
//! together share a single wake.
//!
//! The bridge needs `add_reader`, which Windows' default proactor event loop
//! does not support, so it is Unix-only; elsewhere [`spawn`] always uses
//! `pyo3_async_runtimes`.

use std::future::Future;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[cfg(unix)]
pub use unix::{Bridge, BridgeCore};

/// Placeholder for platforms without the bridge; it cannot be constructed.
#[cfg(not(unix))]
pub enum BridgeCore {}

/// Raw bytes that become a Python `bytes` object.
pub struct Bytes(pub Vec<u8>);

impl<'py> IntoPyObject<'py> for Bytes {
    type Target = PyBytes;
    type Output = Bound<'py, PyBytes>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyBytes::new(py, &self.0))
    }
}

/// Run `future` on the Tokio runtime and return an awaitable for its result.
///
/// Uses the bridge when it belongs to the running event loop, and falls back
/// to `pyo3_async_runtimes` otherwise.
pub fn spawn<'py, F, T>(
    py: Python<'py>,
    bridge: Option<&Arc<BridgeCore>>,
    future: F,
) -> PyResult<Bound<'py, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'a> IntoPyObject<'a> + Send + 'static,
{
    match bridge {
        #[cfg(unix)]
        Some(core) => core.spawn(py, future),
        #[cfg(not(unix))]
        Some(core) => match **core {},
        None => pyo3_async_runtimes::tokio::future_into_py(py, future),
    }
}

#[cfg(unix)]
mod unix {
    use std::collections::HashMap;
    use std::future::Future;
    use std::io::{ErrorKind, Read, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, PoisonError};
    use std::task::{Context, Poll};

    use pyo3::IntoPyObjectExt;
    use pyo3::exceptions::PyRuntimeError;
    use pyo3::panic::PanicException;
    use pyo3::prelude::*;

    type Outcome = Box<dyn FnOnce(Python<'_>) -> PyResult<Py<PyAny>> + Send>;

    /// Converts a panic in a Rust future into an error instead of losing the
    /// result, so the awaiting Python future can never hang.
    struct CatchUnwind<F>(Pin<Box<F>>);

    impl<F: Future> Future for CatchUnwind<F> {
        type Output = Result<F::Output, String>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let future = self.0.as_mut();
            match catch_unwind(AssertUnwindSafe(|| future.poll(cx))) {
                Ok(poll) => poll.map(Ok),
                Err(payload) => Poll::Ready(Err(payload
                    .downcast_ref::<&str>()
                    .map(|message| (*message).to_owned())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "rust future panicked".to_owned()))),
            }
        }
    }

    /// State shared between the event loop thread and runtime threads.
    pub struct BridgeCore {
        event_loop: Py<PyAny>,
        get_running_loop: Py<PyAny>,
        reader: UnixStream,
        writer: UnixStream,
        signaled: AtomicBool,
        done: Mutex<Vec<(u64, Outcome)>>,
        pending: Mutex<HashMap<u64, Py<PyAny>>>,
        next_id: AtomicU64,
    }

    impl BridgeCore {
        pub(super) fn spawn<'py, F, T>(
            self: &Arc<Self>,
            py: Python<'py>,
            future: F,
        ) -> PyResult<Bound<'py, PyAny>>
        where
            F: Future<Output = PyResult<T>> + Send + 'static,
            T: for<'a> IntoPyObject<'a> + Send + 'static,
        {
            let running = self.get_running_loop.bind(py).call0()?;
            if !running.is(self.event_loop.bind(py)) {
                return pyo3_async_runtimes::tokio::future_into_py(py, future);
            }
            let py_future = running.call_method0(pyo3::intern!(py, "create_future"))?;
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            self.pending
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(id, py_future.clone().unbind());
            let core = self.clone();
            pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
                let outcome: Outcome = match CatchUnwind(Box::pin(future)).await {
                    Ok(Ok(value)) => Box::new(move |py| value.into_py_any(py)),
                    Ok(Err(error)) => Box::new(move |_| Err(error)),
                    Err(message) => Box::new(move |_| Err(PanicException::new_err(message))),
                };
                core.complete(id, outcome);
            });
            Ok(py_future)
        }

        fn complete(&self, id: u64, outcome: Outcome) {
            self.done
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((id, outcome));
            if !self.signaled.swap(true, Ordering::AcqRel) {
                // A full socket buffer already holds a pending wake-up.
                let _ = (&self.writer).write(&[1]);
            }
        }
    }

    /// Completion bridge for one asyncio event loop.
    ///
    /// Register it with ``loop.add_reader(bridge.fd, bridge.drain)``.
    #[pyclass(frozen)]
    pub struct Bridge {
        pub(crate) core: Arc<BridgeCore>,
    }

    #[pymethods]
    impl Bridge {
        #[new]
        fn new(py: Python<'_>, event_loop: Py<PyAny>) -> PyResult<Self> {
            let (reader, writer) = UnixStream::pair()?;
            reader.set_nonblocking(true)?;
            writer.set_nonblocking(true)?;
            let get_running_loop = py.import("asyncio")?.getattr("get_running_loop")?.unbind();
            Ok(Bridge {
                core: Arc::new(BridgeCore {
                    event_loop,
                    get_running_loop,
                    reader,
                    writer,
                    signaled: AtomicBool::new(false),
                    done: Mutex::new(Vec::new()),
                    pending: Mutex::new(HashMap::new()),
                    next_id: AtomicU64::new(0),
                }),
            })
        }

        /// File descriptor the event loop watches for completions.
        #[getter]
        fn fd(&self) -> i32 {
            self.core.reader.as_raw_fd()
        }

        /// Number of futures that have not been resolved yet.
        #[getter]
        fn pending(&self) -> usize {
            self.core
                .pending
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .len()
        }

        /// Resolve every completed future; called by the event loop.
        fn drain(&self, py: Python<'_>) -> PyResult<()> {
            let core = &self.core;
            // Consume wake-up bytes first, then reset the flag, then take the
            // queue. A completion that lands before the reset finds the flag set
            // and is in the queue taken below; one that lands after it sets the
            // flag again and writes a fresh byte, so nothing is stranded.
            let mut buffer = [0u8; 64];
            loop {
                match (&core.reader).read(&mut buffer) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                    Err(error) if error.kind() == ErrorKind::Interrupted => {}
                    Err(error) => return Err(PyRuntimeError::new_err(error.to_string())),
                }
            }
            core.signaled.store(false, Ordering::Release);
            let done =
                std::mem::take(&mut *core.done.lock().unwrap_or_else(PoisonError::into_inner));
            let mut first_error = None;
            for (id, outcome) in done {
                let future = core
                    .pending
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&id);
                let Some(future) = future else { continue };
                let future = future.bind(py);
                // A cancelled caller no longer wants the result.
                if future
                    .call_method0(pyo3::intern!(py, "done"))?
                    .is_truthy()?
                {
                    continue;
                }
                let resolved = match outcome(py) {
                    Ok(value) => future.call_method1(pyo3::intern!(py, "set_result"), (value,)),
                    Err(error) => {
                        future.call_method1(pyo3::intern!(py, "set_exception"), (error.value(py),))
                    }
                };
                if let Err(error) = resolved {
                    first_error.get_or_insert(error);
                }
            }
            first_error.map_or(Ok(()), Err)
        }
    }
}
