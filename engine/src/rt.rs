// SPDX-License-Identifier: Apache-2.0
//! The library's runtime, and how a call of the C ABI runs on it.
//!
//! A PostgreSQL backend is one thread, and its signals must reach that
//! thread: on Linux its latch waits through a signalfd for SIGURG, which a
//! thread with the signal unblocked would take instead.  So every thread
//! the library makes blocks every signal: the runtime is built while the
//! calling thread blocks them, so its workers inherit the mask, and each
//! thread blocks them again as it starts.  Lance's own pools are made
//! lazily by code that runs on those workers, and inherit it too, because
//! Lance's code never runs on the calling thread: a call spawns its work on
//! the runtime and the calling thread waits for the answer on a channel,
//! reading the server's cancel flags as it waits.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc::{RecvTimeoutError, sync_channel};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::FutureExt;
use lance::io::ObjectStoreRegistry;
use lance::session::Session;
use tokio::runtime::Runtime;

use crate::error::{EResult, EngineError, LE_ERR_INTERRUPTED, LE_ERR_PANIC};

pub struct Engine {
    pub rt: Runtime,
    pub session: Arc<Session>,
    cancel_flag: usize,
    die_flag: usize,
    holdoff_count: usize,
    crit_section_count: usize,
    poll: Duration,
}

static ENGINE: OnceLock<Engine> = OnceLock::new();

pub struct Config {
    pub worker_threads: usize,
    pub cancel_flag: *const i32,
    pub die_flag: *const i32,
    pub holdoff_count: *const u32,
    pub crit_section_count: *const u32,
    pub index_cache_bytes: usize,
    pub metadata_cache_bytes: usize,
    pub poll: Duration,
}

fn block_all_signals() {
    // SAFETY: plain libc calls on a local set; the old mask is not needed.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigfillset(&mut set);
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
    }
}

pub fn init(config: Config) -> EResult<()> {
    if ENGINE.get().is_some() {
        return Ok(());
    }
    // SAFETY: the calling thread's mask is saved and restored around the
    // runtime's construction.
    let rt = unsafe {
        let mut all: libc::sigset_t = std::mem::zeroed();
        let mut old: libc::sigset_t = std::mem::zeroed();
        libc::sigfillset(&mut all);
        libc::pthread_sigmask(libc::SIG_BLOCK, &all, &mut old);
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder
            .enable_all()
            .thread_name("lance-engine")
            .on_thread_start(block_all_signals);
        if config.worker_threads > 0 {
            builder.worker_threads(config.worker_threads);
        }
        let rt = builder.build();
        libc::pthread_sigmask(libc::SIG_SETMASK, &old, std::ptr::null_mut());
        rt
    }
    .map_err(|e| EngineError::internal(format!("cannot start the runtime: {e}")))?;
    let session = Arc::new(Session::new(
        config.index_cache_bytes,
        config.metadata_cache_bytes,
        Arc::new(ObjectStoreRegistry::default()),
    ));
    let _ = ENGINE.set(Engine {
        rt,
        session,
        cancel_flag: config.cancel_flag as usize,
        die_flag: config.die_flag as usize,
        holdoff_count: config.holdoff_count as usize,
        crit_section_count: config.crit_section_count as usize,
        poll: config.poll,
    });
    Ok(())
}

pub fn engine() -> EResult<&'static Engine> {
    ENGINE
        .get()
        .ok_or_else(|| EngineError::internal("the engine library is not initialized"))
}

pub fn session() -> EResult<Arc<Session>> {
    Ok(engine()?.session.clone())
}

impl Engine {
    pub fn poll(&self) -> Duration {
        self.poll
    }

    pub fn interrupted(&self) -> bool {
        // SAFETY: the flags are the server's sig_atomic_t globals, alive for
        // the life of the process, read on the thread that writes them.
        unsafe {
            let held = (self.holdoff_count != 0
                && std::ptr::read_volatile(self.holdoff_count as *const u32) != 0)
                || (self.crit_section_count != 0
                    && std::ptr::read_volatile(self.crit_section_count as *const u32) != 0);
            if held {
                return false;
            }
            (self.cancel_flag != 0 && std::ptr::read_volatile(self.cancel_flag as *const i32) != 0)
                || (self.die_flag != 0 && std::ptr::read_volatile(self.die_flag as *const i32) != 0)
        }
    }
}

pub fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "a panic with no message".to_string()
    }
}

/// Run a future on the runtime and wait for it, cancelling it when the
/// server's cancel flags are set.
pub fn run<F, T>(fut: F) -> EResult<T>
where
    F: Future<Output = EResult<T>> + Send + 'static,
    T: Send + 'static,
{
    let engine = engine()?;
    let (tx, rx) = sync_channel(1);
    let handle = engine.rt.spawn(async move {
        let result = AssertUnwindSafe(fut).catch_unwind().await;
        let _ = tx.send(result);
    });
    loop {
        match rx.recv_timeout(engine.poll) {
            Ok(Ok(result)) => return result,
            Ok(Err(panic)) => {
                return Err(EngineError::new(
                    LE_ERR_PANIC,
                    format!("panic in the engine library: {}", panic_message(panic)),
                ));
            }
            Err(RecvTimeoutError::Timeout) => {
                if engine.interrupted() {
                    handle.abort();
                    return Err(EngineError::new(LE_ERR_INTERRUPTED, "canceled"));
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(EngineError::internal("the engine's task ended without an answer"));
            }
        }
    }
}

/// Whether LT_ENGINE_TRACE asks for the library's trace on stderr.
pub fn tracing() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("LT_ENGINE_TRACE").map(|v| v == "1").unwrap_or(false))
}

#[macro_export]
macro_rules! trace {
    ($($arg:tt)*) => {
        if $crate::rt::tracing() {
            eprintln!("lance_engine[{:?}]: {}", std::thread::current().id(), format!($($arg)*));
        }
    };
}
