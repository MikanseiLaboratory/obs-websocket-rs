//! Clock injected by the runtime.

use core::future::Future;

/// Monotonic clock used for request deadlines.
pub trait Timer {
    /// Milliseconds since an arbitrary origin. Only differences are meaningful.
    fn now_ms(&self) -> u64;

    /// Resolves after `duration_ms` milliseconds.
    fn wait(&self, duration_ms: u64) -> impl Future<Output = ()>;
}
