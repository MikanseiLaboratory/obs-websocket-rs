//! Stateful OBS WebSocket v5 client.
//!
//! [`Client`] keeps one driver task for typed requests, raw requests, and batches.
//! Events are available as a broadcast stream and as callbacks.

mod batch;
mod callback;
mod client;
mod config;
mod driver;
mod error;
#[cfg(feature = "state")]
mod state;

#[rustfmt::skip]
pub mod generated;

pub use batch::Batch;
pub use callback::Subscription;
pub use client::{Client, ConnectionState};
pub use config::{ConnectConfig, ReconnectPolicy};
pub use error::Error;
pub use generated::*;
pub use obs_websocket_core::{
    BatchItemResult, Event, RawCall, RequestBatchExecutionType, RequestFailure,
};

#[cfg(feature = "state")]
pub use state::{InputState, ObsState};

#[cfg(test)]
mod tests;
