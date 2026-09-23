//! Async connection driver for [`obs_websocket_core::Session`].
//!
//! The driver is generic over a [`Transport`] and a [`Timer`], so the same
//! handshake, request, and event loop runs on tokio and on embassy.

#![no_std]
extern crate alloc;

#[cfg(test)]
extern crate std;

mod connection;
mod handler;
mod timer;
mod transport;

pub use connection::{Connection, Error};
pub use handler::{EventHandler, NopHandler};
pub use timer::Timer;
pub use transport::{Frame, Transport};
