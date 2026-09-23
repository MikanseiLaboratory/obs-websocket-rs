//! Callbacks invoked while a [`crate::Connection`] is polled.
//!
//! The handler is generic so embassy callers do not need a heap allocation.

use obs_websocket_core::{Event, WebSocketCloseCode};

/// Receives events and lifecycle changes.
pub trait EventHandler {
    /// An identified session produced an event.
    fn on_event(&mut self, event: &Event);

    /// Op 2 arrived.
    fn on_identified(&mut self, _negotiated_rpc_version: u32) {}

    /// The peer closed the socket, or identification failed.
    fn on_closed(&mut self, _code: Option<&WebSocketCloseCode>, _reason: &str) {}
}

/// Handler that discards every callback. Events stay available through [`crate::Connection::next_event`].
#[derive(Debug, Default, Clone, Copy)]
pub struct NopHandler;

impl EventHandler for NopHandler {
    fn on_event(&mut self, _event: &Event) {}
}
