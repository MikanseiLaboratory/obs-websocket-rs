//! Byte-stream boundary implemented by tokio and embassy.

use alloc::string::String;
use alloc::vec::Vec;

use core::future::Future;

/// One WebSocket message.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Frame {
    /// UTF-8 JSON payload.
    Text(Vec<u8>),
    /// MessagePack payload.
    Binary(Vec<u8>),
    /// The peer closed the socket.
    Close {
        /// Close code, when the peer sent one.
        code: Option<u16>,
        /// Close reason.
        reason: String,
    },
}

/// A WebSocket that can carry OBS messages.
pub trait Transport {
    /// Transport failure. Displayed to the caller; it should not include secrets.
    type Error: core::fmt::Debug + core::fmt::Display;

    /// Writes one frame.
    fn send(&mut self, frame: Frame) -> impl Future<Output = Result<(), Self::Error>>;

    /// Reads the next frame.
    fn recv(&mut self) -> impl Future<Output = Result<Frame, Self::Error>>;

    /// Closes the socket.
    fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>>;
}
