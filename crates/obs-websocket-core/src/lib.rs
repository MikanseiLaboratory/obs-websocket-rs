//! Sans I/O core for the OBS WebSocket v5 protocol.
//!
//! This crate does not open sockets. Feed bytes into [`Session`] and poll it for
//! bytes to write and events to surface. Pair it with `obs-websocket-io` and a
//! transport (`obs-websocket-tokio` or `obs-websocket-embassy`).
//!
//! Typed requests, responses, and events are generated from obs-websocket's
//! `protocol.json`. Regenerate them with `cargo xtask codegen`.

#![no_std]
extern crate alloc;

pub mod auth;
pub mod codec;
pub mod protocol;
pub mod raw;
pub mod request;
pub mod session;

#[rustfmt::skip]
pub mod generated;

pub use auth::authentication_string;
pub use codec::{Codec, CodecError, JsonCodec};
pub use generated::enums::{
    EventSubscription, ObsMediaInputAction, ObsOutputState, RequestBatchExecutionType,
    RequestStatus, WebSocketCloseCode, WebSocketOpCode,
};
pub use generated::events::{Event, EventPayload};
pub use generated::requests;
pub use generated::types;
pub use protocol::{CloseReason, OpCode};
pub use raw::RawCall;
pub use request::Request;
pub use session::{
    BatchItemResult, Outgoing, RequestFailure, RequestId, Session, SessionConfig, SessionError,
    SessionEvent,
};

#[cfg(feature = "msgpack")]
pub use codec::MsgpackCodec;
