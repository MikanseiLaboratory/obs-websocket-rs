//! Handwritten wire types for the OBS WebSocket v5 framing.
//!
//! Opcode numbers live here so the session does not depend on generated names
//! for its control flow. [`crate::WebSocketOpCode`] is generated from the same
//! specification and checked against these constants in tests.

use alloc::string::String;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Opcode numbers from the OBS WebSocket v5 specification.
pub mod op {
    /// Server greeting. Contains the RPC version and optional authentication challenge.
    pub const HELLO: u8 = 0;
    /// Client identification, sent once after `Hello`.
    pub const IDENTIFY: u8 = 1;
    /// Server confirmation that identification succeeded.
    pub const IDENTIFIED: u8 = 2;
    /// Client update of event subscriptions on an identified session.
    pub const REIDENTIFY: u8 = 3;
    /// Server-to-client event.
    pub const EVENT: u8 = 5;
    /// Client-to-server request.
    pub const REQUEST: u8 = 6;
    /// Server response to a request.
    pub const REQUEST_RESPONSE: u8 = 7;
    /// Client-to-server batch of requests.
    pub const REQUEST_BATCH: u8 = 8;
    /// Server response to a batch.
    pub const REQUEST_BATCH_RESPONSE: u8 = 9;
}

/// An opcode, including values this crate does not know yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OpCode {
    /// Op 0.
    Hello,
    /// Op 1.
    Identify,
    /// Op 2.
    Identified,
    /// Op 3.
    Reidentify,
    /// Op 5.
    Event,
    /// Op 6.
    Request,
    /// Op 7.
    RequestResponse,
    /// Op 8.
    RequestBatch,
    /// Op 9.
    RequestBatchResponse,
    /// An opcode added by a newer obs-websocket.
    Other(u8),
}

impl OpCode {
    /// Maps a numeric opcode.
    pub fn from_u8(op: u8) -> Self {
        match op {
            op::HELLO => Self::Hello,
            op::IDENTIFY => Self::Identify,
            op::IDENTIFIED => Self::Identified,
            op::REIDENTIFY => Self::Reidentify,
            op::EVENT => Self::Event,
            op::REQUEST => Self::Request,
            op::REQUEST_RESPONSE => Self::RequestResponse,
            op::REQUEST_BATCH => Self::RequestBatch,
            op::REQUEST_BATCH_RESPONSE => Self::RequestBatchResponse,
            other => Self::Other(other),
        }
    }

    /// Numeric opcode.
    pub fn as_u8(self) -> u8 {
        match self {
            Self::Hello => op::HELLO,
            Self::Identify => op::IDENTIFY,
            Self::Identified => op::IDENTIFIED,
            Self::Reidentify => op::REIDENTIFY,
            Self::Event => op::EVENT,
            Self::Request => op::REQUEST,
            Self::RequestResponse => op::REQUEST_RESPONSE,
            Self::RequestBatch => op::REQUEST_BATCH,
            Self::RequestBatchResponse => op::REQUEST_BATCH_RESPONSE,
            Self::Other(op) => op,
        }
    }
}

/// Why the peer closed the WebSocket, when a close code was provided.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CloseReason {
    /// Close code. `4009` is authentication failure.
    pub code: u16,
    /// UTF-8 close reason, possibly empty.
    pub reason: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct Envelope<T> {
    pub op: u8,
    pub d: T,
}

#[derive(Debug, Deserialize)]
pub(crate) struct IncomingEnvelope {
    pub op: u8,
    pub d: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Hello {
    /// Kept so a `Hello` that includes this field still deserializes. Callers read it from `GetVersion`.
    #[serde(default)]
    #[allow(dead_code)]
    pub obs_web_socket_version: String,
    pub rpc_version: u32,
    #[serde(default)]
    pub authentication: Option<HelloAuthentication>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct HelloAuthentication {
    pub challenge: String,
    pub salt: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Identify<'a> {
    pub rpc_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authentication: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_subscriptions: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Identified {
    pub negotiated_rpc_version: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Reidentify {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_subscriptions: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EventMessage {
    pub event_type: String,
    /// Subscription bit that caused the event. Retained for callers that inspect the raw message.
    #[serde(default)]
    #[allow(dead_code)]
    pub event_intent: u64,
    #[serde(default)]
    pub event_data: Value,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RequestMessage<'a> {
    pub request_type: &'a str,
    pub request_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_data: Option<&'a Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResponseMessage {
    #[serde(default)]
    pub request_type: String,
    pub request_id: String,
    pub request_status: RequestStatusBody,
    #[serde(default)]
    pub response_data: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RequestStatusBody {
    pub result: bool,
    pub code: i64,
    #[serde(default)]
    pub comment: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchMessage<'a> {
    pub request_id: &'a str,
    pub halt_on_failure: bool,
    pub execution_type: i64,
    pub requests: &'a [crate::raw::RawCall],
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchResponseMessage {
    pub request_id: String,
    pub results: alloc::vec::Vec<BatchResultBody>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchResultBody {
    #[serde(default)]
    pub request_type: String,
    pub request_status: RequestStatusBody,
    #[serde(default)]
    pub response_data: Option<Value>,
}
