//! Untyped requests, for vendor requests and any call the generated API does not cover.
//!
//! A [`crate::Session`] accepts these on the same connection as typed requests.

use alloc::string::String;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One request inside or outside a batch, addressed by its protocol name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawCall {
    /// `requestType`, for example `"GetVersion"` or a vendor-defined name.
    pub request_type: String,
    /// `requestData`. An empty object is omitted on the wire only when it is [`Value::Null`].
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub request_data: Value,
}

impl RawCall {
    /// Builds a call. `request_data` may be [`Value::Null`] when the request has no fields.
    pub fn new(request_type: impl Into<String>, request_data: Value) -> Self {
        Self {
            request_type: request_type.into(),
            request_data,
        }
    }
}
