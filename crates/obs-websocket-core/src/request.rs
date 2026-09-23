//! Typed request contract implemented by generated request types.

use serde::Serialize;
use serde::de::DeserializeOwned;

/// A generated OBS WebSocket request.
///
/// `REQUEST_TYPE` is the `requestType` string. `Response` is the `responseData` object.
/// Requests with no response fields use an empty struct that deserializes from `{}`.
pub trait Request: Serialize {
    /// Protocol `requestType`.
    const REQUEST_TYPE: &'static str;
    /// obs-websocket version that introduced the request.
    const INITIAL_VERSION: &'static str;
    /// RPC version required by the request.
    const RPC_VERSION: &'static str;
    /// Typed `responseData`.
    type Response: DeserializeOwned;
}
