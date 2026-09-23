//! JSON and MessagePack codecs for OBS WebSocket messages.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use serde::Serialize;
use serde::de::DeserializeOwned;

/// Failure while encoding or decoding a protocol message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecError {
    /// Human-readable cause. Safe to log; it does not include secrets.
    pub message: String,
}

impl CodecError {
    /// Builds an error from a displayable cause.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl core::fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl core::error::Error for CodecError {}

/// Encodes and decodes OBS WebSocket message bodies.
pub trait Codec {
    /// Serializes `value` into a WebSocket payload.
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, CodecError>;

    /// Deserializes a WebSocket payload.
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError>;

    /// Whether payloads are binary frames (MessagePack) rather than text frames (JSON).
    fn is_binary(&self) -> bool;
}

/// `obswebsocket.json` subprotocol.
#[derive(Debug, Default, Clone, Copy)]
pub struct JsonCodec;

impl Codec for JsonCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, CodecError> {
        serde_json::to_vec(value).map_err(|error| CodecError::new(error.to_string()))
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        serde_json::from_slice(bytes).map_err(|error| CodecError::new(error.to_string()))
    }

    fn is_binary(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::JsonCodec;
    use crate::codec::Codec;

    proptest::proptest! {
        #[test]
        fn json_roundtrip(text in "[ -~]{0,24}") {
            let value = serde_json::json!({"op": 6, "d": {"requestType": text}});
            let bytes = JsonCodec.encode(&value).unwrap();
            let decoded: serde_json::Value = JsonCodec.decode(&bytes).unwrap();
            proptest::prop_assert_eq!(value, decoded);
        }
    }
}

/// `obswebsocket.msgpack` subprotocol.
#[cfg(feature = "msgpack")]
#[derive(Debug, Default, Clone, Copy)]
pub struct MsgpackCodec;

#[cfg(feature = "msgpack")]
impl Codec for MsgpackCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, CodecError> {
        rmp_serde::to_vec_named(value).map_err(|error| CodecError::new(error.to_string()))
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
        rmp_serde::from_slice(bytes).map_err(|error| CodecError::new(error.to_string()))
    }

    fn is_binary(&self) -> bool {
        true
    }
}
