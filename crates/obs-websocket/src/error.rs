//! Failures returned by [`crate::Client`].

use obs_websocket_core::SessionError;

/// A connection or request failure.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// `Hello` required a password, or OBS closed the socket with `4009`.
    #[error("authentication failed")]
    AuthFailed,
    /// The TCP connection or handshake failed.
    #[error("obs is unreachable: {0}")]
    Unreachable(String),
    /// OBS rejected the request.
    #[error("request failed ({code}){comment}", comment = comment_suffix(comment))]
    Request {
        /// `requestStatus.code`.
        code: i64,
        /// `requestStatus.comment`, when OBS sent one.
        comment: Option<String>,
    },
    /// The request or the handshake exceeded its deadline.
    #[error("request timed out")]
    Timeout,
    /// The socket closed.
    #[error("connection closed ({code}): {reason}", code = code.map(|code| code.to_string()).unwrap_or_else(|| "none".to_string()))]
    Closed {
        /// WebSocket close code, when one was available.
        code: Option<u16>,
        /// Close reason.
        reason: String,
    },
    /// `GetVersion.availableRequests` does not list this request.
    #[error("request `{request_type}` is not supported by this OBS")]
    UnsupportedRequest {
        /// Protocol `requestType`.
        request_type: String,
    },
    /// The driver task stopped.
    #[error("obs-websocket driver stopped")]
    Disconnected,
    /// `wss://` was requested without the `rustls` feature.
    #[error("wss:// requires the rustls feature")]
    TlsUnavailable,
    /// The session could not encode or accept a message.
    #[error(transparent)]
    Protocol(#[from] SessionError),
}

fn comment_suffix(comment: &Option<String>) -> String {
    match comment {
        Some(comment) => format!(": {comment}"),
        None => String::new(),
    }
}
