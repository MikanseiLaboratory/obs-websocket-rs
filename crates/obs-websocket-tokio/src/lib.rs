//! tokio transport for [`obs_websocket_io::Connection`].
//!
//! `ws://` works with the default features. Enable `rustls` for `wss://`.

use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use obs_websocket_io::{Frame, Timer, Transport};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

#[cfg(feature = "rustls")]
use std::sync::Arc;
#[cfg(feature = "rustls")]
use tokio_tungstenite::{Connector, connect_async_tls_with_config};

const SUBPROTOCOL: &str = "obswebsocket.json";

/// Failure while connecting or framing.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransportError {
    /// The TCP or TLS handshake exceeded `timeout`.
    #[error("connection timed out")]
    Timeout,
    /// tungstenite rejected the URL, handshake, or frame.
    #[error(transparent)]
    WebSocket(Box<tokio_tungstenite::tungstenite::Error>),
    /// A text frame was not UTF-8. OBS JSON frames always are.
    #[error("text frame was not valid UTF-8")]
    Utf8,
    /// A frame variant this transport does not send.
    #[error("unsupported frame")]
    UnsupportedFrame,
}

impl From<tokio_tungstenite::tungstenite::Error> for TransportError {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::WebSocket(Box::new(error))
    }
}

/// WebSocket connected with tokio.
pub struct TokioTransport {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl TokioTransport {
    /// Connects to `ws://` or, with the `rustls` feature, `wss://`.
    ///
    /// The client offers the `obswebsocket.json` subprotocol.
    pub async fn connect(url: &str, timeout: Duration) -> Result<Self, TransportError> {
        #[cfg(feature = "rustls")]
        if url.starts_with("wss://") {
            ensure_crypto_provider();
        }
        let request = client_request(url)?;
        let connect = tokio::time::timeout(timeout, connect_async(request));
        let (stream, _) = connect.await.map_err(|_| TransportError::Timeout)??;
        Ok(Self { stream })
    }

    /// Connects with a caller-supplied rustls config.
    ///
    /// Use this when the server certificate is not in the webpki root store,
    /// for example a test certificate.
    #[cfg(feature = "rustls")]
    pub async fn connect_with_rustls(
        url: &str,
        timeout: Duration,
        tls: Arc<rustls::ClientConfig>,
    ) -> Result<Self, TransportError> {
        ensure_crypto_provider();
        let request = client_request(url)?;
        let connect =
            connect_async_tls_with_config(request, None, false, Some(Connector::Rustls(tls)));
        let (stream, _) = tokio::time::timeout(timeout, connect)
            .await
            .map_err(|_| TransportError::Timeout)??;
        Ok(Self { stream })
    }
}

impl Transport for TokioTransport {
    type Error = TransportError;

    async fn send(&mut self, frame: Frame) -> Result<(), Self::Error> {
        let message = match frame {
            Frame::Text(bytes) => {
                let text = String::from_utf8(bytes).map_err(|_| TransportError::Utf8)?;
                Message::text(text)
            }
            Frame::Binary(bytes) => Message::binary(bytes),
            Frame::Close { code, reason } => Message::Close(Some(CloseFrame {
                code: code.unwrap_or(1000).into(),
                reason: reason.into(),
            })),
            _ => return Err(TransportError::UnsupportedFrame),
        };
        self.stream.send(message).await?;
        Ok(())
    }

    async fn recv(&mut self) -> Result<Frame, Self::Error> {
        loop {
            match self.stream.next().await {
                Some(Ok(Message::Text(text))) => return Ok(Frame::Text(text.as_bytes().to_vec())),
                Some(Ok(Message::Binary(bytes))) => return Ok(Frame::Binary(bytes.to_vec())),
                Some(Ok(Message::Close(frame))) => {
                    return Ok(Frame::Close {
                        code: frame.as_ref().map(|frame| u16::from(frame.code)),
                        reason: frame
                            .map(|frame| frame.reason.to_string())
                            .unwrap_or_default(),
                    });
                }
                Some(Ok(Message::Ping(payload))) => {
                    self.stream.send(Message::Pong(payload)).await?;
                }
                Some(Ok(Message::Pong(_) | Message::Frame(_))) => {}
                Some(Err(error)) => return Err(error.into()),
                None => {
                    return Ok(Frame::Close {
                        code: None,
                        reason: String::new(),
                    });
                }
            }
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.stream.send(Message::Close(None)).await?;
        Ok(())
    }
}

/// Monotonic clock backed by [`tokio::time::sleep`].
#[derive(Debug, Clone)]
pub struct TokioTimer {
    origin: Instant,
}

impl TokioTimer {
    /// Starts the clock at the current instant.
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for TokioTimer {
    fn default() -> Self {
        Self::new()
    }
}

impl Timer for TokioTimer {
    fn now_ms(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    async fn wait(&self, duration_ms: u64) {
        tokio::time::sleep(Duration::from_millis(duration_ms)).await;
    }
}

fn client_request(
    url: &str,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, TransportError> {
    let mut request = url.into_client_request()?;
    request.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        HeaderValue::from_static(SUBPROTOCOL),
    );
    Ok(request)
}

#[cfg(feature = "rustls")]
fn ensure_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
