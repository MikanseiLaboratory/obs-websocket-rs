//! WebSocket transport for `no_std` targets.
//!
//! Framing uses `embedded-websocket` 0.9. The HTTP handshake and masking key come
//! from that crate. [`EmbassyTransport`] is generic over [`embedded_io_async::Read`]
//! and [`embedded_io_async::Write`], so it runs on an embassy-net [`tcp_socket`]
//! or on any other async byte stream. Send and receive buffers are const generics
//! owned by the transport.

#![no_std]

extern crate alloc;

#[cfg(test)]
extern crate std;

use alloc::vec::Vec;

use embedded_io_async::{Read, Write};
use embedded_websocket::{
    WebSocketClient, WebSocketCloseStatusCode, WebSocketOptions, WebSocketReadResult,
    WebSocketReceiveMessageType, WebSocketSendMessageType,
};
use obs_websocket_io::{Frame, Transport};
use rand_core::{Error as RngError, RngCore};

/// Builds an embassy-net TCP socket. The caller supplies the stack buffers.
pub fn tcp_socket<'a>(
    stack: embassy_net::Stack<'a>,
    rx_buffer: &'a mut [u8],
    tx_buffer: &'a mut [u8],
) -> embassy_net::tcp::TcpSocket<'a> {
    embassy_net::tcp::TcpSocket::new(stack, rx_buffer, tx_buffer)
}

/// Failure while framing or writing the socket.
#[derive(Debug)]
pub enum TransportError<E> {
    /// The byte stream failed.
    Io(E),
    /// `embedded-websocket` rejected a frame or the handshake.
    Protocol(embedded_websocket::Error),
    /// A frame did not fit in the const-generic buffer.
    BufferTooSmall,
    /// The peer closed the TCP stream.
    UnexpectedEof,
}

impl<E: core::fmt::Display> core::fmt::Display for TransportError<E> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "io error: {error}"),
            Self::Protocol(error) => write!(formatter, "websocket error: {error}"),
            Self::BufferTooSmall => formatter.write_str("websocket buffer is too small"),
            Self::UnexpectedEof => formatter.write_str("connection closed"),
        }
    }
}

/// OBS WebSocket client over an async byte stream.
pub struct EmbassyTransport<S, const RX: usize, const TX: usize> {
    stream: S,
    websocket: WebSocketClient<XorShift64>,
    rx: [u8; RX],
    rx_len: usize,
    tx: [u8; TX],
    message: Vec<u8>,
    message_is_text: bool,
}

impl<S, E, const RX: usize, const TX: usize> EmbassyTransport<S, RX, TX>
where
    S: Read<Error = E> + Write<Error = E>,
{
    /// Completes the WebSocket handshake, offering `obswebsocket.json`.
    ///
    /// `host` is the HTTP `Host` header, typically `192.168.1.10:4455`.
    /// `seed` feeds the masking-key generator. Zero is replaced.
    pub async fn connect(mut stream: S, host: &str, seed: u64) -> Result<Self, TransportError<E>> {
        let mut websocket = WebSocketClient::new_client(XorShift64::new(seed));
        let mut tx = [0u8; TX];
        let options = WebSocketOptions {
            path: "/",
            host,
            origin: "obs-websocket-rs",
            sub_protocols: Some(&["obswebsocket.json"]),
            additional_headers: None,
        };
        let (request_len, key) = websocket
            .client_connect(&options, &mut tx)
            .map_err(TransportError::Protocol)?;
        write_all(&mut stream, &tx[..request_len]).await?;

        let mut rx = [0u8; RX];
        let mut rx_len = 0usize;
        let header_end = loop {
            if let Some(end) = find_header_end(&rx[..rx_len]) {
                break end;
            }
            if rx_len == RX {
                return Err(TransportError::BufferTooSmall);
            }
            let read = stream
                .read(&mut rx[rx_len..])
                .await
                .map_err(TransportError::Io)?;
            if read == 0 {
                return Err(TransportError::UnexpectedEof);
            }
            rx_len += read;
        };
        let (consumed, _) = websocket
            .client_accept(&key, &rx[..header_end])
            .map_err(TransportError::Protocol)?;
        let leftover = rx_len.saturating_sub(consumed);
        if leftover > 0 {
            rx.copy_within(consumed..rx_len, 0);
        }
        Ok(Self {
            stream,
            websocket,
            rx,
            rx_len: leftover,
            tx,
            message: Vec::new(),
            message_is_text: true,
        })
    }
}

impl<S, E, const RX: usize, const TX: usize> Transport for EmbassyTransport<S, RX, TX>
where
    S: Read<Error = E> + Write<Error = E>,
    E: core::fmt::Debug + core::fmt::Display,
{
    type Error = TransportError<E>;

    async fn send(&mut self, frame: Frame) -> Result<(), Self::Error> {
        match frame {
            Frame::Text(bytes) => {
                self.write_message(WebSocketSendMessageType::Text, &bytes)
                    .await
            }
            Frame::Binary(bytes) => {
                self.write_message(WebSocketSendMessageType::Binary, &bytes)
                    .await
            }
            Frame::Close { code, reason } => {
                let status = WebSocketCloseStatusCode::Custom(code.unwrap_or(1000));
                let len = self
                    .websocket
                    .close(status, Some(reason.as_str()), &mut self.tx)
                    .map_err(TransportError::Protocol)?;
                write_all(&mut self.stream, &self.tx[..len]).await
            }
            _ => Err(TransportError::Protocol(
                embedded_websocket::Error::InvalidOpCode,
            )),
        }
    }

    async fn recv(&mut self) -> Result<Frame, Self::Error> {
        let mut decoded = [0u8; RX];
        loop {
            if self.rx_len == 0 {
                self.fill().await?;
            }
            match self.websocket.read(&self.rx[..self.rx_len], &mut decoded) {
                Err(embedded_websocket::Error::ReadFrameIncomplete) => {
                    self.fill().await?;
                }
                Err(error) => return Err(TransportError::Protocol(error)),
                Ok(result) => {
                    self.consume(result.len_from);
                    if let Some(frame) = self.take_message(&result, &decoded).await? {
                        return Ok(frame);
                    }
                }
            }
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.send(Frame::Close {
            code: Some(1000),
            reason: alloc::string::String::new(),
        })
        .await
    }
}

impl<S, E, const RX: usize, const TX: usize> EmbassyTransport<S, RX, TX>
where
    S: Read<Error = E> + Write<Error = E>,
{
    async fn write_message(
        &mut self,
        kind: WebSocketSendMessageType,
        payload: &[u8],
    ) -> Result<(), TransportError<E>> {
        let len = self
            .websocket
            .write(kind, true, payload, &mut self.tx)
            .map_err(TransportError::Protocol)?;
        write_all(&mut self.stream, &self.tx[..len]).await
    }

    async fn fill(&mut self) -> Result<(), TransportError<E>> {
        if self.rx_len == RX {
            return Err(TransportError::BufferTooSmall);
        }
        let read = self
            .stream
            .read(&mut self.rx[self.rx_len..])
            .await
            .map_err(TransportError::Io)?;
        if read == 0 {
            return Err(TransportError::UnexpectedEof);
        }
        self.rx_len += read;
        Ok(())
    }

    fn consume(&mut self, len: usize) {
        let len = len.min(self.rx_len);
        self.rx.copy_within(len..self.rx_len, 0);
        self.rx_len -= len;
    }

    async fn take_message(
        &mut self,
        result: &WebSocketReadResult,
        decoded: &[u8],
    ) -> Result<Option<Frame>, TransportError<E>> {
        match result.message_type {
            WebSocketReceiveMessageType::Ping => {
                let len = self
                    .websocket
                    .write(
                        WebSocketSendMessageType::Pong,
                        true,
                        &decoded[..result.len_to],
                        &mut self.tx,
                    )
                    .map_err(TransportError::Protocol)?;
                write_all(&mut self.stream, &self.tx[..len]).await?;
                Ok(None)
            }
            WebSocketReceiveMessageType::Pong => Ok(None),
            WebSocketReceiveMessageType::CloseMustReply => {
                let len = self
                    .websocket
                    .write(
                        WebSocketSendMessageType::CloseReply,
                        true,
                        &decoded[..result.len_to],
                        &mut self.tx,
                    )
                    .map_err(TransportError::Protocol)?;
                write_all(&mut self.stream, &self.tx[..len]).await?;
                Ok(Some(close_frame(result, decoded)))
            }
            WebSocketReceiveMessageType::CloseCompleted => Ok(Some(close_frame(result, decoded))),
            WebSocketReceiveMessageType::Text | WebSocketReceiveMessageType::Binary => {
                if self.message.is_empty() {
                    self.message_is_text = result.message_type == WebSocketReceiveMessageType::Text;
                }
                self.message.extend_from_slice(&decoded[..result.len_to]);
                if !result.end_of_message {
                    return Ok(None);
                }
                let payload = core::mem::take(&mut self.message);
                if self.message_is_text {
                    Ok(Some(Frame::Text(payload)))
                } else {
                    Ok(Some(Frame::Binary(payload)))
                }
            }
        }
    }
}

fn close_frame(result: &WebSocketReadResult, decoded: &[u8]) -> Frame {
    let code = result.close_status.map(close_code);
    let reason = if result.len_to > 2 {
        alloc::string::String::from_utf8(decoded[2..result.len_to].to_vec()).unwrap_or_default()
    } else {
        alloc::string::String::new()
    };
    Frame::Close { code, reason }
}

fn close_code(code: WebSocketCloseStatusCode) -> u16 {
    match code {
        WebSocketCloseStatusCode::NormalClosure => 1000,
        WebSocketCloseStatusCode::EndpointUnavailable => 1001,
        WebSocketCloseStatusCode::ProtocolError => 1002,
        WebSocketCloseStatusCode::InvalidMessageType => 1003,
        WebSocketCloseStatusCode::Reserved => 1004,
        WebSocketCloseStatusCode::Empty => 1005,
        WebSocketCloseStatusCode::InvalidPayloadData => 1007,
        WebSocketCloseStatusCode::PolicyViolation => 1008,
        WebSocketCloseStatusCode::MessageTooBig => 1009,
        WebSocketCloseStatusCode::MandatoryExtension => 1010,
        WebSocketCloseStatusCode::InternalServerError => 1011,
        WebSocketCloseStatusCode::TlsHandshake => 1015,
        WebSocketCloseStatusCode::Custom(code) => code,
    }
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
}

async fn write_all<S, E>(stream: &mut S, mut bytes: &[u8]) -> Result<(), TransportError<E>>
where
    S: Write<Error = E>,
{
    while !bytes.is_empty() {
        let written = stream.write(bytes).await.map_err(TransportError::Io)?;
        if written == 0 {
            return Err(TransportError::UnexpectedEof);
        }
        bytes = &bytes[written..];
    }
    stream.flush().await.map_err(TransportError::Io)?;
    Ok(())
}

/// Small deterministic generator for WebSocket masking keys.
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 {
                0x9E37_79B9_7F4A_7C15
            } else {
                seed
            },
        }
    }
}

impl RngCore for XorShift64 {
    fn next_u32(&mut self) -> u32 {
        self.next_u64() as u32
    }

    fn next_u64(&mut self) -> u64 {
        let mut state = self.state;
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        self.state = state;
        state
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        for chunk in dest.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), RngError> {
        self.fill_bytes(dest);
        Ok(())
    }
}
