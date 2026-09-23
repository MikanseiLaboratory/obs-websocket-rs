//! Drives a [`Session`](obs_websocket_core::Session) over a [`Transport`](crate::Transport).

use alloc::collections::VecDeque;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use core::pin::pin;

use futures_util::future::{Either, select};
use obs_websocket_core::{
    BatchItemResult, Codec, CodecError, Event, JsonCodec, RawCall, Request, RequestFailure,
    RequestId, Session, SessionConfig, SessionError, SessionEvent, WebSocketCloseCode,
};
use serde_json::Value;

use crate::handler::EventHandler;
use crate::timer::Timer;
use crate::transport::{Frame, Transport};

/// Failure while driving a connection.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error<T> {
    /// The session rejected or could not decode a message.
    Session(SessionError),
    /// The transport failed.
    Transport(T),
    /// A request deadline elapsed.
    Timeout,
    /// The socket closed.
    Closed {
        /// Close code, when one was available.
        code: Option<WebSocketCloseCode>,
        /// Close reason.
        reason: String,
    },
    /// `Hello` required a password and none was configured.
    AuthFailed,
    /// OBS rejected the request.
    Request(RequestFailure),
}

impl<T: core::fmt::Display> core::fmt::Display for Error<T> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Session(error) => write!(formatter, "{error}"),
            Self::Transport(error) => write!(formatter, "transport error: {error}"),
            Self::Timeout => formatter.write_str("request timed out"),
            Self::Closed { code, reason } => match code {
                Some(code) => write!(
                    formatter,
                    "connection closed ({:?}): {reason}",
                    code.as_u16()
                ),
                None => write!(formatter, "connection closed: {reason}"),
            },
            Self::AuthFailed => formatter.write_str("authentication failed"),
            Self::Request(error) => write!(formatter, "{error}"),
        }
    }
}

impl<T: core::fmt::Debug + core::fmt::Display> core::error::Error for Error<T> {}

/// Identified OBS session bound to a transport.
pub struct Connection<T: Transport, C: Codec = JsonCodec> {
    transport: T,
    session: Session<C>,
    inbox: VecDeque<SessionEvent>,
    request_timeout_ms: u64,
}

impl<T: Transport> Connection<T, JsonCodec> {
    /// Completes the OBS handshake and returns an identified connection.
    pub async fn connect(
        transport: T,
        config: SessionConfig,
        request_timeout_ms: u64,
        timer: &impl Timer,
    ) -> Result<Self, Error<T::Error>> {
        let mut connection = Self {
            transport,
            session: Session::json(config),
            inbox: VecDeque::new(),
            request_timeout_ms,
        };
        connection.drive_until_identified(timer).await?;
        Ok(connection)
    }
}

impl<T: Transport, C: Codec> Connection<T, C> {
    /// Sends a typed request and waits for its `responseData`.
    pub async fn request<R: Request>(
        &mut self,
        request: &R,
        timer: &impl Timer,
    ) -> Result<R::Response, Error<T::Error>> {
        let deadline = timer.now_ms().saturating_add(self.request_timeout_ms);
        let id = self
            .session
            .send_request(request, Some(deadline))
            .map_err(Error::Session)?;
        let value = self.wait_single(id, deadline, timer).await?;
        serde_json::from_value(value).map_err(|error| {
            Error::Session(SessionError::Decode(CodecError::new(error.to_string())))
        })
    }

    /// Sends an untyped request on this same connection.
    pub async fn raw_request(
        &mut self,
        call: &RawCall,
        timer: &impl Timer,
    ) -> Result<Value, Error<T::Error>> {
        let deadline = timer.now_ms().saturating_add(self.request_timeout_ms);
        let id = self
            .session
            .send_raw(call, Some(deadline))
            .map_err(Error::Session)?;
        self.wait_single(id, deadline, timer).await
    }

    /// Sends op 8 and waits for op 9.
    pub async fn raw_batch(
        &mut self,
        calls: &[RawCall],
        halt_on_failure: bool,
        execution_type: i64,
        timer: &impl Timer,
    ) -> Result<Vec<BatchItemResult>, Error<T::Error>> {
        let deadline = timer.now_ms().saturating_add(self.request_timeout_ms);
        let id = self
            .session
            .send_batch(calls, halt_on_failure, execution_type, Some(deadline))
            .map_err(Error::Session)?;
        self.wait_batch(id, deadline, timer).await
    }

    /// Next event, reading the socket until one arrives.
    pub async fn next_event(&mut self) -> Result<Event, Error<T::Error>> {
        loop {
            if let Some(event) = self.take_event() {
                return Ok(event);
            }
            self.recv_one().await?;
            if let Some(error) = self.closed_error() {
                return Err(error);
            }
        }
    }

    /// Replaces event subscriptions (op 3).
    pub async fn reidentify(
        &mut self,
        event_subscriptions: Option<u32>,
    ) -> Result<(), Error<T::Error>> {
        self.session
            .reidentify(event_subscriptions)
            .map_err(Error::Session)?;
        self.flush().await
    }

    /// Reads one socket message and delivers events to `handler`.
    pub async fn poll<H: EventHandler>(&mut self, handler: &mut H) -> Result<(), Error<T::Error>> {
        self.recv_one().await?;
        self.dispatch(handler);
        if let Some(error) = self.closed_error() {
            return Err(error);
        }
        Ok(())
    }

    /// Negotiated RPC version, after [`Self::connect`].
    pub fn negotiated_rpc_version(&self) -> Option<u32> {
        self.session.negotiated_rpc_version()
    }

    async fn drive_until_identified(&mut self, timer: &impl Timer) -> Result<(), Error<T::Error>> {
        let deadline = timer
            .now_ms()
            .saturating_add(self.request_timeout_ms.max(1));
        loop {
            if let Some(version) = self.take_identified() {
                let _ = version;
                return Ok(());
            }
            if let Some(error) = self.closed_error() {
                return Err(error);
            }
            let now = timer.now_ms();
            if now >= deadline {
                return Err(Error::Timeout);
            }
            self.recv_with_timeout(deadline - now, timer).await?;
        }
    }

    async fn wait_single(
        &mut self,
        id: RequestId,
        deadline: u64,
        timer: &impl Timer,
    ) -> Result<Value, Error<T::Error>> {
        loop {
            if let Some(result) = self.take_single(&id) {
                return result;
            }
            if let Some(error) = self.closed_error() {
                return Err(error);
            }
            let now = timer.now_ms();
            self.session.handle_timeout(now);
            self.drain();
            if let Some(result) = self.take_single(&id) {
                return result;
            }
            if now >= deadline {
                return Err(Error::Timeout);
            }
            self.recv_with_timeout(deadline - now, timer).await?;
        }
    }

    async fn wait_batch(
        &mut self,
        id: RequestId,
        deadline: u64,
        timer: &impl Timer,
    ) -> Result<Vec<BatchItemResult>, Error<T::Error>> {
        loop {
            if let Some(result) = self.take_batch(&id) {
                return result;
            }
            if let Some(error) = self.closed_error() {
                return Err(error);
            }
            let now = timer.now_ms();
            self.session.handle_timeout(now);
            self.drain();
            if let Some(result) = self.take_batch(&id) {
                return result;
            }
            if now >= deadline {
                return Err(Error::Timeout);
            }
            self.recv_with_timeout(deadline - now, timer).await?;
        }
    }

    async fn recv_with_timeout(
        &mut self,
        wait_ms: u64,
        timer: &impl Timer,
    ) -> Result<(), Error<T::Error>> {
        self.flush().await?;
        let frame = {
            let recv = pin!(self.transport.recv());
            let sleep = pin!(timer.wait(wait_ms.max(1)));
            match select(recv, sleep).await {
                Either::Left((frame, _)) => frame.map_err(Error::Transport)?,
                Either::Right(((), _)) => return Err(Error::Timeout),
            }
        };
        self.ingest(frame)?;
        self.flush().await
    }

    async fn recv_one(&mut self) -> Result<(), Error<T::Error>> {
        self.flush().await?;
        let frame = self.transport.recv().await.map_err(Error::Transport)?;
        self.ingest(frame)?;
        self.flush().await
    }

    async fn flush(&mut self) -> Result<(), Error<T::Error>> {
        while let Some(outgoing) = self.session.poll_transmit() {
            let frame = if outgoing.binary {
                Frame::Binary(outgoing.payload)
            } else {
                Frame::Text(outgoing.payload)
            };
            self.transport.send(frame).await.map_err(Error::Transport)?;
        }
        Ok(())
    }

    fn ingest(&mut self, frame: Frame) -> Result<(), Error<T::Error>> {
        match frame {
            Frame::Text(bytes) | Frame::Binary(bytes) => self
                .session
                .handle_message(&bytes)
                .map_err(|error| match error {
                    SessionError::AuthRequired => Error::AuthFailed,
                    other => Error::Session(other),
                })?,
            Frame::Close { code, reason } => self.session.handle_close(code, &reason),
        }
        self.drain();
        Ok(())
    }

    fn drain(&mut self) {
        while let Some(event) = self.session.poll_event() {
            self.inbox.push_back(event);
        }
    }

    fn dispatch<H: EventHandler>(&mut self, handler: &mut H) {
        let mut kept = VecDeque::new();
        while let Some(event) = self.inbox.pop_front() {
            match event {
                SessionEvent::Event(event) => handler.on_event(&event),
                SessionEvent::Identified {
                    negotiated_rpc_version,
                } => {
                    handler.on_identified(negotiated_rpc_version);
                }
                SessionEvent::Closed { code, reason } => {
                    handler.on_closed(code.as_ref(), &reason);
                    kept.push_back(SessionEvent::Closed { code, reason });
                }
                other => kept.push_back(other),
            }
        }
        self.inbox = kept;
    }

    fn take_identified(&mut self) -> Option<u32> {
        self.extract(|event| match event {
            SessionEvent::Identified {
                negotiated_rpc_version,
            } => Some(*negotiated_rpc_version),
            _ => None,
        })
    }

    fn take_event(&mut self) -> Option<Event> {
        self.extract(|event| match event {
            SessionEvent::Event(event) => Some(event.clone()),
            _ => None,
        })
    }

    fn take_single(&mut self, id: &RequestId) -> Option<Result<Value, Error<T::Error>>> {
        self.extract(|event| match event {
            SessionEvent::Response {
                request_id, result, ..
            } if request_id == id => Some(match result {
                Ok(value) => Ok(value.clone()),
                Err(RequestFailure::Timeout) => Err(Error::Timeout),
                Err(failure) => Err(Error::Request(failure.clone())),
            }),
            _ => None,
        })
    }

    fn take_batch(
        &mut self,
        id: &RequestId,
    ) -> Option<Result<Vec<BatchItemResult>, Error<T::Error>>> {
        self.extract(|event| match event {
            SessionEvent::BatchResponse {
                request_id,
                results,
            } if request_id == id => {
                let timed_out = results
                    .iter()
                    .any(|item| matches!(item.result, Err(RequestFailure::Timeout)));
                if timed_out {
                    Some(Err(Error::Timeout))
                } else {
                    Some(Ok(results.clone()))
                }
            }
            _ => None,
        })
    }

    fn extract<U>(&mut self, mut pick: impl FnMut(&SessionEvent) -> Option<U>) -> Option<U> {
        let mut index = 0;
        while index < self.inbox.len() {
            if let Some(value) = pick(&self.inbox[index]) {
                self.inbox.remove(index);
                return Some(value);
            }
            index += 1;
        }
        None
    }

    fn closed_error(&self) -> Option<Error<T::Error>> {
        self.inbox.iter().find_map(|event| match event {
            SessionEvent::Closed { code, reason } => Some(match code {
                Some(WebSocketCloseCode::AuthenticationFailed) => Error::AuthFailed,
                code => Error::Closed {
                    code: *code,
                    reason: reason.clone(),
                },
            }),
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Frame, Timer, Transport};
    use alloc::collections::VecDeque;
    use core::future::pending;
    use futures_executor::block_on;
    use obs_websocket_core::requests::GetVersion;

    #[derive(Debug)]
    struct ScriptError;

    impl core::fmt::Display for ScriptError {
        fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            formatter.write_str("script")
        }
    }

    struct Script {
        inbound: VecDeque<Frame>,
        outbound: Vec<Frame>,
    }

    impl Transport for Script {
        type Error = ScriptError;

        async fn send(&mut self, frame: Frame) -> Result<(), ScriptError> {
            self.outbound.push(frame);
            Ok(())
        }

        async fn recv(&mut self) -> Result<Frame, ScriptError> {
            match self.inbound.pop_front() {
                Some(frame) => Ok(frame),
                None => pending().await,
            }
        }

        async fn close(&mut self) -> Result<(), ScriptError> {
            Ok(())
        }
    }

    struct ManualTimer {
        now: u64,
        fire: bool,
    }

    impl Timer for ManualTimer {
        fn now_ms(&self) -> u64 {
            self.now
        }

        async fn wait(&self, _duration_ms: u64) {
            if !self.fire {
                pending().await
            }
        }
    }

    fn text(json: &str) -> Frame {
        Frame::Text(json.as_bytes().to_vec())
    }

    #[test]
    fn handshake_request_and_event() {
        block_on(async {
            let script = Script {
                inbound: VecDeque::from([
                    text(r#"{"op":0,"d":{"obsWebSocketVersion":"5.7.4","rpcVersion":1}}"#),
                    text(r#"{"op":2,"d":{"negotiatedRpcVersion":1}}"#),
                    text(r#"{"op":5,"d":{"eventType":"ExitStarted","eventIntent":1}}"#),
                    text(
                        r#"{"op":7,"d":{"requestType":"GetVersion","requestId":"1","requestStatus":{"result":true,"code":100},"responseData":{"obsVersion":"30.2.0","obsWebSocketVersion":"5.7.4","rpcVersion":1,"availableRequests":[],"supportedImageFormats":[],"platform":"test","platformDescription":"test"}}}"#,
                    ),
                ]),
                outbound: Vec::new(),
            };
            let timer = ManualTimer {
                now: 0,
                fire: false,
            };
            let mut connection =
                Connection::connect(script, SessionConfig::default(), 1_000, &timer)
                    .await
                    .unwrap();
            assert_eq!(connection.negotiated_rpc_version(), Some(1));
            let event = connection.next_event().await.unwrap();
            assert_eq!(event.event_type(), "ExitStarted");
            let version = connection
                .request(&GetVersion::new(), &timer)
                .await
                .unwrap();
            assert_eq!(version.obs_version, "30.2.0");
        });
    }

    #[test]
    fn connect_times_out_when_hello_never_arrives() {
        block_on(async {
            let script = Script {
                inbound: VecDeque::new(),
                outbound: Vec::new(),
            };
            let timer = ManualTimer { now: 0, fire: true };
            let error =
                match Connection::connect(script, SessionConfig::default(), 10, &timer).await {
                    Ok(_) => panic!("expected a timeout"),
                    Err(error) => error,
                };
            assert!(matches!(error, Error::Timeout));
        });
    }
}
