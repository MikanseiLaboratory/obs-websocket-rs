//! Sans I/O session state machine.
//!
//! The session never reads a clock or a socket. The caller feeds messages,
//! close codes, and timestamps, then polls outgoing frames and events.

use alloc::collections::{BTreeMap, VecDeque};
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use serde::Serialize;
use serde_json::Value;

use crate::auth::authentication_string;
use crate::codec::{Codec, CodecError, JsonCodec};
use crate::generated::{Event, WebSocketCloseCode};
use crate::protocol::{
    BatchMessage, BatchResponseMessage, Envelope, EventMessage, Hello, Identified, Identify,
    IncomingEnvelope, Reidentify, RequestMessage, ResponseMessage, op,
};
use crate::raw::RawCall;
use crate::request::Request;

/// Configuration captured at identification time.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// WebSocket password. Empty is treated as no password.
    pub password: Option<String>,
    /// `eventSubscriptions` bitset. `None` lets OBS apply its default (`All`).
    pub event_subscriptions: Option<u32>,
    /// RPC version requested in `Identify`. Negotiated down to the server's version.
    pub rpc_version: u32,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            password: None,
            event_subscriptions: None,
            rpc_version: 1,
        }
    }
}

impl SessionConfig {
    /// Sets the password, treating an empty string as absent.
    pub fn password(mut self, password: Option<&str>) -> Self {
        self.password = password
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        self
    }
}

/// Client-assigned request id. OBS echoes it on the response.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId(pub String);

impl RequestId {
    /// Borrows the id string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl core::fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A frame the caller should write to the socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outgoing {
    /// Encoded OBS message.
    pub payload: Vec<u8>,
    /// `true` for MessagePack, `false` for JSON text.
    pub binary: bool,
}

/// A request that did not produce `responseData`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RequestFailure {
    /// OBS rejected the request. `code` is a [`crate::RequestStatus`] value.
    Status {
        /// `requestStatus.code`.
        code: i64,
        /// `requestStatus.comment`, when OBS sent one.
        comment: Option<String>,
    },
    /// `deadline_ms` passed before a matching response arrived.
    Timeout,
    /// The socket closed while the request was in flight.
    Closed,
}

impl core::fmt::Display for RequestFailure {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Status { code, comment } => match comment {
                Some(comment) => write!(formatter, "request failed ({code}): {comment}"),
                None => write!(formatter, "request failed ({code})"),
            },
            Self::Timeout => formatter.write_str("request timed out"),
            Self::Closed => formatter.write_str("connection closed"),
        }
    }
}

impl core::error::Error for RequestFailure {}

/// One entry of a batch response, in request order.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchItemResult {
    /// `requestType` of this entry.
    pub request_type: String,
    /// `responseData` or the status error.
    pub result: Result<Value, RequestFailure>,
}

/// Something the caller should observe after feeding the session.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum SessionEvent {
    /// Op 2. Requests may be sent after this.
    Identified {
        /// RPC version the server agreed to.
        negotiated_rpc_version: u32,
    },
    /// Op 5.
    Event(Event),
    /// Op 7 matched to a pending request.
    Response {
        /// Id returned by [`Session::send_request`] or [`Session::send_raw`].
        request_id: RequestId,
        /// `requestType` echoed by OBS.
        request_type: String,
        /// `responseData`, or `{}` when OBS omitted it.
        result: Result<Value, RequestFailure>,
    },
    /// Op 9 matched to a pending batch.
    BatchResponse {
        /// Id returned by [`Session::send_batch`].
        request_id: RequestId,
        /// Per-request results.
        results: Vec<BatchItemResult>,
    },
    /// Op 7 or op 9 whose id is not pending. The usual cause is a response after timeout.
    UnmatchedResponse {
        /// Id OBS sent.
        request_id: String,
    },
    /// The peer closed the socket, or identification failed locally.
    Closed {
        /// Close code, when one was provided.
        code: Option<WebSocketCloseCode>,
        /// Close reason.
        reason: String,
    },
}

/// Errors produced while driving a [`Session`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SessionError {
    /// The session has already closed.
    Closed,
    /// A request was attempted before op 2.
    NotIdentified,
    /// `Hello` required authentication and no password was configured.
    AuthRequired,
    /// A payload could not be encoded or decoded.
    Decode(CodecError),
    /// An opcode arrived in a state that cannot accept it.
    Unexpected {
        /// Numeric opcode.
        op: u8,
    },
}

impl core::fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Closed => formatter.write_str("session is closed"),
            Self::NotIdentified => formatter.write_str("session is not identified"),
            Self::AuthRequired => formatter.write_str("server requires a password"),
            Self::Decode(error) => write!(formatter, "protocol decode error: {error}"),
            Self::Unexpected { op } => write!(formatter, "unexpected opcode {op}"),
        }
    }
}

impl core::error::Error for SessionError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Decode(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    AwaitingHello,
    Identifying,
    Identified,
    Closed,
}

struct Pending {
    deadline_ms: Option<u64>,
    batch: bool,
}

/// Client side of an OBS WebSocket session, without I/O.
pub struct Session<C: Codec = JsonCodec> {
    config: SessionConfig,
    codec: C,
    phase: Phase,
    negotiated_rpc_version: Option<u32>,
    next_id: u64,
    outgoing: VecDeque<Outgoing>,
    events: VecDeque<SessionEvent>,
    pending: BTreeMap<String, Pending>,
}

impl Session<JsonCodec> {
    /// Starts a JSON session that is waiting for `Hello`.
    pub fn json(config: SessionConfig) -> Self {
        Self::new(config, JsonCodec)
    }
}

impl<C: Codec> Session<C> {
    /// Starts a session that is waiting for `Hello`.
    pub fn new(config: SessionConfig, codec: C) -> Self {
        Self {
            config,
            codec,
            phase: Phase::AwaitingHello,
            negotiated_rpc_version: None,
            next_id: 1,
            outgoing: VecDeque::new(),
            events: VecDeque::new(),
            pending: BTreeMap::new(),
        }
    }

    /// RPC version from op 2, once identification has succeeded.
    pub fn negotiated_rpc_version(&self) -> Option<u32> {
        self.negotiated_rpc_version
    }

    /// Whether op 2 has been received and the socket is still open.
    pub fn is_identified(&self) -> bool {
        self.phase == Phase::Identified
    }

    /// Feeds one WebSocket payload (JSON text or MessagePack bytes).
    pub fn handle_message(&mut self, bytes: &[u8]) -> Result<(), SessionError> {
        if self.phase == Phase::Closed {
            return Err(SessionError::Closed);
        }
        let envelope: IncomingEnvelope = self.codec.decode(bytes).map_err(SessionError::Decode)?;
        match envelope.op {
            op::HELLO if self.phase == Phase::AwaitingHello => self.on_hello(envelope.d),
            op::IDENTIFIED if self.phase == Phase::Identifying => self.on_identified(envelope.d),
            op::EVENT if self.phase == Phase::Identified => self.on_event(envelope.d),
            op::REQUEST_RESPONSE if self.phase == Phase::Identified => self.on_response(envelope.d),
            op::REQUEST_BATCH_RESPONSE if self.phase == Phase::Identified => {
                self.on_batch_response(envelope.d)
            }
            op => Err(SessionError::Unexpected { op }),
        }
    }

    /// Records a WebSocket close. Pending requests fail with [`RequestFailure::Closed`].
    pub fn handle_close(&mut self, code: Option<u16>, reason: &str) {
        self.phase = Phase::Closed;
        self.events.push_back(SessionEvent::Closed {
            code: code.map(WebSocketCloseCode::from_u16),
            reason: reason.to_string(),
        });
        self.fail_pending(RequestFailure::Closed);
    }

    /// Fails pending requests whose deadline is at or before `now_ms`.
    ///
    /// The clock is the caller's. Pass the same unit that was used as `deadline_ms`.
    pub fn handle_timeout(&mut self, now_ms: u64) {
        let expired: Vec<String> = self
            .pending
            .iter()
            .filter(|(_, pending)| {
                pending
                    .deadline_ms
                    .is_some_and(|deadline| deadline <= now_ms)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            let Some(pending) = self.pending.remove(&id) else {
                continue;
            };
            self.push_failure(RequestId(id), pending.batch, RequestFailure::Timeout);
        }
    }

    /// Sends a typed request. Legal only after [`SessionEvent::Identified`].
    pub fn send_request<R: Request>(
        &mut self,
        request: &R,
        deadline_ms: Option<u64>,
    ) -> Result<RequestId, SessionError> {
        let data = serde_json::to_value(request)
            .map_err(|error| SessionError::Decode(CodecError::new(error.to_string())))?;
        self.enqueue(R::REQUEST_TYPE, data, deadline_ms, false)
    }

    /// Sends an untyped request on this same session.
    pub fn send_raw(
        &mut self,
        call: &RawCall,
        deadline_ms: Option<u64>,
    ) -> Result<RequestId, SessionError> {
        self.enqueue(
            &call.request_type,
            call.request_data.clone(),
            deadline_ms,
            false,
        )
    }

    /// Sends op 8. `execution_type` is [`crate::RequestBatchExecutionType`] as a number.
    pub fn send_batch(
        &mut self,
        calls: &[RawCall],
        halt_on_failure: bool,
        execution_type: i64,
        deadline_ms: Option<u64>,
    ) -> Result<RequestId, SessionError> {
        self.ensure_identified()?;
        let id = self.alloc_id();
        let message = BatchMessage {
            request_id: id.as_str(),
            halt_on_failure,
            execution_type,
            requests: calls,
        };
        self.queue(op::REQUEST_BATCH, &message)?;
        self.pending.insert(
            id.0.clone(),
            Pending {
                deadline_ms,
                batch: true,
            },
        );
        Ok(id)
    }

    /// Sends op 3 to replace event subscriptions.
    pub fn reidentify(&mut self, event_subscriptions: Option<u32>) -> Result<(), SessionError> {
        self.ensure_identified()?;
        self.config.event_subscriptions = event_subscriptions;
        self.queue(
            op::REIDENTIFY,
            &Reidentify {
                event_subscriptions,
            },
        )
    }

    /// Next frame to write, if the session has produced one.
    pub fn poll_transmit(&mut self) -> Option<Outgoing> {
        self.outgoing.pop_front()
    }

    /// Next event to surface, if the session has produced one.
    pub fn poll_event(&mut self) -> Option<SessionEvent> {
        self.events.pop_front()
    }

    fn on_hello(&mut self, data: Value) -> Result<(), SessionError> {
        let hello: Hello = decode_data(data)?;
        let authentication = if let Some(challenge) = hello.authentication {
            let Some(password) = self
                .config
                .password
                .as_deref()
                .filter(|password| !password.is_empty())
            else {
                self.phase = Phase::Closed;
                self.events.push_back(SessionEvent::Closed {
                    code: Some(WebSocketCloseCode::AuthenticationFailed),
                    reason: "server requires a password".into(),
                });
                return Err(SessionError::AuthRequired);
            };
            Some(authentication_string(
                password,
                &challenge.salt,
                &challenge.challenge,
            ))
        } else {
            None
        };
        let rpc_version = if hello.rpc_version == 0 {
            self.config.rpc_version
        } else {
            self.config.rpc_version.min(hello.rpc_version)
        };
        let identify = Identify {
            rpc_version,
            authentication: authentication.as_deref(),
            event_subscriptions: self.config.event_subscriptions,
        };
        self.queue(op::IDENTIFY, &identify)?;
        self.phase = Phase::Identifying;
        Ok(())
    }

    fn on_identified(&mut self, data: Value) -> Result<(), SessionError> {
        let identified: Identified = decode_data(data)?;
        self.phase = Phase::Identified;
        self.negotiated_rpc_version = Some(identified.negotiated_rpc_version);
        self.events.push_back(SessionEvent::Identified {
            negotiated_rpc_version: identified.negotiated_rpc_version,
        });
        Ok(())
    }

    fn on_event(&mut self, data: Value) -> Result<(), SessionError> {
        let message: EventMessage = decode_data(data)?;
        let event = Event::from_parts(&message.event_type, message.event_data)
            .map_err(|error| SessionError::Decode(CodecError::new(error.to_string())))?;
        self.events.push_back(SessionEvent::Event(event));
        Ok(())
    }

    fn on_response(&mut self, data: Value) -> Result<(), SessionError> {
        let message: ResponseMessage = decode_data(data)?;
        let Some(pending) = self.pending.remove(&message.request_id) else {
            self.events.push_back(SessionEvent::UnmatchedResponse {
                request_id: message.request_id,
            });
            return Ok(());
        };
        if pending.batch {
            self.events.push_back(SessionEvent::UnmatchedResponse {
                request_id: message.request_id,
            });
            return Ok(());
        }
        self.events.push_back(SessionEvent::Response {
            request_id: RequestId(message.request_id),
            request_type: message.request_type,
            result: status_result(&message.request_status, message.response_data),
        });
        Ok(())
    }

    fn on_batch_response(&mut self, data: Value) -> Result<(), SessionError> {
        let message: BatchResponseMessage = decode_data(data)?;
        let Some(pending) = self.pending.remove(&message.request_id) else {
            self.events.push_back(SessionEvent::UnmatchedResponse {
                request_id: message.request_id,
            });
            return Ok(());
        };
        if !pending.batch {
            self.events.push_back(SessionEvent::UnmatchedResponse {
                request_id: message.request_id,
            });
            return Ok(());
        }
        let results = message
            .results
            .into_iter()
            .map(|item| BatchItemResult {
                request_type: item.request_type,
                result: status_result(&item.request_status, item.response_data),
            })
            .collect();
        self.events.push_back(SessionEvent::BatchResponse {
            request_id: RequestId(message.request_id),
            results,
        });
        Ok(())
    }

    fn enqueue(
        &mut self,
        request_type: &str,
        data: Value,
        deadline_ms: Option<u64>,
        batch: bool,
    ) -> Result<RequestId, SessionError> {
        self.ensure_identified()?;
        let id = self.alloc_id();
        let message = RequestMessage {
            request_type,
            request_id: id.as_str(),
            request_data: if data.is_null() { None } else { Some(&data) },
        };
        self.queue(op::REQUEST, &message)?;
        self.pending
            .insert(id.0.clone(), Pending { deadline_ms, batch });
        Ok(id)
    }

    fn ensure_identified(&self) -> Result<(), SessionError> {
        match self.phase {
            Phase::Identified => Ok(()),
            Phase::Closed => Err(SessionError::Closed),
            Phase::AwaitingHello | Phase::Identifying => Err(SessionError::NotIdentified),
        }
    }

    fn alloc_id(&mut self) -> RequestId {
        let id = RequestId(self.next_id.to_string());
        self.next_id += 1;
        id
    }

    fn queue<T: Serialize>(&mut self, op: u8, data: &T) -> Result<(), SessionError> {
        let payload = self
            .codec
            .encode(&Envelope { op, d: data })
            .map_err(SessionError::Decode)?;
        self.outgoing.push_back(Outgoing {
            payload,
            binary: self.codec.is_binary(),
        });
        Ok(())
    }

    fn fail_pending(&mut self, failure: RequestFailure) {
        let pending = core::mem::take(&mut self.pending);
        for (id, pending) in pending {
            self.push_failure(RequestId(id), pending.batch, failure.clone());
        }
    }

    fn push_failure(&mut self, request_id: RequestId, batch: bool, failure: RequestFailure) {
        if batch {
            self.events.push_back(SessionEvent::BatchResponse {
                request_id,
                results: alloc::vec![BatchItemResult {
                    request_type: String::new(),
                    result: Err(failure),
                }],
            });
        } else {
            self.events.push_back(SessionEvent::Response {
                request_id,
                request_type: String::new(),
                result: Err(failure),
            });
        }
    }
}

fn decode_data<T: serde::de::DeserializeOwned>(data: Value) -> Result<T, SessionError> {
    serde_json::from_value(data)
        .map_err(|error| SessionError::Decode(CodecError::new(error.to_string())))
}

fn status_result(
    status: &crate::protocol::RequestStatusBody,
    data: Option<Value>,
) -> Result<Value, RequestFailure> {
    if status.result {
        Ok(data.unwrap_or_else(|| Value::Object(serde_json::Map::new())))
    } else {
        Err(RequestFailure::Status {
            code: status.code,
            comment: status.comment.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::WebSocketCloseCode;

    fn session(password: Option<&str>) -> Session {
        Session::json(
            SessionConfig::default()
                .password(password)
                .with_subscriptions(1),
        )
    }

    impl SessionConfig {
        fn with_subscriptions(mut self, bits: u32) -> Self {
            self.event_subscriptions = Some(bits);
            self
        }
    }

    fn feed(session: &mut Session, json: &str) {
        session.handle_message(json.as_bytes()).unwrap();
    }

    fn take_json(session: &mut Session) -> Value {
        let outgoing = session.poll_transmit().expect("outgoing frame");
        assert!(!outgoing.binary);
        serde_json::from_slice(&outgoing.payload).unwrap()
    }

    const HELLO_AUTH: &str = r#"{"op":0,"d":{"obsWebSocketVersion":"5.7.4","rpcVersion":1,"authentication":{"challenge":"+IxH4CnCiqpX1rM9scsNynZzbOe4KhDeYcTNS3PDaeY=","salt":"lM1GncleQOaCu9lT1yeUZhFYnqhsLLP1G5lAGo3ixaI="}}}"#;
    const HELLO_OPEN: &str = r#"{"op":0,"d":{"obsWebSocketVersion":"5.7.4","rpcVersion":1}}"#;
    const IDENTIFIED: &str = r#"{"op":2,"d":{"negotiatedRpcVersion":1}}"#;

    fn identify(session: &mut Session) {
        feed(session, HELLO_OPEN);
        let identify = take_json(session);
        assert_eq!(identify["op"], 1);
        assert!(identify["d"].get("authentication").is_none());
        feed(session, IDENTIFIED);
        assert!(matches!(
            session.poll_event(),
            Some(SessionEvent::Identified {
                negotiated_rpc_version: 1
            })
        ));
    }

    #[test]
    fn hello_with_password_sends_authentication() {
        let mut session = session(Some("supersecretpassword"));
        feed(&mut session, HELLO_AUTH);
        let identify = take_json(&mut session);
        assert_eq!(
            identify["d"]["authentication"],
            "1Ct943GAT+6YQUUX47Ia/ncufilbe6+oD6lY+5kaCu4="
        );
        assert_eq!(identify["d"]["eventSubscriptions"], 1);
        assert_eq!(identify["d"]["rpcVersion"], 1);
    }

    #[test]
    fn missing_password_fails_before_identify() {
        let mut session = session(None);
        let error = session.handle_message(HELLO_AUTH.as_bytes()).unwrap_err();
        assert_eq!(error, SessionError::AuthRequired);
        assert!(session.poll_transmit().is_none());
        assert!(matches!(
            session.poll_event(),
            Some(SessionEvent::Closed {
                code: Some(WebSocketCloseCode::AuthenticationFailed),
                ..
            })
        ));
    }

    #[test]
    fn negotiates_rpc_down_to_server() {
        let mut session = Session::json(SessionConfig {
            rpc_version: 9,
            ..SessionConfig::default()
        });
        feed(&mut session, HELLO_OPEN);
        let identify = take_json(&mut session);
        assert_eq!(identify["d"]["rpcVersion"], 1);
    }

    #[test]
    fn request_response_and_unmatched_id() {
        let mut session = session(None);
        identify(&mut session);
        let id = session
            .send_raw(&RawCall::new("GetVersion", Value::Null), Some(1_000))
            .unwrap();
        let request = take_json(&mut session);
        assert_eq!(request["op"], 6);
        assert_eq!(request["d"]["requestType"], "GetVersion");
        assert_eq!(request["d"]["requestId"], id.as_str());
        assert!(request["d"].get("requestData").is_none());

        feed(
            &mut session,
            r#"{"op":7,"d":{"requestType":"GetVersion","requestId":"nope","requestStatus":{"result":true,"code":100}}}"#,
        );
        assert!(matches!(
            session.poll_event(),
            Some(SessionEvent::UnmatchedResponse { .. })
        ));

        let body = alloc::format!(
            r#"{{"op":7,"d":{{"requestType":"GetVersion","requestId":"{}","requestStatus":{{"result":true,"code":100}},"responseData":{{"obsVersion":"30.2.0"}}}}}}"#,
            id.as_str()
        );
        feed(&mut session, &body);
        match session.poll_event() {
            Some(SessionEvent::Response {
                request_id, result, ..
            }) => {
                assert_eq!(request_id, id);
                assert_eq!(result.unwrap()["obsVersion"], "30.2.0");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn failed_request_keeps_status_code() {
        let mut session = session(None);
        identify(&mut session);
        let id = session
            .send_raw(
                &RawCall::new(
                    "SetCurrentProgramScene",
                    serde_json::json!({"sceneName": "Missing"}),
                ),
                None,
            )
            .unwrap();
        let _ = take_json(&mut session);
        let body = alloc::format!(
            r#"{{"op":7,"d":{{"requestType":"SetCurrentProgramScene","requestId":"{}","requestStatus":{{"result":false,"code":600,"comment":"missing"}}}}}}"#,
            id.as_str()
        );
        feed(&mut session, &body);
        match session.poll_event() {
            Some(SessionEvent::Response {
                result: Err(RequestFailure::Status { code, comment }),
                ..
            }) => {
                assert_eq!(code, 600);
                assert_eq!(comment.as_deref(), Some("missing"));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn timeout_then_late_response_is_unmatched() {
        let mut session = session(None);
        identify(&mut session);
        let id = session
            .send_raw(
                &RawCall::new("Sleep", serde_json::json!({"sleepMillis": 1})),
                Some(50),
            )
            .unwrap();
        let _ = take_json(&mut session);
        session.handle_timeout(49);
        assert!(session.poll_event().is_none());
        session.handle_timeout(50);
        assert!(matches!(
            session.poll_event(),
            Some(SessionEvent::Response {
                result: Err(RequestFailure::Timeout),
                ..
            })
        ));
        let body = alloc::format!(
            r#"{{"op":7,"d":{{"requestType":"Sleep","requestId":"{}","requestStatus":{{"result":true,"code":100}}}}}}"#,
            id.as_str()
        );
        feed(&mut session, &body);
        assert!(matches!(
            session.poll_event(),
            Some(SessionEvent::UnmatchedResponse { .. })
        ));
    }

    #[test]
    fn batch_and_reidentify_and_unknown_event() {
        let mut session = session(None);
        identify(&mut session);
        let id = session
            .send_batch(
                &[
                    RawCall::new("GetVersion", Value::Null),
                    RawCall::new("Sleep", serde_json::json!({"sleepMillis": 0})),
                ],
                true,
                0,
                None,
            )
            .unwrap();
        let batch = take_json(&mut session);
        assert_eq!(batch["op"], 8);
        assert_eq!(batch["d"]["haltOnFailure"], true);
        assert_eq!(batch["d"]["executionType"], 0);
        assert_eq!(batch["d"]["requests"][0]["requestType"], "GetVersion");
        assert!(batch["d"]["requests"][0].get("requestData").is_none());
        assert_eq!(batch["d"]["requests"][1]["requestData"]["sleepMillis"], 0);

        let body = alloc::format!(
            r#"{{"op":9,"d":{{"requestId":"{}","results":[{{"requestType":"GetVersion","requestStatus":{{"result":true,"code":100}},"responseData":{{}}}},{{"requestType":"Sleep","requestStatus":{{"result":false,"code":207,"comment":"not ready"}}}}]}}}}"#,
            id.as_str()
        );
        feed(&mut session, &body);
        match session.poll_event() {
            Some(SessionEvent::BatchResponse { results, .. }) => {
                assert!(results[0].result.is_ok());
                assert!(matches!(
                    &results[1].result,
                    Err(RequestFailure::Status { code: 207, .. })
                ));
            }
            other => panic!("unexpected {other:?}"),
        }

        session.reidentify(Some(0)).unwrap();
        let reidentify = take_json(&mut session);
        assert_eq!(reidentify["op"], 3);
        assert_eq!(reidentify["d"]["eventSubscriptions"], 0);

        feed(
            &mut session,
            r#"{"op":5,"d":{"eventType":"VendorSpecific","eventIntent":512,"eventData":{"k":1}}}"#,
        );
        match session.poll_event() {
            Some(SessionEvent::Event(Event::Unknown {
                event_type,
                event_data,
            })) => {
                assert_eq!(event_type, "VendorSpecific");
                assert_eq!(event_data["k"], 1);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn close_during_request_is_authentication_failure() {
        let mut session = session(None);
        identify(&mut session);
        let _ = session
            .send_raw(&RawCall::new("GetVersion", Value::Null), None)
            .unwrap();
        let _ = take_json(&mut session);
        session.handle_close(Some(4009), "Authentication failed.");
        assert!(matches!(
            session.poll_event(),
            Some(SessionEvent::Closed {
                code: Some(WebSocketCloseCode::AuthenticationFailed),
                ..
            })
        ));
        assert!(matches!(
            session.poll_event(),
            Some(SessionEvent::Response {
                result: Err(RequestFailure::Closed),
                ..
            })
        ));
        assert_eq!(
            session.handle_message(br#"{"op":5,"d":{"eventType":"ExitStarted"}}"#),
            Err(SessionError::Closed)
        );
    }

    #[test]
    fn request_before_identify_is_rejected() {
        let mut session = session(None);
        let error = session
            .send_raw(&RawCall::new("GetVersion", Value::Null), None)
            .unwrap_err();
        assert_eq!(error, SessionError::NotIdentified);
    }

    #[test]
    fn typed_and_raw_share_the_request_id_space() {
        let mut session = session(None);
        identify(&mut session);
        let first = session
            .send_request(&crate::requests::GetVersion::new(), None)
            .unwrap();
        let _ = take_json(&mut session);
        let second = session
            .send_raw(
                &RawCall::new("BroadcastCustomEvent", serde_json::json!({"eventData": {}})),
                None,
            )
            .unwrap();
        let _ = take_json(&mut session);
        assert_eq!(first.as_str(), "1");
        assert_eq!(second.as_str(), "2");
    }
}
