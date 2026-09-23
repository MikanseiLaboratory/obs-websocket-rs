//! Scriptable OBS WebSocket v5 server for tests.
//!
//! The server speaks the same JSON subprotocol as OBS: op 0 through op 9.
//! Password checks use [`obs_websocket_core::authentication_string`].

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use obs_websocket_core::authentication_string;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::{WebSocketStream, accept_hdr_async};

/// Salt from the obs-websocket v5 protocol documentation.
pub const SALT: &str = "lM1GncleQOaCu9lT1yeUZhFYnqhsLLP1G5lAGo3ixaI=";
/// Challenge from the obs-websocket v5 protocol documentation.
pub const CHALLENGE: &str = "+IxH4CnCiqpX1rM9scsNynZzbOe4KhDeYcTNS3PDaeY=";

const SUBPROTOCOL: &str = "obswebsocket.json";

/// How the mock answers one `requestType`.
#[derive(Debug, Clone)]
pub enum Reply {
    /// `requestStatus.result = true` and this `responseData`.
    Success(Value),
    /// `requestStatus.result = false`.
    Failure {
        /// `requestStatus.code`.
        code: i64,
        /// `requestStatus.comment`.
        comment: Option<String>,
    },
    /// Never answers, so the client deadline can fire.
    Hang,
    /// Waits, then applies `reply`.
    Delay {
        /// How long to wait before answering.
        after: Duration,
        /// The reply sent after the wait.
        reply: Box<Reply>,
    },
}

/// Listener configuration.
#[derive(Debug, Clone)]
pub struct MockConfig {
    /// When set, `Hello` includes a challenge and `Identify` must match it.
    pub password: Option<String>,
    /// RPC version advertised in `Hello`.
    pub rpc_version: u32,
    /// `obsVersion` in the default `GetVersion` response.
    pub obs_version: String,
    /// `obsWebSocketVersion` in `Hello` and `GetVersion`.
    pub obs_web_socket_version: String,
    /// `availableRequests` in the default `GetVersion` response.
    pub available_requests: Vec<String>,
}

impl Default for MockConfig {
    fn default() -> Self {
        Self {
            password: None,
            rpc_version: 1,
            obs_version: "31.0.0".to_string(),
            obs_web_socket_version: "5.7.4".to_string(),
            available_requests: vec!["GetVersion".to_string()],
        }
    }
}

impl MockConfig {
    /// Requires `password` on `Identify`.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }
}

#[derive(Debug, Clone)]
enum Notice {
    Event {
        event_type: String,
        event_data: Value,
    },
    Disconnect,
    Shutdown,
}

struct State {
    config: MockConfig,
    replies: Mutex<HashMap<String, Reply>>,
    identifications: AtomicUsize,
    event_subscriptions: Mutex<Option<u32>>,
}

/// A fake OBS server bound to `127.0.0.1`.
pub struct MockObs {
    local_addr: SocketAddr,
    state: Arc<State>,
    notices: broadcast::Sender<Notice>,
    accept_task: JoinHandle<()>,
}

impl MockObs {
    /// Binds `127.0.0.1:0` and accepts connections until dropped.
    pub async fn spawn(config: MockConfig) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        let (notices, _) = broadcast::channel(64);
        let state = Arc::new(State {
            config,
            replies: Mutex::new(HashMap::new()),
            identifications: AtomicUsize::new(0),
            event_subscriptions: Mutex::new(None),
        });
        let accept_task = tokio::spawn(accept_loop(listener, Arc::clone(&state), notices.clone()));
        Ok(Self {
            local_addr,
            state,
            notices,
            accept_task,
        })
    }

    /// `ws://127.0.0.1:<port>`.
    pub fn url(&self) -> String {
        format!("ws://{}", self.local_addr)
    }

    /// Bound address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Pushes an op 5 event to every identified connection.
    pub fn emit(&self, event_type: impl Into<String>, event_data: Value) {
        let _ = self.notices.send(Notice::Event {
            event_type: event_type.into(),
            event_data,
        });
    }

    /// Closes current sockets. New connections are still accepted.
    pub fn disconnect(&self) {
        let _ = self.notices.send(Notice::Disconnect);
    }

    /// Replaces the scripted reply for `request_type`.
    pub fn set_reply(&self, request_type: impl Into<String>, reply: Reply) {
        self.state
            .replies
            .lock()
            .expect("reply map")
            .insert(request_type.into(), reply);
    }

    /// How many sessions have completed `Identify`.
    pub fn identifications(&self) -> usize {
        self.state.identifications.load(Ordering::SeqCst)
    }

    /// `eventSubscriptions` from the most recent `Identify`.
    pub fn event_subscriptions(&self) -> Option<u32> {
        *self
            .state
            .event_subscriptions
            .lock()
            .expect("subscriptions")
    }

    /// Runs the OBS session on a WebSocket that is already open.
    ///
    /// The `wss` tests terminate TLS first, then hand the socket here.
    pub async fn serve<S>(&self, socket: WebSocketStream<S>)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    {
        drive(socket, Arc::clone(&self.state), self.notices.subscribe()).await;
    }
}

impl Drop for MockObs {
    fn drop(&mut self) {
        let _ = self.notices.send(Notice::Shutdown);
        self.accept_task.abort();
    }
}

async fn accept_loop(listener: TcpListener, state: Arc<State>, notices: broadcast::Sender<Notice>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let state = Arc::clone(&state);
        let notices = notices.subscribe();
        tokio::spawn(async move {
            if let Ok(socket) = accept_hdr_async(stream, select_subprotocol).await {
                drive(socket, state, notices).await;
            }
        });
    }
}

#[allow(clippy::result_large_err)]
fn select_subprotocol(
    request: &Request,
    mut response: Response,
) -> Result<Response, tokio_tungstenite::tungstenite::handshake::server::ErrorResponse> {
    let offered = request
        .headers()
        .get("Sec-WebSocket-Protocol")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.split(',').any(|part| part.trim() == SUBPROTOCOL));
    if offered {
        response.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_static(SUBPROTOCOL),
        );
    }
    Ok(response)
}

async fn drive<S>(
    mut socket: WebSocketStream<S>,
    state: Arc<State>,
    mut notices: broadcast::Receiver<Notice>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    if socket
        .send(Message::text(hello_json(&state.config)))
        .await
        .is_err()
    {
        return;
    }
    let Some(Ok(Message::Text(identify))) = socket.next().await else {
        return;
    };
    if !accept_identify(&state, identify.as_str()) {
        let _ = socket
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::from(4009u16),
                reason: "Authentication failed".into(),
            })))
            .await;
        return;
    }
    let identified = json!({
        "op": 2,
        "d": { "negotiatedRpcVersion": state.config.rpc_version }
    });
    if socket
        .send(Message::text(identified.to_string()))
        .await
        .is_err()
    {
        return;
    }
    state.identifications.fetch_add(1, Ordering::SeqCst);

    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<String>();
    loop {
        tokio::select! {
            incoming = socket.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        handle_client_text(text.as_str(), &state, &outgoing_tx);
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
            notice = notices.recv() => {
                match notice {
                    Ok(Notice::Event { event_type, event_data }) => {
                        let message = json!({
                            "op": 5,
                            "d": {
                                "eventType": event_type,
                                "eventIntent": 1,
                                "eventData": event_data,
                            }
                        });
                        if socket.send(Message::text(message.to_string())).await.is_err() {
                            break;
                        }
                    }
                    Ok(Notice::Disconnect | Notice::Shutdown) | Err(_) => break,
                }
            }
            outgoing = outgoing_rx.recv() => {
                let Some(payload) = outgoing else { break };
                if socket.send(Message::text(payload)).await.is_err() {
                    break;
                }
            }
        }
    }
}

fn hello_json(config: &MockConfig) -> String {
    let mut data = json!({
        "obsWebSocketVersion": config.obs_web_socket_version,
        "rpcVersion": config.rpc_version,
    });
    if config.password.is_some() {
        data["authentication"] = json!({
            "challenge": CHALLENGE,
            "salt": SALT,
        });
    }
    json!({ "op": 0, "d": data }).to_string()
}

fn accept_identify(state: &State, text: &str) -> bool {
    let Ok(envelope) = serde_json::from_str::<Envelope>(text) else {
        return false;
    };
    if envelope.op != 1 {
        return false;
    }
    let Ok(identify) = serde_json::from_value::<IdentifyBody>(envelope.d) else {
        return false;
    };
    *state.event_subscriptions.lock().expect("subscriptions") = identify.event_subscriptions;
    let Some(password) = state.config.password.as_deref() else {
        return true;
    };
    let expected = authentication_string(password, SALT, CHALLENGE);
    identify.authentication.as_deref() == Some(expected.as_str())
}

fn handle_client_text(text: &str, state: &Arc<State>, outgoing: &mpsc::UnboundedSender<String>) {
    let Ok(envelope) = serde_json::from_str::<Envelope>(text) else {
        return;
    };
    match envelope.op {
        6 => {
            let Ok(request) = serde_json::from_value::<RequestBody>(envelope.d) else {
                return;
            };
            let reply = lookup_reply(state, &request.request_type);
            let outgoing = outgoing.clone();
            tokio::spawn(async move {
                if let Some(payload) = render_single(&request, reply).await {
                    let _ = outgoing.send(payload);
                }
            });
        }
        8 => {
            let Ok(batch) = serde_json::from_value::<BatchBody>(envelope.d) else {
                return;
            };
            let replies: Vec<Reply> = batch
                .requests
                .iter()
                .map(|request| lookup_reply(state, &request.request_type))
                .collect();
            let outgoing = outgoing.clone();
            tokio::spawn(async move {
                if let Some(payload) = render_batch(&batch, replies).await {
                    let _ = outgoing.send(payload);
                }
            });
        }
        _ => {}
    }
}

fn lookup_reply(state: &State, request_type: &str) -> Reply {
    if let Some(reply) = state
        .replies
        .lock()
        .expect("reply map")
        .get(request_type)
        .cloned()
    {
        return reply;
    }
    if request_type == "GetVersion" {
        return Reply::Success(json!({
            "obsVersion": state.config.obs_version,
            "obsWebSocketVersion": state.config.obs_web_socket_version,
            "rpcVersion": state.config.rpc_version,
            "availableRequests": state.config.available_requests,
            "supportedImageFormats": ["png"],
            "platform": "test",
            "platformDescription": "obs-websocket-mock",
        }));
    }
    Reply::Success(json!({}))
}

async fn render_single(request: &RequestBody, reply: Reply) -> Option<String> {
    let body = render_item(&request.request_type, reply).await?;
    Some(
        json!({
            "op": 7,
            "d": {
                "requestType": body.request_type,
                "requestId": request.request_id,
                "requestStatus": body.status,
                "responseData": body.data,
            }
        })
        .to_string(),
    )
}

async fn render_batch(batch: &BatchBody, replies: Vec<Reply>) -> Option<String> {
    let mut results = Vec::new();
    for (request, reply) in batch.requests.iter().zip(replies) {
        let item = render_item(&request.request_type, reply).await?;
        let failed = item.status["result"] == false;
        results.push(json!({
            "requestType": item.request_type,
            "requestStatus": item.status,
            "responseData": item.data,
        }));
        if failed && batch.halt_on_failure {
            break;
        }
    }
    Some(
        json!({
            "op": 9,
            "d": {
                "requestId": batch.request_id,
                "results": results,
            }
        })
        .to_string(),
    )
}

struct Rendered {
    request_type: String,
    status: Value,
    data: Value,
}

async fn render_item(request_type: &str, reply: Reply) -> Option<Rendered> {
    match reply {
        Reply::Hang => None,
        Reply::Delay { after, reply } => {
            tokio::time::sleep(after).await;
            Box::pin(render_item(request_type, *reply)).await
        }
        Reply::Success(data) => Some(Rendered {
            request_type: request_type.to_string(),
            status: json!({ "result": true, "code": 100 }),
            data,
        }),
        Reply::Failure { code, comment } => {
            let mut status = json!({ "result": false, "code": code });
            if let Some(comment) = comment {
                status["comment"] = Value::String(comment);
            }
            Some(Rendered {
                request_type: request_type.to_string(),
                status,
                data: json!({}),
            })
        }
    }
}

#[derive(Debug, Deserialize)]
struct Envelope {
    op: u8,
    d: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IdentifyBody {
    authentication: Option<String>,
    #[serde(default)]
    event_subscriptions: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RequestBody {
    request_type: String,
    request_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BatchBody {
    request_id: String,
    #[serde(default)]
    halt_on_failure: bool,
    requests: Vec<CallBody>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CallBody {
    request_type: String,
}
