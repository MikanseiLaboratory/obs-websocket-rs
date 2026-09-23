//! Single-task driver. It owns the socket and the [`Session`](obs_websocket_core::Session).

use std::collections::HashMap;
use std::time::Duration;

use obs_websocket_core::{
    BatchItemResult, RawCall, Request, RequestFailure, RequestId, Session, SessionConfig,
    SessionEvent, WebSocketCloseCode,
};
use obs_websocket_io::{Frame, Timer, Transport};
use obs_websocket_tokio::{TokioTimer, TokioTransport, TransportError};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use crate::client::{Command, ConnectionState, Inner};
use crate::config::ConnectConfig;
use crate::error::Error;

enum Waiter {
    Call(oneshot::Sender<Result<Value, Error>>),
    Batch(oneshot::Sender<Result<Vec<BatchItemResult>, Error>>),
}

struct Pending {
    waiter: Waiter,
    deadline_ms: u64,
}

struct Pump {
    transport: TokioTransport,
    session: Session,
    timer: TokioTimer,
    request_timeout_ms: u64,
    pending: HashMap<String, Pending>,
}

pub(crate) async fn run(
    config: ConnectConfig,
    mut commands: mpsc::UnboundedReceiver<Command>,
    shared: std::sync::Arc<Inner>,
    ready: oneshot::Sender<Result<(), Error>>,
) {
    let mut session_config = SessionConfig {
        password: config.password.clone(),
        event_subscriptions: config.event_subscriptions,
        rpc_version: 1,
    };
    let mut ready = Some(ready);
    let mut attempt = 0u32;
    loop {
        match serve(
            &config,
            &mut session_config,
            &mut commands,
            &shared,
            &mut ready,
        )
        .await
        {
            Outcome::Shutdown => return,
            Outcome::Disconnected(error) if matches!(error, Error::AuthFailed) => {
                if let Some(ready) = ready.take() {
                    let _ = ready.send(Err(Error::AuthFailed));
                }
                publish(
                    &shared,
                    ConnectionState::Closed {
                        code: Some(WebSocketCloseCode::AuthenticationFailed.as_u16()),
                        reason: error.to_string(),
                    },
                );
                return;
            }
            Outcome::Disconnected(error) if ready.is_some() => {
                if let Some(ready) = ready.take() {
                    let _ = ready.send(Err(error.clone()));
                }
                publish(
                    &shared,
                    ConnectionState::Closed {
                        code: None,
                        reason: error.to_string(),
                    },
                );
                return;
            }
            Outcome::Disconnected(error) => {
                if !config.reconnect.enabled {
                    publish_closed(&shared, &error);
                    return;
                }
                attempt = attempt.saturating_add(1);
                if config
                    .reconnect
                    .max_attempts
                    .is_some_and(|max| attempt > max)
                {
                    publish_closed(&shared, &error);
                    return;
                }
                publish(&shared, ConnectionState::Reconnecting { attempt });
                tokio::time::sleep(config.reconnect.delay(attempt)).await;
            }
        }
    }
}

enum Outcome {
    Shutdown,
    Disconnected(Error),
}

async fn serve(
    config: &ConnectConfig,
    session_config: &mut SessionConfig,
    commands: &mut mpsc::UnboundedReceiver<Command>,
    shared: &std::sync::Arc<Inner>,
    ready: &mut Option<oneshot::Sender<Result<(), Error>>>,
) -> Outcome {
    #[cfg(not(feature = "rustls"))]
    if config.tls {
        return Outcome::Disconnected(Error::TlsUnavailable);
    }
    let transport = match connect_transport(config).await {
        Ok(transport) => transport,
        Err(error) => return Outcome::Disconnected(error),
    };
    let mut pump = Pump {
        transport,
        session: Session::json(session_config.clone()),
        timer: TokioTimer::new(),
        request_timeout_ms: duration_ms(config.request_timeout),
        pending: HashMap::new(),
    };
    let deadline = pump
        .timer
        .now_ms()
        .saturating_add(duration_ms(config.connect_timeout).max(pump.request_timeout_ms));
    if let Err(error) = pump.until_identified(deadline).await {
        return Outcome::Disconnected(error);
    }
    if let Err(error) = record_version(&mut pump, shared).await {
        return Outcome::Disconnected(error);
    }
    #[cfg(feature = "state")]
    if let Err(error) = snapshot(&mut pump, shared).await {
        return Outcome::Disconnected(error);
    }
    publish(shared, ConnectionState::Connected);
    if let Some(ready) = ready.take() {
        let _ = ready.send(Ok(()));
    }
    match pump.command_loop(commands, session_config, shared).await {
        LoopEnd::Shutdown => Outcome::Shutdown,
        LoopEnd::Disconnected(error) => Outcome::Disconnected(error),
    }
}

async fn connect_transport(config: &ConnectConfig) -> Result<TokioTransport, Error> {
    TokioTransport::connect(&config.url(), config.connect_timeout)
        .await
        .map_err(map_transport)
}

fn map_transport(error: TransportError) -> Error {
    match error {
        TransportError::Timeout => Error::Timeout,
        other => Error::Unreachable(other.to_string()),
    }
}

async fn record_version(pump: &mut Pump, shared: &Inner) -> Result<(), Error> {
    match pump
        .call(&obs_websocket_core::requests::GetVersion::new(), shared)
        .await
    {
        Ok(version) if !version.available_requests.is_empty() => {
            *shared.available.lock().expect("available requests") =
                Some(version.available_requests.into_iter().collect());
        }
        Ok(_) => {
            *shared.available.lock().expect("available requests") = None;
        }
        Err(error) if connection_failed(&error) => return Err(error),
        Err(_) => {
            *shared.available.lock().expect("available requests") = None;
        }
    }
    Ok(())
}

fn connection_failed(error: &Error) -> bool {
    matches!(
        error,
        Error::AuthFailed
            | Error::Closed { .. }
            | Error::Disconnected
            | Error::Unreachable(_)
            | Error::TlsUnavailable
    )
}

#[cfg(feature = "state")]
async fn snapshot(pump: &mut Pump, shared: &Inner) -> Result<(), Error> {
    use obs_websocket_core::requests::{
        GetCurrentProgramScene, GetInputList, GetRecordStatus, GetStreamStatus,
        GetStudioModeEnabled, GetVirtualCamStatus,
    };
    if let Some(scene) = tolerate(pump.call(&GetCurrentProgramScene::new(), shared).await)? {
        let mut state = shared.obs_state.lock().expect("state");
        state.program_scene = Some(scene.scene_name);
        state.program_scene_uuid = Some(scene.scene_uuid);
    }
    if let Some(studio) = tolerate(pump.call(&GetStudioModeEnabled::new(), shared).await)? {
        shared.obs_state.lock().expect("state").studio_mode = Some(studio.studio_mode_enabled);
    }
    if let Some(inputs) = tolerate(pump.call(&GetInputList::new(), shared).await)? {
        let calls: Vec<RawCall> = inputs
            .inputs
            .iter()
            .flat_map(|input| {
                [
                    RawCall::new(
                        "GetInputMute",
                        serde_json::json!({ "inputName": input.input_name }),
                    ),
                    RawCall::new(
                        "GetInputVolume",
                        serde_json::json!({ "inputName": input.input_name }),
                    ),
                ]
            })
            .collect();
        let names: Vec<String> = inputs
            .inputs
            .into_iter()
            .map(|input| input.input_name)
            .collect();
        {
            let mut state = shared.obs_state.lock().expect("state");
            for name in &names {
                state.inputs.entry(name.clone()).or_default();
            }
        }
        if !calls.is_empty() {
            if let Some(results) = tolerate(pump.call_batch(&calls, false, 0, shared).await)? {
                let mut state = shared.obs_state.lock().expect("state");
                for (index, name) in names.iter().enumerate() {
                    let input = state.inputs.entry(name.clone()).or_default();
                    if let Some(item) = results.get(index * 2) {
                        if let Ok(value) = &item.result {
                            if let Some(muted) = value.get("inputMuted").and_then(Value::as_bool) {
                                input.muted = Some(muted);
                            }
                        }
                    }
                    if let Some(item) = results.get(index * 2 + 1) {
                        if let Ok(value) = &item.result {
                            input.volume_mul = value.get("inputVolumeMul").and_then(Value::as_f64);
                            input.volume_db = value.get("inputVolumeDb").and_then(Value::as_f64);
                        }
                    }
                }
            }
        }
    }
    if let Some(stream) = tolerate(pump.call(&GetStreamStatus::new(), shared).await)? {
        shared.obs_state.lock().expect("state").streaming = Some(stream.output_active);
    }
    if let Some(record) = tolerate(pump.call(&GetRecordStatus::new(), shared).await)? {
        shared.obs_state.lock().expect("state").recording = Some(record.output_active);
    }
    if let Some(camera) = tolerate(pump.call(&GetVirtualCamStatus::new(), shared).await)? {
        shared.obs_state.lock().expect("state").virtual_cam = Some(camera.output_active);
    }
    Ok(())
}

#[cfg(feature = "state")]
fn tolerate<T>(result: Result<T, Error>) -> Result<Option<T>, Error> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if connection_failed(&error) => Err(error),
        Err(_) => Ok(None),
    }
}

enum LoopEnd {
    Shutdown,
    Disconnected(Error),
}

impl Pump {
    async fn until_identified(&mut self, deadline: u64) -> Result<(), Error> {
        loop {
            if let Some(error) = self.take_terminal() {
                return Err(error);
            }
            if self.session.negotiated_rpc_version().is_some() {
                return Ok(());
            }
            let now = self.timer.now_ms();
            if now >= deadline {
                return Err(Error::Timeout);
            }
            self.drive_once(deadline - now).await?;
        }
    }

    async fn call<R: Request>(
        &mut self,
        request: &R,
        shared: &Inner,
    ) -> Result<R::Response, Error> {
        let (tx, rx) = oneshot::channel();
        self.enqueue_call(
            R::REQUEST_TYPE,
            &serde_json::to_value(request).unwrap_or(Value::Null),
            tx,
        )?;
        let value = self.wait_reply(rx, shared).await?;
        serde_json::from_value(value).map_err(|error| {
            Error::Protocol(obs_websocket_core::SessionError::Decode(
                obs_websocket_core::CodecError::new(error.to_string()),
            ))
        })
    }

    #[cfg(feature = "state")]
    async fn call_batch(
        &mut self,
        calls: &[RawCall],
        halt_on_failure: bool,
        execution_type: i64,
        shared: &Inner,
    ) -> Result<Vec<BatchItemResult>, Error> {
        let (tx, rx) = oneshot::channel();
        let deadline = self.arm_deadline();
        let id = self
            .session
            .send_batch(calls, halt_on_failure, execution_type, Some(deadline))
            .map_err(Error::Protocol)?;
        self.pending.insert(
            id.as_str().to_string(),
            Pending {
                waiter: Waiter::Batch(tx),
                deadline_ms: deadline,
            },
        );
        self.wait_batch_reply(rx, shared).await
    }

    fn enqueue_call(
        &mut self,
        request_type: &str,
        data: &Value,
        reply: oneshot::Sender<Result<Value, Error>>,
    ) -> Result<(), Error> {
        let deadline = self.arm_deadline();
        let call = RawCall::new(request_type, data.clone());
        let id = self
            .session
            .send_raw(&call, Some(deadline))
            .map_err(Error::Protocol)?;
        self.pending.insert(
            id.as_str().to_string(),
            Pending {
                waiter: Waiter::Call(reply),
                deadline_ms: deadline,
            },
        );
        Ok(())
    }

    async fn wait_reply(
        &mut self,
        mut reply: oneshot::Receiver<Result<Value, Error>>,
        shared: &Inner,
    ) -> Result<Value, Error> {
        loop {
            tokio::select! {
                result = &mut reply => {
                    return result.map_err(|_| Error::Disconnected)?;
                }
                step = self.drive_once(self.request_timeout_ms.max(1)) => {
                    step?;
                    self.dispatch(shared)?;
                }
            }
        }
    }

    #[cfg(feature = "state")]
    async fn wait_batch_reply(
        &mut self,
        mut reply: oneshot::Receiver<Result<Vec<BatchItemResult>, Error>>,
        shared: &Inner,
    ) -> Result<Vec<BatchItemResult>, Error> {
        loop {
            tokio::select! {
                result = &mut reply => {
                    return result.map_err(|_| Error::Disconnected)?;
                }
                step = self.drive_once(self.request_timeout_ms.max(1)) => {
                    step?;
                    self.dispatch(shared)?;
                }
            }
        }
    }

    async fn command_loop(
        &mut self,
        commands: &mut mpsc::UnboundedReceiver<Command>,
        session_config: &mut SessionConfig,
        shared: &Inner,
    ) -> LoopEnd {
        loop {
            let wait_ms = self
                .nearest_deadline()
                .map(|deadline| deadline.saturating_sub(self.timer.now_ms()));
            let timer = self.timer.clone();
            let step = tokio::select! {
                command = commands.recv() => Step::Command(command),
                frame = self.transport.recv() => Step::Frame(frame),
                _ = async {
                    match wait_ms {
                        Some(ms) => timer.wait(ms.max(1)).await,
                        None => std::future::pending::<()>().await,
                    }
                } => Step::Timeout,
            };
            let ended = match step {
                Step::Command(None) => return LoopEnd::Shutdown,
                Step::Command(Some(command)) => self.apply(command, session_config),
                Step::Frame(Err(error)) => Err(map_transport(error)),
                Step::Frame(Ok(frame)) => self.ingest(frame),
                Step::Timeout => {
                    self.session.handle_timeout(self.timer.now_ms());
                    Ok(())
                }
            };
            if let Err(error) = ended {
                self.fail_pending(error.clone());
                return LoopEnd::Disconnected(error);
            }
            if let Err(error) = self.flush().await {
                self.fail_pending(error.clone());
                return LoopEnd::Disconnected(error);
            }
            if let Err(error) = self.dispatch(shared) {
                self.fail_pending(error.clone());
                return LoopEnd::Disconnected(error);
            }
        }
    }

    fn apply(&mut self, command: Command, session_config: &mut SessionConfig) -> Result<(), Error> {
        match command {
            Command::Call {
                request_type,
                data,
                reply,
            } => {
                self.enqueue_call(&request_type, &data, reply)?;
            }
            Command::Batch {
                calls,
                halt_on_failure,
                execution_type,
                reply,
            } => {
                let deadline = self.arm_deadline();
                match self.session.send_batch(
                    &calls,
                    halt_on_failure,
                    execution_type,
                    Some(deadline),
                ) {
                    Ok(id) => {
                        self.pending.insert(
                            id.as_str().to_string(),
                            Pending {
                                waiter: Waiter::Batch(reply),
                                deadline_ms: deadline,
                            },
                        );
                    }
                    Err(error) => {
                        let _ = reply.send(Err(Error::Protocol(error)));
                    }
                }
            }
            Command::Reidentify {
                event_subscriptions,
                reply,
            } => {
                session_config.event_subscriptions = event_subscriptions;
                let result = self
                    .session
                    .reidentify(event_subscriptions)
                    .map_err(Error::Protocol);
                let _ = reply.send(result);
            }
        }
        Ok(())
    }

    async fn drive_once(&mut self, wait_ms: u64) -> Result<(), Error> {
        self.flush().await?;
        let timer = self.timer.clone();
        let frame = tokio::select! {
            frame = self.transport.recv() => frame.map_err(map_transport)?,
            _ = timer.wait(wait_ms.max(1)) => {
                self.session.handle_timeout(self.timer.now_ms());
                return Ok(());
            }
        };
        self.ingest(frame)
    }

    fn ingest(&mut self, frame: Frame) -> Result<(), Error> {
        match frame {
            Frame::Text(bytes) | Frame::Binary(bytes) => {
                self.session
                    .handle_message(&bytes)
                    .map_err(|error| match error {
                        obs_websocket_core::SessionError::AuthRequired => Error::AuthFailed,
                        other => Error::Protocol(other),
                    })?;
            }
            Frame::Close { code, reason } => self.session.handle_close(code, &reason),
            _ => {}
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Error> {
        while let Some(outgoing) = self.session.poll_transmit() {
            let frame = if outgoing.binary {
                Frame::Binary(outgoing.payload)
            } else {
                Frame::Text(outgoing.payload)
            };
            self.transport.send(frame).await.map_err(map_transport)?;
        }
        Ok(())
    }

    fn dispatch(&mut self, shared: &Inner) -> Result<(), Error> {
        let events = self.drain();
        for event in events {
            match event {
                SessionEvent::Response {
                    request_id, result, ..
                } => {
                    self.complete_call(&request_id, result);
                }
                SessionEvent::BatchResponse {
                    request_id,
                    results,
                } => {
                    self.complete_batch(&request_id, results);
                }
                SessionEvent::Event(event) => shared.emit_event(&event),
                SessionEvent::Closed { code, reason } => {
                    return Err(map_closed(code, reason));
                }
                SessionEvent::Identified { .. } | SessionEvent::UnmatchedResponse { .. } => {}
                _ => {}
            }
        }
        Ok(())
    }

    fn drain(&mut self) -> Vec<SessionEvent> {
        let mut events = Vec::new();
        while let Some(event) = self.session.poll_event() {
            events.push(event);
        }
        events
    }

    fn take_terminal(&mut self) -> Option<Error> {
        let events = self.drain();
        let mut error = None;
        for event in events {
            match event {
                SessionEvent::Closed { code, reason } => error = Some(map_closed(code, reason)),
                SessionEvent::Event(event) => {
                    let _ = event;
                }
                _ => {}
            }
        }
        error
    }

    fn complete_call(&mut self, id: &RequestId, result: Result<Value, RequestFailure>) {
        let Some(pending) = self.pending.remove(id.as_str()) else {
            return;
        };
        if let Waiter::Call(reply) = pending.waiter {
            let _ = reply.send(result.map_err(map_failure));
        }
    }

    fn complete_batch(&mut self, id: &RequestId, results: Vec<BatchItemResult>) {
        let Some(pending) = self.pending.remove(id.as_str()) else {
            return;
        };
        if let Waiter::Batch(reply) = pending.waiter {
            let _ = reply.send(Ok(results));
        }
    }

    fn fail_pending(&mut self, error: Error) {
        for (_, pending) in self.pending.drain() {
            match pending.waiter {
                Waiter::Call(reply) => {
                    let _ = reply.send(Err(error.clone()));
                }
                Waiter::Batch(reply) => {
                    let _ = reply.send(Err(error.clone()));
                }
            }
        }
    }

    fn nearest_deadline(&self) -> Option<u64> {
        self.pending
            .values()
            .map(|pending| pending.deadline_ms)
            .min()
    }

    fn arm_deadline(&self) -> u64 {
        self.timer
            .now_ms()
            .saturating_add(self.request_timeout_ms.max(1))
    }
}

enum Step {
    Command(Option<Command>),
    Frame(Result<Frame, TransportError>),
    Timeout,
}

fn map_failure(failure: RequestFailure) -> Error {
    match failure {
        RequestFailure::Status { code, comment } => Error::Request { code, comment },
        RequestFailure::Timeout => Error::Timeout,
        RequestFailure::Closed => Error::Closed {
            code: None,
            reason: "connection closed".to_string(),
        },
        _ => Error::Disconnected,
    }
}

fn map_closed(code: Option<WebSocketCloseCode>, reason: String) -> Error {
    match code {
        Some(WebSocketCloseCode::AuthenticationFailed) => Error::AuthFailed,
        other => Error::Closed {
            code: other.as_ref().map(|code| code.as_u16()),
            reason,
        },
    }
}

fn publish(shared: &Inner, state: ConnectionState) {
    *shared.connection.lock().expect("connection state") = state.clone();
    let callbacks = shared
        .callbacks
        .lock()
        .expect("callbacks")
        .state_callbacks();
    for callback in callbacks {
        callback(&state);
    }
}

fn publish_closed(shared: &Inner, error: &Error) {
    let (code, reason) = match error {
        Error::Closed { code, reason } => (*code, reason.clone()),
        Error::AuthFailed => (
            Some(WebSocketCloseCode::AuthenticationFailed.as_u16()),
            error.to_string(),
        ),
        other => (None, other.to_string()),
    };
    publish(shared, ConnectionState::Closed { code, reason });
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
