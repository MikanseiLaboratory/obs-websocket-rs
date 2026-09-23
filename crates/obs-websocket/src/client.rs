//! Stateful client. One driver task owns the socket.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use futures_util::{Stream, stream};
use obs_websocket_core::{BatchItemResult, Event, EventPayload, RawCall, Request};
use serde_json::Value;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::batch::Batch;
use crate::callback::{Registry, Subscription};
use crate::config::ConnectConfig;
use crate::driver;
use crate::error::Error;

#[cfg(feature = "state")]
use crate::state::ObsState;

/// Where the driver is in its connection lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionState {
    /// `connect` has started and the socket is not up yet.
    Connecting,
    /// Identified and accepting requests.
    Connected,
    /// Waiting to open the socket again.
    Reconnecting {
        /// 1-based retry count.
        attempt: u32,
    },
    /// The driver gave up or authentication failed.
    Closed {
        /// Close code, when one was available.
        code: Option<u16>,
        /// Close reason.
        reason: String,
    },
}

pub(crate) enum Command {
    Call {
        request_type: String,
        data: Value,
        reply: oneshot::Sender<Result<Value, Error>>,
    },
    Batch {
        calls: Vec<RawCall>,
        halt_on_failure: bool,
        execution_type: i64,
        reply: oneshot::Sender<Result<Vec<BatchItemResult>, Error>>,
    },
    Reidentify {
        event_subscriptions: Option<u32>,
        reply: oneshot::Sender<Result<(), Error>>,
    },
}

pub(crate) struct Inner {
    commands: mpsc::UnboundedSender<Command>,
    events: broadcast::Sender<Event>,
    pub(crate) callbacks: Arc<Mutex<Registry>>,
    pub(crate) available: Arc<Mutex<Option<HashSet<String>>>>,
    pub(crate) connection: Arc<Mutex<ConnectionState>>,
    #[cfg(feature = "state")]
    pub(crate) obs_state: Arc<Mutex<ObsState>>,
}

impl Inner {
    pub(crate) fn emit_event(&self, event: &Event) {
        #[cfg(feature = "state")]
        {
            self.obs_state.lock().expect("state").apply(event);
        }
        let callbacks = self.callbacks.lock().expect("callbacks").event_callbacks();
        for callback in callbacks {
            callback(event);
        }
        let _ = self.events.send(event.clone());
    }
}

/// OBS WebSocket client.
///
/// Typed requests, raw requests, and batches share one session. Dropping the
/// last clone stops the driver.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Client")
            .field("connection", &self.connection_state())
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Connects, identifies, and reads `GetVersion` before returning.
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), obs_websocket::Error> {
    /// let client = obs_websocket::Client::connect(
    ///     obs_websocket::ConnectConfig::new("127.0.0.1", 4455).password(Some("secret")),
    /// )
    /// .await?;
    /// let version = client.general().get_version().await?;
    /// let _ = version.obs_version;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(config: ConnectConfig) -> Result<Self, Error> {
        let (commands, commands_rx) = mpsc::unbounded_channel();
        let (events, _) = broadcast::channel(256);
        let (ready_tx, ready_rx) = oneshot::channel();
        let inner = Arc::new(Inner {
            commands,
            events,
            callbacks: Arc::new(Mutex::new(Registry::new())),
            available: Arc::new(Mutex::new(None)),
            connection: Arc::new(Mutex::new(ConnectionState::Connecting)),
            #[cfg(feature = "state")]
            obs_state: Arc::new(Mutex::new(ObsState::default())),
        });
        let shared = Arc::clone(&inner);
        tokio::spawn(async move {
            driver::run(config, commands_rx, shared, ready_tx).await;
        });
        ready_rx.await.map_err(|_| Error::Disconnected)??;
        Ok(Self { inner })
    }

    /// Sends a typed request on this session.
    pub async fn request<R: Request>(&self, request: &R) -> Result<R::Response, Error> {
        self.ensure_supported(R::REQUEST_TYPE)?;
        let data = serde_json::to_value(request).map_err(|error| {
            Error::Protocol(obs_websocket_core::SessionError::Decode(
                obs_websocket_core::CodecError::new(error.to_string()),
            ))
        })?;
        let value = self.dispatch_call(R::REQUEST_TYPE, data).await?;
        serde_json::from_value(value).map_err(|error| {
            Error::Protocol(obs_websocket_core::SessionError::Decode(
                obs_websocket_core::CodecError::new(error.to_string()),
            ))
        })
    }

    /// Sends an untyped request on this session.
    pub async fn raw_request(
        &self,
        request_type: impl Into<String>,
        data: Value,
    ) -> Result<Value, Error> {
        let request_type = request_type.into();
        self.ensure_supported(&request_type)?;
        self.dispatch_call(&request_type, data).await
    }

    /// Starts an op 8 batch on this session.
    pub fn batch(&self) -> Batch<'_> {
        Batch::new(self)
    }

    /// Replaces event subscriptions. The new bitset is sent again after reconnect.
    pub async fn reidentify(&self, event_subscriptions: Option<u32>) -> Result<(), Error> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Reidentify {
                event_subscriptions,
                reply,
            })
            .map_err(|_| Error::Disconnected)?;
        rx.await.map_err(|_| Error::Disconnected)?
    }

    /// Broadcast stream of events. Lagged receivers skip missed events.
    pub fn events(&self) -> impl Stream<Item = Event> + Send + use<> {
        let receiver = self.inner.events.subscribe();
        stream::unfold(receiver, |mut receiver| async move {
            loop {
                match receiver.recv().await {
                    Ok(event) => return Some((event, receiver)),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        })
    }

    /// Invokes `callback` for events whose payload is `E`.
    ///
    /// The callback runs on the driver task. Spawn a task if it needs to send a request.
    pub fn on<E, F>(&self, callback: F) -> Subscription
    where
        E: EventPayload + Send + Sync + 'static,
        F: Fn(&E) + Send + Sync + 'static,
    {
        let callback = Arc::new(callback);
        let wrapped: Arc<dyn Fn(&Event) + Send + Sync> = Arc::new(move |event: &Event| {
            if let Some(payload) = E::from_event(event) {
                callback(payload);
            }
        });
        let id = self
            .inner
            .callbacks
            .lock()
            .expect("callbacks")
            .push_event(wrapped);
        Subscription::new(id, &self.inner.callbacks)
    }

    /// Invokes `callback` for every event, including ones this build does not know.
    pub fn on_any<F>(&self, callback: F) -> Subscription
    where
        F: Fn(&Event) + Send + Sync + 'static,
    {
        let id = self
            .inner
            .callbacks
            .lock()
            .expect("callbacks")
            .push_event(Arc::new(callback));
        Subscription::new(id, &self.inner.callbacks)
    }

    /// Invokes `callback` when the driver connects, retries, or gives up.
    pub fn on_connection_state<F>(&self, callback: F) -> Subscription
    where
        F: Fn(&ConnectionState) + Send + Sync + 'static,
    {
        let id = self
            .inner
            .callbacks
            .lock()
            .expect("callbacks")
            .push_state(Arc::new(callback));
        Subscription::new(id, &self.inner.callbacks)
    }

    /// Latest lifecycle state.
    pub fn connection_state(&self) -> ConnectionState {
        self.inner
            .connection
            .lock()
            .expect("connection state")
            .clone()
    }

    /// Cached scene, input, and output state.
    #[cfg(feature = "state")]
    pub fn state(&self) -> ObsState {
        self.inner.obs_state.lock().expect("state").clone()
    }

    pub(crate) fn ensure_supported(&self, request_type: &str) -> Result<(), Error> {
        let available = self.inner.available.lock().expect("available requests");
        if let Some(available) = available.as_ref() {
            if !available.contains(request_type) {
                return Err(Error::UnsupportedRequest {
                    request_type: request_type.to_string(),
                });
            }
        }
        Ok(())
    }

    async fn dispatch_call(&self, request_type: &str, data: Value) -> Result<Value, Error> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Call {
                request_type: request_type.to_string(),
                data,
                reply,
            })
            .map_err(|_| Error::Disconnected)?;
        rx.await.map_err(|_| Error::Disconnected)?
    }

    pub(crate) async fn dispatch_batch(
        &self,
        calls: Vec<RawCall>,
        halt_on_failure: bool,
        execution_type: i64,
    ) -> Result<Vec<BatchItemResult>, Error> {
        let (reply, rx) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Batch {
                calls,
                halt_on_failure,
                execution_type,
                reply,
            })
            .map_err(|_| Error::Disconnected)?;
        rx.await.map_err(|_| Error::Disconnected)?
    }
}
