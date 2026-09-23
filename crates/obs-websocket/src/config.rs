//! Connection settings.

use std::time::Duration;

use crate::error::Error;

/// How the client behaves after the socket drops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconnectPolicy {
    /// When `false`, the first disconnect is final.
    pub enabled: bool,
    /// Delay before the first retry.
    pub initial_delay: Duration,
    /// Upper bound on the exponential backoff.
    pub max_delay: Duration,
    /// `None` retries until the client is dropped. `Some(0)` does not retry.
    pub max_attempts: Option<u32>,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            initial_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(10),
            max_attempts: None,
        }
    }
}

impl ReconnectPolicy {
    /// Does not retry.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }

    /// Delay for `attempt`, starting at 1.
    pub(crate) fn delay(&self, attempt: u32) -> Duration {
        let shift = attempt.saturating_sub(1).min(16);
        let multiplier = 1u32 << shift;
        let millis = self
            .initial_delay
            .as_millis()
            .saturating_mul(u128::from(multiplier));
        let capped = millis.min(self.max_delay.as_millis());
        Duration::from_millis(u64::try_from(capped).unwrap_or(u64::MAX))
    }
}

/// Arguments for [`crate::Client::connect`].
#[derive(Debug, Clone)]
pub struct ConnectConfig {
    /// OBS host, without a scheme.
    pub host: String,
    /// obs-websocket port. OBS defaults to `4455`.
    pub port: u16,
    /// WebSocket password. Empty is treated as no password.
    pub password: Option<String>,
    /// `eventSubscriptions` bitset. `None` lets OBS apply its default.
    pub event_subscriptions: Option<u32>,
    /// Budget for the TCP and WebSocket handshake.
    pub connect_timeout: Duration,
    /// Budget for each request, including the identification handshake.
    pub request_timeout: Duration,
    /// Behavior after a drop that is not an authentication failure.
    pub reconnect: ReconnectPolicy,
    /// Use `wss://`. Requires the `rustls` feature.
    pub tls: bool,
}

impl ConnectConfig {
    /// Connects to `host:port` over `ws://` with a 5 second handshake and request timeout.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            password: None,
            event_subscriptions: None,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            reconnect: ReconnectPolicy::default(),
            tls: false,
        }
    }

    /// Parses `ws://host:port` or `wss://host:port`.
    pub fn from_url(url: &str) -> Result<Self, Error> {
        let (tls, rest) = if let Some(rest) = url.strip_prefix("wss://") {
            (true, rest)
        } else if let Some(rest) = url.strip_prefix("ws://") {
            (false, rest)
        } else {
            return Err(Error::Unreachable(format!("unsupported url `{url}`")));
        };
        let (host, port) = rest
            .rsplit_once(':')
            .ok_or_else(|| Error::Unreachable(format!("url `{url}` is missing a port")))?;
        let port = port
            .parse::<u16>()
            .map_err(|_| Error::Unreachable(format!("url `{url}` has an invalid port")))?;
        if host.is_empty() {
            return Err(Error::Unreachable(format!("url `{url}` is missing a host")));
        }
        Ok(Self::new(host, port).tls(tls))
    }

    /// Sets the password. An empty string clears it.
    pub fn password(mut self, password: Option<&str>) -> Self {
        self.password = password
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        self
    }

    /// Sets the `eventSubscriptions` bitset sent in `Identify` and again after reconnect.
    pub fn event_subscriptions(mut self, bits: u32) -> Self {
        self.event_subscriptions = Some(bits);
        self
    }

    /// Sets the handshake timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Sets the per-request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Sets the reconnect policy.
    pub fn reconnect(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }

    /// Selects `wss://` when `tls` is true.
    pub fn tls(mut self, tls: bool) -> Self {
        self.tls = tls;
        self
    }

    pub(crate) fn url(&self) -> String {
        let scheme = if self.tls { "wss" } else { "ws" };
        format!("{scheme}://{}:{}", self.host, self.port)
    }
}
