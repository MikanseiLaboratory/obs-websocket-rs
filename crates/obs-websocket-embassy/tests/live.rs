//! Talks to a running OBS. Ignored in CI.
//!
//! ```text
//! OBS_WS_HOST=127.0.0.1 cargo test -p obs-websocket-embassy --features std --test live -- --ignored
//! ```

use std::time::{Duration, Instant};

use embedded_io_adapters::tokio_1::FromTokio;
use obs_websocket_core::SessionConfig;
use obs_websocket_core::requests::GetVersion;
use obs_websocket_embassy::EmbassyTransport;
use obs_websocket_io::{Connection, Timer};
use tokio::net::TcpStream;

struct HostTimer {
    origin: Instant,
}

impl Timer for HostTimer {
    fn now_ms(&self) -> u64 {
        self.origin.elapsed().as_millis() as u64
    }

    async fn wait(&self, duration_ms: u64) {
        tokio::time::sleep(Duration::from_millis(duration_ms)).await;
    }
}

fn required_host() -> String {
    let host = std::env::var("OBS_WS_HOST").unwrap_or_default();
    if host.is_empty() {
        panic!("set OBS_WS_HOST to the OBS machine");
    }
    host
}

fn port() -> u16 {
    std::env::var("OBS_WS_PORT")
        .ok()
        .map(|value| value.parse().expect("OBS_WS_PORT is a u16"))
        .unwrap_or(4455)
}

fn password() -> Option<String> {
    std::env::var("OBS_WS_PASSWORD")
        .ok()
        .filter(|value| !value.is_empty())
}

#[unsafe(no_mangle)]
fn _embassy_time_now() -> u64 {
    0
}

#[unsafe(no_mangle)]
fn _embassy_time_schedule_wake(_at: u64, _waker: &core::task::Waker) {}

#[tokio::test]
#[ignore = "requires a running OBS; set OBS_WS_HOST, optional OBS_WS_PORT and OBS_WS_PASSWORD"]
async fn live_get_version() {
    critical_section::with(|_| {});
    let host = required_host();
    let port = port();
    let header = format!("{host}:{port}");
    let tcp = TcpStream::connect((host.as_str(), port)).await.unwrap();
    let transport = EmbassyTransport::<_, 4096, 4096>::connect(FromTokio::new(tcp), &header, 1)
        .await
        .unwrap();
    let timer = HostTimer {
        origin: Instant::now(),
    };
    let mut connection = Connection::connect(
        transport,
        SessionConfig::default().password(password().as_deref()),
        2_000,
        &timer,
    )
    .await
    .unwrap();
    assert!(connection.negotiated_rpc_version().is_some());
    let version = connection
        .request(&GetVersion::new(), &timer)
        .await
        .unwrap();
    assert!(!version.obs_web_socket_version.is_empty());
}
