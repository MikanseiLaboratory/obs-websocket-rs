//! Runs [`obs_websocket_embassy::EmbassyTransport`] on a tokio TCP stream.
//!
//! ```text
//! cargo run -p obs-websocket-embassy --features std --example embassy-std
//! ```
//!
//! The example starts a mock OBS server, then identifies with the embassy transport.

use std::time::{Duration, Instant};

use embedded_io_adapters::tokio_1::FromTokio;
use obs_websocket_core::SessionConfig;
use obs_websocket_core::requests::GetVersion;
use obs_websocket_embassy::EmbassyTransport;
use obs_websocket_io::{Connection, Timer};
use obs_websocket_mock::{MockConfig, MockObs};
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

#[tokio::main]
async fn main() {
    critical_section::with(|_| {});
    let server = MockObs::spawn(MockConfig::default()).await.expect("mock");
    let tcp = TcpStream::connect(server.local_addr()).await.expect("tcp");
    let stream = FromTokio::new(tcp);
    let host = server.local_addr().to_string();
    let transport = EmbassyTransport::<_, 4096, 4096>::connect(stream, &host, 1)
        .await
        .expect("handshake");
    let timer = HostTimer {
        origin: Instant::now(),
    };
    let mut connection = Connection::connect(transport, SessionConfig::default(), 2_000, &timer)
        .await
        .expect("identify");
    let version = connection
        .request(&GetVersion::new(), &timer)
        .await
        .expect("version");
    println!("{}", version.obs_web_socket_version);
}

#[unsafe(no_mangle)]
fn _embassy_time_now() -> u64 {
    0
}

#[unsafe(no_mangle)]
fn _embassy_time_schedule_wake(_at: u64, _waker: &core::task::Waker) {}
