use std::time::Duration;

use obs_websocket_core::{RawCall, SessionConfig};
use obs_websocket_io::Connection;
use obs_websocket_mock::{MockConfig, MockObs, Reply};
use obs_websocket_tokio::{TokioTimer, TokioTransport};
use serde_json::json;

fn timeout() -> Duration {
    Duration::from_secs(2)
}

async fn identified(
    server: &MockObs,
    password: Option<&str>,
    request_timeout_ms: u64,
) -> Connection<TokioTransport> {
    let transport = TokioTransport::connect(&server.url(), timeout())
        .await
        .expect("socket");
    let config = SessionConfig::default().password(password);
    Connection::connect(transport, config, request_timeout_ms, &TokioTimer::new())
        .await
        .expect("identify")
}

#[tokio::test]
async fn connects_and_reads_version() {
    let server = MockObs::spawn(MockConfig::default()).await.unwrap();
    let mut connection = identified(&server, None, 2_000).await;
    let version = connection
        .request(
            &obs_websocket_core::requests::GetVersion::new(),
            &TokioTimer::new(),
        )
        .await
        .unwrap();
    assert_eq!(version.obs_version, "31.0.0");
    assert_eq!(version.obs_web_socket_version, "5.7.4");
}

#[tokio::test]
async fn authenticates_with_password() {
    let server = MockObs::spawn(MockConfig::default().password("secret"))
        .await
        .unwrap();
    let mut connection = identified(&server, Some("secret"), 2_000).await;
    let version = connection
        .raw_request(
            &RawCall::new("GetVersion", serde_json::Value::Null),
            &TokioTimer::new(),
        )
        .await
        .unwrap();
    assert_eq!(version["platform"], "test");
}

#[tokio::test]
async fn rejects_a_wrong_password() {
    let server = MockObs::spawn(MockConfig::default().password("secret"))
        .await
        .unwrap();
    let transport = TokioTransport::connect(&server.url(), timeout())
        .await
        .unwrap();
    let error = match Connection::connect(
        transport,
        SessionConfig::default().password(Some("nope")),
        2_000,
        &TokioTimer::new(),
    )
    .await
    {
        Ok(_) => panic!("expected authentication to fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        obs_websocket_io::Error::AuthFailed | obs_websocket_io::Error::Closed { .. }
    ));
}

#[tokio::test]
async fn mixes_a_raw_request_and_a_batch() {
    let server = MockObs::spawn(MockConfig::default()).await.unwrap();
    server.set_reply("VendorRequest", Reply::Success(json!({ "ok": true })));
    server.set_reply(
        "VendorFail",
        Reply::Failure {
            code: 600,
            comment: Some("nope".to_string()),
        },
    );
    let mut connection = identified(&server, None, 2_000).await;
    let timer = TokioTimer::new();
    let raw = connection
        .raw_request(
            &RawCall::new("VendorRequest", serde_json::Value::Null),
            &timer,
        )
        .await
        .unwrap();
    assert_eq!(raw["ok"], true);
    let batch = connection
        .raw_batch(
            &[
                RawCall::new("VendorRequest", serde_json::Value::Null),
                RawCall::new("VendorFail", serde_json::Value::Null),
            ],
            true,
            0,
            &timer,
        )
        .await
        .unwrap();
    assert!(batch[0].result.is_ok());
    assert!(matches!(
        &batch[1].result,
        Err(obs_websocket_core::RequestFailure::Status { code: 600, .. })
    ));
}

#[tokio::test]
async fn request_times_out_when_the_server_hangs() {
    let server = MockObs::spawn(MockConfig::default()).await.unwrap();
    server.set_reply("GetVersion", Reply::Hang);
    let mut connection = identified(&server, None, 200).await;
    let error = connection
        .request(
            &obs_websocket_core::requests::GetVersion::new(),
            &TokioTimer::new(),
        )
        .await
        .expect_err("timeout");
    assert!(matches!(error, obs_websocket_io::Error::Timeout));
}

#[tokio::test]
async fn delivers_an_event() {
    let server = MockObs::spawn(MockConfig::default()).await.unwrap();
    let mut connection = identified(&server, None, 2_000).await;
    server.emit("ExitStarted", json!({}));
    let event = connection.next_event().await.unwrap();
    assert_eq!(event.event_type(), "ExitStarted");
}
