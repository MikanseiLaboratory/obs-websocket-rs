use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use obs_websocket_core::requests::SetCurrentProgramScene;
use obs_websocket_mock::{MockConfig, MockObs, Reply};
use serde_json::json;

use crate::{
    Client, ConnectConfig, ConnectionState, Error, Event, ReconnectPolicy,
    RequestBatchExecutionType,
};

fn open_mock() -> MockConfig {
    MockConfig {
        available_requests: Vec::new(),
        ..MockConfig::default()
    }
}

async fn connect_to(
    server: &MockObs,
    configure: impl FnOnce(ConnectConfig) -> ConnectConfig,
) -> Client {
    let addr = server.local_addr();
    let config = configure(
        ConnectConfig::new(addr.ip().to_string(), addr.port())
            .request_timeout(Duration::from_secs(2))
            .reconnect(ReconnectPolicy::disabled()),
    );
    Client::connect(config).await.expect("connect")
}

async fn wait_until(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition");
}

#[tokio::test]
async fn typed_and_raw_requests_share_one_session() {
    let server = MockObs::spawn(open_mock()).await.unwrap();
    server.set_reply("VendorRequest", Reply::Success(json!({ "ok": true })));
    let client = connect_to(&server, |config| config).await;
    let version = client.general().get_version().await.unwrap();
    assert_eq!(version.obs_version, "31.0.0");
    let raw = client
        .raw_request("VendorRequest", json!({}))
        .await
        .unwrap();
    assert_eq!(raw["ok"], true);
    assert_eq!(server.identifications(), 1);
}

#[tokio::test]
async fn concurrent_requests_keep_their_responses() {
    let server = MockObs::spawn(open_mock()).await.unwrap();
    server.set_reply(
        "GetVersion",
        Reply::Delay {
            after: Duration::from_millis(80),
            reply: Box::new(Reply::Success(json!({
                "obsVersion": "31.0.0",
                "obsWebSocketVersion": "5.7.4",
                "rpcVersion": 1,
                "availableRequests": [],
                "supportedImageFormats": ["png"],
                "platform": "test",
                "platformDescription": "obs-websocket-mock",
            }))),
        },
    );
    server.set_reply("VendorRequest", Reply::Success(json!({ "n": 1 })));
    let client = connect_to(&server, |config| config).await;
    let general = client.general();
    let version = general.get_version();
    let raw = client.raw_request("VendorRequest", serde_json::Value::Null);
    let (version, raw) = tokio::join!(version, raw);
    assert_eq!(version.unwrap().obs_version, "31.0.0");
    assert_eq!(raw.unwrap()["n"], 1);
}

#[tokio::test]
async fn rejects_requests_missing_from_available_requests() {
    let config = MockConfig {
        available_requests: vec!["GetVersion".to_string()],
        ..MockConfig::default()
    };
    let server = MockObs::spawn(config).await.unwrap();
    let client = connect_to(&server, |config| config).await;
    let error = client
        .scenes()
        .set_current_program_scene(&SetCurrentProgramScene::new().scene_name("Live"))
        .await
        .expect_err("unsupported");
    assert!(matches!(
        error,
        Error::UnsupportedRequest { request_type } if request_type == "SetCurrentProgramScene"
    ));
}

#[tokio::test]
async fn sends_a_batch_with_a_typed_and_a_raw_call() {
    let server = MockObs::spawn(open_mock()).await.unwrap();
    server.set_reply(
        "VendorFail",
        Reply::Failure {
            code: 600,
            comment: Some("nope".to_string()),
        },
    );
    let client = connect_to(&server, |config| config).await;
    let results = client
        .batch()
        .add(&obs_websocket_core::requests::GetVersion::new())
        .add_raw(obs_websocket_core::RawCall::new(
            "VendorFail",
            serde_json::Value::Null,
        ))
        .execution(RequestBatchExecutionType::SerialFrame)
        .halt_on_failure(true)
        .send()
        .await
        .unwrap();
    assert!(results[0].result.is_ok());
    assert!(matches!(
        &results[1].result,
        Err(obs_websocket_core::RequestFailure::Status { code: 600, .. })
    ));
}

#[tokio::test]
async fn callback_stops_when_the_subscription_is_dropped() {
    let server = MockObs::spawn(open_mock()).await.unwrap();
    let client = connect_to(&server, |config| config).await;
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&hits);
    let subscription =
        client.on::<obs_websocket_core::generated::events::ExitStarted, _>(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
        });
    server.emit("ExitStarted", json!({}));
    wait_until(|| hits.load(Ordering::SeqCst) == 1).await;
    drop(subscription);
    server.emit("ExitStarted", json!({}));
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn event_stream_receives_events() {
    let server = MockObs::spawn(open_mock()).await.unwrap();
    let client = connect_to(&server, |config| config).await;
    let mut events = Box::pin(client.events());
    server.emit("ExitStarted", json!({}));
    let event = tokio::time::timeout(Duration::from_secs(1), events.next())
        .await
        .expect("event")
        .expect("stream item");
    assert!(matches!(event, Event::ExitStarted(_)));
}

#[tokio::test]
async fn reconnects_and_restores_subscriptions() {
    let server = MockObs::spawn(open_mock()).await.unwrap();
    let client = connect_to(&server, |config| {
        config.event_subscriptions(4).reconnect(ReconnectPolicy {
            enabled: true,
            initial_delay: Duration::from_millis(20),
            max_delay: Duration::from_millis(40),
            max_attempts: Some(3),
        })
    })
    .await;
    assert_eq!(server.identifications(), 1);
    assert_eq!(server.event_subscriptions(), Some(4));
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&hits);
    let _subscription = client.on_any(move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
    });
    let states = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = Arc::clone(&states);
    let _states = client.on_connection_state(move |state| {
        recorded.lock().expect("states").push(state.clone());
    });
    server.disconnect();
    wait_until(|| server.identifications() >= 2).await;
    assert_eq!(server.event_subscriptions(), Some(4));
    // The state feature snapshots OBS before it reports Connected.
    wait_until(|| client.connection_state() == ConnectionState::Connected).await;
    server.emit("ExitStarted", json!({}));
    wait_until(|| hits.load(Ordering::SeqCst) >= 1).await;
    let states = states.lock().expect("states");
    assert!(
        states
            .iter()
            .any(|state| matches!(state, ConnectionState::Reconnecting { .. }))
    );
    assert!(
        states
            .iter()
            .any(|state| matches!(state, ConnectionState::Connected))
    );
}

#[tokio::test]
async fn authentication_failure_is_typed() {
    let server = MockObs::spawn(MockConfig::default().password("secret"))
        .await
        .unwrap();
    let addr = server.local_addr();
    let error = Client::connect(
        ConnectConfig::new(addr.ip().to_string(), addr.port())
            .password(Some("nope"))
            .reconnect(ReconnectPolicy::disabled()),
    )
    .await
    .expect_err("auth");
    assert!(matches!(error, Error::AuthFailed));
}

#[cfg(feature = "state")]
#[tokio::test]
async fn state_cache_tracks_the_snapshot_and_events() {
    let server = MockObs::spawn(open_mock()).await.unwrap();
    server.set_reply(
        "GetCurrentProgramScene",
        Reply::Success(json!({
            "sceneName": "Live",
            "sceneUuid": "scene-1",
            "currentProgramSceneName": "Live",
            "currentProgramSceneUuid": "scene-1",
        })),
    );
    server.set_reply(
        "GetStudioModeEnabled",
        Reply::Success(json!({ "studioModeEnabled": false })),
    );
    server.set_reply(
        "GetInputList",
        Reply::Success(json!({
            "inputs": [{
                "inputName": "Mic",
                "inputUuid": "input-1",
                "inputKind": "wasapi_input_capture",
                "unversionedInputKind": "wasapi_input_capture",
                "inputKindCaps": 0
            }]
        })),
    );
    server.set_reply(
        "GetInputMute",
        Reply::Success(json!({ "inputMuted": true })),
    );
    server.set_reply(
        "GetInputVolume",
        Reply::Success(json!({ "inputVolumeMul": 0.5, "inputVolumeDb": -6.0 })),
    );
    server.set_reply(
        "GetStreamStatus",
        Reply::Success(json!({
            "outputActive": false,
            "outputReconnecting": false,
            "outputTimecode": "00:00:00.000",
            "outputDuration": 0,
            "outputCongestion": 0.0,
            "outputBytes": 0,
            "outputSkippedFrames": 0,
            "outputTotalFrames": 0
        })),
    );
    server.set_reply(
        "GetRecordStatus",
        Reply::Success(json!({
            "outputActive": false,
            "outputPaused": false,
            "outputTimecode": "00:00:00.000",
            "outputDuration": 0,
            "outputBytes": 0
        })),
    );
    server.set_reply(
        "GetVirtualCamStatus",
        Reply::Success(json!({ "outputActive": false })),
    );
    let client = connect_to(&server, |config| config).await;
    let state = client.state();
    assert_eq!(state.program_scene.as_deref(), Some("Live"));
    assert_eq!(state.studio_mode, Some(false));
    assert_eq!(state.inputs["Mic"].muted, Some(true));
    assert_eq!(state.inputs["Mic"].volume_mul, Some(0.5));
    assert_eq!(state.streaming, Some(false));
    server.emit(
        "CurrentProgramSceneChanged",
        json!({ "sceneName": "Break", "sceneUuid": "scene-2" }),
    );
    server.emit(
        "StreamStateChanged",
        json!({ "outputActive": true, "outputState": "OBS_WEBSOCKET_OUTPUT_STARTED" }),
    );
    wait_until(|| client.state().program_scene.as_deref() == Some("Break")).await;
    assert_eq!(client.state().streaming, Some(true));
}

#[tokio::test]
#[ignore = "set OBS_WS_URL and OBS_WS_PASSWORD to run against a live OBS"]
async fn live_obs_get_version() {
    let Ok(url) = std::env::var("OBS_WS_URL") else {
        return;
    };
    let mut config = ConnectConfig::from_url(&url)
        .unwrap()
        .reconnect(ReconnectPolicy::disabled());
    if let Ok(password) = std::env::var("OBS_WS_PASSWORD") {
        config = config.password(Some(&password));
    }
    let client = Client::connect(config).await.unwrap();
    let version = client.general().get_version().await.unwrap();
    assert!(!version.obs_version.is_empty());
}
