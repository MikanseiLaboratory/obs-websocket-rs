# obs-websocket-rs

OBS WebSocket v5 client for Rust. The protocol core is Sans I/O and `no_std + alloc`. A stateful client on tokio sends typed requests, raw requests, and batches on one session.

obs-websocket v5 のクライアントです。プロトコル処理はソケットを持たない `no_std + alloc` のコアにあり、tokio 上のクライアントは型付きリクエストと Raw リクエストを同じセッションで送れます。

## Supported protocol

| This crate | obs-websocket | Commit |
| --- | --- | --- |
| 0.1.0 | [5.7.4](https://github.com/obsproject/obs-websocket/tree/5.7.4) | [`1ef34bf48110c2a18184e50e41cd0b1a855e2147`](https://github.com/obsproject/obs-websocket/commit/1ef34bf48110c2a18184e50e41cd0b1a855e2147) |

`protocol/UPSTREAM.toml` records the vendored tag. Typed requests and events are generated from `protocol/protocol.json`. Object fields are given concrete types in `protocol/overrides.toml`; a field without an override stays `serde_json::Value`.

## Crates

| Crate | Role |
| --- | --- |
| `obs-websocket` | Stateful client: typed API, raw requests, batches, callbacks, reconnect, optional state cache |
| `obs-websocket-tokio` | `ws://` and `wss://` transport for tokio |
| `obs-websocket-io` | `Transport` and a runtime-agnostic connection driver |
| `obs-websocket-core` | Messages, authentication, and the Sans I/O session |
| `obs-websocket-embassy` | `embassy-net` transport for `thumbv7em-none-eabihf` and `riscv32imc-unknown-none-elf` |
| `obs-websocket-mock` | Scriptable OBS stand-in used by tests (`publish = false`) |

MSRV is Rust 1.85, except `obs-websocket-embassy`, which needs Rust 1.91 because `smoltcp` 0.13 does. Edition is 2024.

## Connect

`obs-websocket` defaults to `ws://`. Enable `wss://` with the `rustls` feature, which is on by default.

```rust
use obs_websocket::Client;
use obs_websocket::ConnectConfig;

let client = Client::connect(
    ConnectConfig::new("127.0.0.1", 4455).password(Some("secret")),
)
.await?;
let version = client.general().get_version().await?;
println!("{}", version.obs_version);
```

`Client::connect` identifies, then reads `GetVersion`. A request missing from `availableRequests` returns `Error::UnsupportedRequest` before it is sent. Authentication failure is `Error::AuthFailed` and is not retried. Other disconnects reconnect with exponential backoff and restore the latest event subscriptions.

## Raw requests and batches

Typed calls and op 6 / op 8 share the session owned by the driver task.

```rust
use obs_websocket::RawCall;
use obs_websocket::RequestBatchExecutionType;
use obs_websocket_core::requests::GetVersion;
use serde_json::json;

let raw = client
    .raw_request("GetVersion", serde_json::Value::Null)
    .await?;

let batch = client
    .batch()
    .add(&GetVersion::new())
    .add_raw(RawCall::new("GetStudioModeEnabled", json!({})))
    .execution(RequestBatchExecutionType::SerialFrame)
    .halt_on_failure(true)
    .send()
    .await?;
let _ = (raw, batch);
```

## Events

```rust
use futures_util::StreamExt;
use obs_websocket_core::generated::events::ExitStarted;

let mut events = Box::pin(client.events());
while let Some(event) = events.next().await {
    println!("{}", event.event_type());
}

let subscription = client.on::<ExitStarted, _>(|_event| {
    // Runs on the driver task. Do not wait on `Client::request` here.
});
drop(subscription);
```

`client.on_any` receives every event, including ones this build does not know (`Event::Unknown`). `client.on_connection_state` reports reconnects. Dropping the returned `Subscription` unregisters the callback.

The `state` feature caches the current program scene, input mute and volume, and stream, record, and virtual camera status. Read it with `client.state()` after connect.

## embassy

`obs-websocket-embassy` frames WebSocket traffic with `embedded-websocket` 0.9 on an `embassy-net` TCP socket. The caller supplies the read and write buffers.

```text
cargo check -p obs-websocket-embassy --target thumbv7em-none-eabihf
cargo check -p obs-websocket-embassy --target riscv32imc-unknown-none-elf
cargo run -p obs-websocket-embassy --features std --example embassy-std
```

The example wraps a tokio `TcpStream` so the same framing can be exercised on a host.

## Regenerating the protocol

```text
cargo xtask codegen
cargo xtask codegen --check
cargo xtask fetch-protocol
cargo xtask fetch-protocol --ref 5.7.4
cargo xtask protocol-diff protocol/previous.json protocol/protocol.json
```

`fetch-protocol` follows obs-websocket git tags. It does not use GitHub Releases, because the newest release tag is not the newest v5 protocol. A weekly workflow opens a pull request when a newer 5.x tag changes `protocol.json`. The pull request body lists added, removed, and deprecated requests and events, plus Object fields that still decode as JSON values.

## Live OBS

`live_obs_get_version` is ignored unless `OBS_WS_URL` is set. Run it locally with:

```text
OBS_WS_URL=ws://127.0.0.1:4455 OBS_WS_PASSWORD=secret cargo test -p obs-websocket --lib -- --ignored
```

The `live-obs` GitHub workflow runs the same test from the `OBS_WS_URL` and `OBS_WS_PASSWORD` repository secrets.

## License

MIT OR Apache-2.0.
