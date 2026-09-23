# obs-websocket-rs

Rust client for [OBS WebSocket](https://github.com/obsproject/obs-websocket) v5 (vendored protocol 5.7.4).

`obs-websocket-core` is a Sans I/O session (`no_std` + `alloc`): messages, authentication, and request tracking, with no socket. `obs-websocket` is a stateful client on tokio. Typed requests, raw op-6 requests, and op-8 batches share that one session. Transports live in `obs-websocket-tokio` (`ws` / `wss`) and `obs-websocket-embassy` (`embassy-net`).

## Sample

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

## CLI example

`crates/obs-websocket/examples/cli.rs` queries and controls a running OBS. The password is `--password` or `OBS_WS_PASSWORD`.

```text
cargo run -p obs-websocket --example cli -- status
cargo run -p obs-websocket --example cli -- scenes list
cargo run -p obs-websocket --example cli -- raw GetVersion
cargo run -p obs-websocket --example cli -- watch
```

## License

Apache-2.0 OR MIT.
