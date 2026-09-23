# obs-websocket-core

Sans I/O, `no_std + alloc` implementation of the OBS WebSocket v5 client protocol.

[`Session`](https://docs.rs/obs-websocket-core) turns socket bytes into requests and events. It does not open connections. Use `obs-websocket-io` with `obs-websocket-tokio` or `obs-websocket-embassy`, or the stateful `obs-websocket` client.

Typed requests and events are generated from `protocol/protocol.json`. Do not edit `src/generated`.
