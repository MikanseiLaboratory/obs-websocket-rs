# 技術スタック

バージョンを変更する場合は承認を得ること。MSRV は Rust 1.85、edition は 2024。

## クレート構成

- `obs-websocket-core` 0.1.0: Sans I/O、`no_std + alloc`
- `obs-websocket-io` 0.1.0: ランタイム非依存の `Transport` と `Connection`
- `obs-websocket-tokio` 0.1.0: tokio 向けトランスポート（`ws` / `wss`）
- `obs-websocket-embassy` 0.1.0: embassy-net 向けトランスポート
- `obs-websocket` 0.1.0: ステートフルな高レベルクライアント
- `obs-websocket-mock`: テスト用の疑似 OBS サーバー（非公開）
- `xtask`: `protocol.json` からのコード生成（非公開）

## 依存関係（MSRV 1.85 で固定）

- `serde` 1.0（`alloc`）
- `serde_json` 1.0（`alloc`）
- `sha2` 0.10.8
- `base64` 0.22.1
- `bitflags` 2.9
- `thiserror` 2.0（std クレートのみ）
- `tokio` 1.44
- `tokio-tungstenite` 0.26.2（`wss` は `rustls-tls-webpki-roots`）
- `futures-util` 0.3.31
- `rmp-serde` 1.3（`msgpack` 機能）
- `embassy-net` 0.9.1（`tcp`, `proto-ipv4`, `medium-ethernet`）
- `embedded-io-async` 0.6
- `embedded-websocket` 0.9.5（`default-features = false`。既定の `std` 機能は no_std ターゲットで `httparse` を std 付きで引き込む）
- `rand_core` 0.6（マスキング鍵。`no_std`）
- `rustls` 0.23（`ring`, `std`, `tls12`。`obs-websocket-tokio` の `rustls` 機能）
- `tokio-rustls` 0.26（開発依存。`wss` テストのサーバー側）
- `rcgen` 0.13（開発依存。`wss` テスト証明書）
- `critical-section` 1.2（`std`。ホスト上の embassy テストと例がリンクするために必要）
- `embedded-io-adapters` 0.6（`tokio-1`。`examples/embassy-std`）

`tokio-tungstenite` 0.30 も rust-version 1.85 だが、利用先の streamdeck-obs-websocket が 0.26 系であるため 0.26.2 に揃える。

`edge-ws` 0.8 は rust-version 1.88 のため不採用。WebSocket フレーム処理は `embedded-websocket` 0.9.5 のバイト列コーデックを embassy-net の `TcpSocket`（`embedded-io-async`）の上で使う。

`embassy-net` 0.9.1 が依存する `smoltcp` 0.13 の rust-version は 1.91 である。そのため `obs-websocket-embassy` の MSRV は 1.91 とし、他のクレートは 1.85 のままにする。

Cargo.lock は MSRV 1.85 を保つため、推移的依存を次に固定している。`cargo update` で上げる場合は 1.85 でのビルドを確認すること。

- `time` 0.3.41（`rcgen` 0.13 経由。0.3.47 以降は RUSTSEC-2026-0009 の修正だが rust-version 1.88。テスト証明書の生成では RFC 2822 の解析をしないため、deny.toml でこの advisory を無視する）
- `idna_adapter` 1.2.0（`ureq` 経由。1.2.2 は rust-version 1.86 で、icu 2.3 は 1.88）

## プロトコル

- OBS WebSocket v5（obs-websocket 5.7.4 の `docs/generated/protocol.json`）
- 型付き API は `cargo xtask codegen` で生成する
- Object 型の具体化は `protocol/overrides.toml`

## ライセンス

MIT OR Apache-2.0。ライブラリクレートは crates.io へ公開可能な構成にする（mock と xtask は `publish = false`）。
