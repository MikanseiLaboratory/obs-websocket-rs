# ディレクトリ構成

```
Cargo.toml                 ワークスペース
technologystack.md
directorystructure.md
protocol/
  protocol.json            上流 obs-websocket から取り込んだ定義
  overrides.toml           Object 型の上書き
  UPSTREAM.toml            対応するタグとコミット
crates/
  obs-websocket-core/      メッセージ、認証、Session、生成された型
  obs-websocket-io/        Transport と Connection
  obs-websocket-tokio/     tokio-tungstenite
  obs-websocket-embassy/   embassy-net
  obs-websocket/           Client、コールバック、再接続、状態キャッシュ
    examples/cli.rs        clap の照会・操作 CLI
  obs-websocket-mock/      テスト用サーバー
xtask/                     codegen / fetch-protocol / protocol-diff
.github/workflows/         CI と週次の protocol-sync
```
