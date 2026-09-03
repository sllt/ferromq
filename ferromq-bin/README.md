[**English**](README.md) | [简体中文](README-CN.md)

# ferromqd

[![crates.io page](https://img.shields.io/crates/v/rmqtt.svg)](https://crates.io/crates/rmqtt)
![Rust](https://img.shields.io/badge/rust-1.94%2B-blue)

Official binary entry point for the FerroMQ MQTT broker.

## What it does

- **Startup flow**: Parse CLI args → initialize `ferromq_conf::Settings` singleton → install rustls crypto backend → init tracing logger → create `ferromq::context::ServerContext` → start gRPC server → register plugins from `Cargo.toml` metadata → bind configured listeners → start MQTT server
- **Build-time plugin registration**: `build.rs` reads `[package.metadata.plugins]` from `Cargo.toml`, auto-generates `plugin.rs` with a `registers()` function. Each plugin is registered based on `default_startup` and `immutable` flags
- **Listener types**: TCP, TLS, WebSocket (WS), TLS-WebSocket (WSS), QUIC
- **Signal handling**: `Ctrl+C` on Windows, `SIGTERM` + `SIGINT` on Unix; 100ms graceful delay before exit
- **Logging**: Configured via `ferromq_conf::logging::Log` — supports `off/console/file/both` modes, UTC+8 timestamps, non-blocking file writer
- **Linux allocator**: Uses `tikv-jemallocator` as the default memory allocator on Linux

## Build

```bash
cargo build -p ferromqd --release
# Artifact: target/release/ferromqd (and compatibility alias target/release/ferromqd)
```

## Run

```bash
./target/release/ferromqd
./target/release/ferromqd -f /path/to/ferromq.toml
./target/release/ferromqd --config /path/to/ferromq.toml
./target/release/ferromqd --id 1
# Compatibility alias (prints a deprecation warning):
./target/release/ferromqd
```

## CLI arguments

Defined by `ferromq_conf::Options` (via `clap::Parser`):

| Argument | Type | Description |
|----------|------|-------------|
| `-f`, `--config` | `Option<String>` | Config file path |
| `-V`, `--version` | `bool` | Print version info |
| `--id` | `Option<u64>` | Node ID |
| `--plugins-default-startups` | `Option<Vec<String>>` | Override default plugin startups (repeatable) |
| `--node-grpc-addrs` | `Option<Vec<NodeAddr>>` | Node gRPC addresses, format `"1@127.0.0.1:5363"` (repeatable) |
| `--raft-peer-addrs` | `Option<Vec<NodeAddr>>` | Raft peer addresses, format `"1@127.0.0.1:6003"` (repeatable) |
| `--raft-leader-id` | `Option<u64>` | Raft leader ID; default 0 (first node becomes leader) |

## Configuration

Loaded by `ferromq_conf::Settings` from the following sources (later wins):

1. `/etc/ferromq/ferromq.{toml,json,...}` and `/etc/ferromq.{toml,json,...}` (optional)
2. `./ferromq.{toml,json,...}` (optional)
3. `FERROMQ_*` environment variables
4. `-f` / `--config` specified file
5. CLI arguments (`--id`, plugin startups, cluster addrs)

## Docker

Three Dockerfiles for different architectures:
- `Dockerfile` — default
- `Dockerfile.amd64` — x86_64
- `Dockerfile.aarch64` — ARM64

```bash
docker build -t ferromqd .
```

## Related crates

- [ferromq] — Core MQTT Broker library
- [ferromq-conf] — Configuration management
- [ferromq-plugins] — Plugin collection

[ferromq]: https://crates.io/crates/rmqtt
[ferromq-conf]: https://crates.io/crates/ferromq-conf
[ferromq-plugins]: https://crates.io/crates/ferromq-plugins

## License

MIT OR Apache-2.0
