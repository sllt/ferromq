[**English**](getting-started.md) | [简体中文](../../zh_CN/development/getting-started.md)

# Getting Started with FerroMQ Development

This guide walks you through setting up a development environment, building FerroMQ from source, running tests, and understanding the development workflow.

---

## Prerequisites

### Required

- **Rust** 1.94.0+ (install via [rustup](https://rustup.rs/))
- **protoc** (protobuf compiler) — required for building tonic/prost dependencies
- **Git** (for cloning and version management)

### Platform-Specific Setup

**Linux (Ubuntu/Debian)**:
```bash
sudo apt update
sudo apt install build-essential protobuf-compiler cmake pkg-config libssl-dev curl-dev zlib1g-dev
```

**Linux (Alpine)** — used for Docker builds:
```bash
apk add musl-dev protoc make pkgconfig openssl-dev openssl-libs-static cmake g++ curl-dev curl-static zlib-dev zlib-static
```

**macOS**:
```bash
brew install protobuf cmake pkg-config openssl
```

**Windows**:
1. Install [protoc](https://github.com/protocolbuffers/protobuf/releases) — download `protoc-{version}-win64.zip`, extract to `C:\protoc`, add to PATH
2. Install [Build Tools for Visual Studio](https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2022) (for C++ build tools)
3. Ensure `cmake` is available (install via [CMake website](https://cmake.org/download/) or `winget install cmake`)

### Verify Installation

```bash
rustc --version          # should be 1.94.0+
cargo --version          # should be 1.94.0+
protoc --version         # should be 3.x+
```

---

## Clone and Build

### Get the Source

```bash
git clone https://github.com/ferromq/rmqtt.git
cd ferromq
```

### Build FerroMQ

```bash
# Debug build (fast compilation, for development)
cargo build

# Release build (optimized, for testing and production)
cargo build --release

# Build a specific sub-crate
cargo build -p ferromq-codec
cargo build -p ferromqd

# Build with all features
cargo build --release --all-features
```

The production binary is at `target/release/ferromqd` (or `ferromqd.exe` on Windows).

### Build Time Optimization

First build compiles all dependencies and can take 10-30 minutes. Subsequent builds are incremental:

```bash
# Use a specific crate for faster iteration during development
cargo build -p ferromq-codec

# Build only the core library (skip plugins and binary)
cargo build -p ferromq
```

---

## Project Structure

```
ferromq/
├── Cargo.toml              # Workspace root
├── ferromq.toml              # Server configuration (for testing)
│
├── ferromq/                  # Core broker library
│   ├── Cargo.toml          # Features: metrics, stats, plugin, grpc, tls, ws, quic...
│   ├── src/                # Source code (25+ modules)
│   └── examples/           # Library mode examples (simple, multi, plugin, tls, ws, quic)
│
├── ferromq-bin/              # Binary entry point
│   └── src/
│       ├── server.rs       # Main entry: CLI → config → plugins → start
│       ├── logger.rs       # Tracing-based logger setup
│       └── build.rs        # Plugin registration codegen
│
├── ferromq-codec/            # MQTT protocol codec
│   └── src/
│       ├── v3/             # MQTT v3.1.1
│       ├── v5/             # MQTT v5.0
│       ├── version/        # Version negotiation
│       ├── error.rs        # Error types
│       └── types.rs        # Shared protocol types
│
├── ferromq-net/              # Network layer
│   └── src/
│       ├── builder.rs      # Builder + Listener + Acceptor
│       ├── stream.rs       # MQTT stream (v3/v5)
│       ├── ws.rs           # WebSocket support
│       ├── quic.rs         # QUIC support
│       └── error.rs        # MqttError
│
├── ferromq-conf/             # Configuration management
│   └── src/
│       ├── listener.rs     # Listener configuration
│       ├── logging.rs      # Logging configuration
│       └── options.rs      # CLI argument parsing
│
├── ferromq-utils/            # Shared utilities
│   └── src/
│       ├── counter.rs      # Atomic counter with merge
│       └── lib.rs          # Bytesize, NodeAddr, timers, serde helpers
│
├── ferromq-macros/           # Procedural macros (Metrics, Plugin)
│   └── src/
│       ├── metrics.rs      # #[derive(Metrics)] — atomic counter generation
│       └── plugin.rs       # #[derive(Plugin)] — PackageInfo trait
│
├── ferromq-test/             # Test harness
│   └── src/
│       ├── main.rs         # ferromq_harness entry point
│       ├── broker/         # Broker lifecycle management
│       ├── mqtt/           # Custom MQTT clients (v3/v5)
│       ├── framework/      # Test framework (TestCase, scheduler, context)
│       ├── tests/          # Test cases (functional, stress, chaos)
│       └── report/         # Output reports (console, JSON, HTML)
│
├── ferromq-plugins/          # Plugin collection
│   ├── Cargo.toml          # Meta-crate with 20+ feature flags
│   ├── *.toml              # Plugin configuration files
│   └── ferromq-*/            # Individual plugin crates (25 crates)
│
├── docs/                   # Documentation
│   ├── en_US/              # English docs (28 files)
│   └── zh_CN/              # Chinese docs (28 files)
│
├── Dockerfile              # Docker build (x86_64)
├── Dockerfile.amd64        # Docker build (AMD64)
├── Dockerfile.aarch64      # Docker build (ARM64)
└── Makefile                # Docker build automation
```

---

## Development Workflow

### 1. Code → Build → Test Loop

```bash
# Edit code, then:
cargo check              # Fast validation (no binary generation)
cargo build -p ferromq     # Build specific crate
cargo test -p ferromq-codec # Run unit tests for a crate
```

### 2. Linting

```bash
# Format code
cargo fmt --all

# Lint (zero warnings required)
cargo clippy --all-targets

# If clippy introduces new warnings, fix them before committing
```

### 3. Running the Full Test Suite

```bash
# Build release binary (required by test harness)
cargo build --release

# Run all unit tests
cargo test

# Run integration tests using the test harness
cargo build -p ferromq-test --release
# Starts the broker with the self-contained config ferromq-test/configs/default/ferromq.toml
./target/release/ferromq_harness --workspace .
```

### 4. Manual Testing

```bash
# Start the broker in dev mode
cargo run -p ferromqd

# Or with a specific config
cargo run -p ferromqd -- -f my-config.toml

# Test with mosquitto clients (separate terminal)
mosquitto_sub -h 127.0.0.1 -p 1883 -t "test/#" -v
mosquitto_pub -h 127.0.0.1 -p 1883 -t "test/topic" -m "hello"
```

---

## Understanding Feature Flags

The core library (`ferromq`) has 15 feature flags. For development, the most commonly used combinations are:

```bash
# Minimal plugin development
cargo build -p ferromq --features "plugin"

# Full feature set (what ferromqd uses)
cargo build -p ferromq --features "full"

# Test a specific feature combination
cargo test -p ferromq --features "plugin,metrics"
```

When adding a new feature:
1. Add it to `ferromq/Cargo.toml` `[features]` section
2. Gate module imports with `#[cfg(feature = "your-feature")]`
3. Add it to the `full` feature list if it should be included by default in production
4. Update the feature table in documentation

---

## Working with Plugins

### Creating a New Plugin

1. Create a new crate under `ferromq-plugins/`:

```bash
mkdir ferromq-plugins/ferromq-my-plugin
# Create Cargo.toml and src/lib.rs
```

2. Add the plugin to `ferromq-plugins/Cargo.toml` as an optional dependency:

```toml
ferromq-my-plugin = { path = "ferromq-my-plugin", optional = true }
```

3. Add a feature flag and re-export in `ferromq-plugins/src/lib.rs`:

```rust
#[cfg(feature = "my-plugin")]
pub use ferromq_my_plugin as my_plugin;
```

4. Add the plugin to `ferromq-bin/Cargo.toml` for inclusion in the binary.

For the plugin structure, follow the standard pattern:

```rust
use ferromq::plugin::{PackageInfo, Plugin};
use ferromq::context::ServerContext;

pub struct MyPlugin { /* ... */ }

register!(MyPlugin::new);

impl MyPlugin {
    pub async fn new<S: Into<String>>(scx: ServerContext, name: S) -> Result<Self> {
        // Load config, get register handle
    }
}

#[async_trait]
impl Plugin for MyPlugin {
    async fn init(&mut self) -> Result<()> { /* register hooks */ }
    async fn start(&mut self) -> Result<()> { /* activate */ }
    async fn stop(&mut self) -> Result<bool> { /* deactivate */ }
    // ...
}
```

---

## Debugging Tips

### Enable Debug Logging

```bash
# Console only
RUST_LOG=debug cargo run -p ferromqd

# File output with trace level
RUST_LOG=trace cargo run -p ferromqd -- -f ferromq.toml
```

### MQTT Packet Tracing

The test harness (`ferromq_harness`) logs MQTT packet hex dumps at debug level:

```bash
RUST_LOG=debug ./target/release/ferromq_harness --workspace .
```

### Common Issues

**`protoc` not found**:
```bash
# Install protobuf compiler
# Linux: sudo apt install protobuf-compiler
# macOS: brew install protobuf
# Windows: download from GitHub releases, add to PATH
```

**OpenSSL build errors**:
```bash
# Linux: sudo apt install pkg-config libssl-dev
# macOS: brew install openssl pkg-config
```

**`cargo clippy` warnings in new code**:
- Run `cargo clippy --fix` to auto-fix where possible
- Use `#[allow(clippy::xxx)]` only when you have a justifiable reason

---

## Documentation Standards

All new features and changes must include:

1. **Code documentation** — `///` doc comments on all public API items
2. **Module-level docs** — `//!` at the top of each module explaining its purpose
3. **README updates** — Update the corresponding README.md and README-CN.md
4. **Feature docs** — If adding a plugin or significant feature, add a doc in `docs/en_US/` and `docs/zh_CN/`

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for the full contribution guide.
