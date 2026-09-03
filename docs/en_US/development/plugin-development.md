[**English**](plugin-development.md) | [简体中文](../../zh_CN/development/plugin-development.md)

# Plugin Development Guide

This guide explains how to develop plugins for FerroMQ, covering the plugin lifecycle, hook system, configuration loading, and best practices.

---

## Plugin Architecture Overview

FerroMQ plugins are independent Rust crates that implement the `Plugin` trait. They can intercept broker events through the hook system and extend broker functionality without modifying core code.

```mermaid
graph LR
    subgraph "Plugin Crate"
        Struct[Plugin Struct]
        Config[Config Struct]
        Handler[Handler Struct]
    end

    subgraph "Registration"
        Macro[register! macro]
        Register[Register Handle]
    end

    subgraph "FerroMQ Core"
        Hook[Hook System]
        PluginManager[Plugin Manager]
    end

    Struct --> Macro
    Macro --> Register
    Register --> PluginManager
    PluginManager --> Hook
    Handler --> Hook
    Struct --> Config
```

---

## Plugin Lifecycle

```mermaid
sequenceDiagram
    participant App as ferromqd
    participant Plugin as Plugin Instance
    participant Hook as Hook System
    
    App->>Plugin: new(scx, name)
    Plugin->>Plugin: load_config()
    Plugin-->>App: Instance
    
    App->>Plugin: init()
    Plugin->>Hook: register(handler)
    Plugin-->>App: Ok(())
    
    App->>Plugin: start()
    Plugin->>Hook: activate handlers
    Plugin-->>App: Ok(())
    
    Note over App,Plugin: Runtime operation...
    
    App->>Plugin: load_config()
    Plugin->>Plugin: reload config from file
    Plugin-->>App: Ok(())
    
    App->>Plugin: stop()
    Plugin->>Hook: deactivate handlers
    Plugin-->>App: bool (true=stoppable)
```

---

## Creating a Plugin

### Step 1: Create the Crate

```bash
# Create directory
mkdir ferromq-plugins/ferromq-my-plugin
cd ferromq-plugins/ferromq-my-plugin
```

### Step 2: Cargo.toml

```toml
[package]
name = "ferromq-my-plugin"
version.workspace = true
description = "My custom FerroMQ plugin"
edition.workspace = true
license.workspace = true

[dependencies]
ferromq = { workspace = true, features = ["plugin"] }
serde = { workspace = true, features = ["derive"] }
tokio = { workspace = true, features = ["sync"] }
async-trait.workspace = true
log.workspace = true
serde_json.workspace = true
```

### Step 3: Plugin Structure (src/lib.rs)

```rust
use std::sync::Arc;
use tokio::sync::RwLock;
use ferromq::context::ServerContext;
use ferromq::plugin::{PackageInfo, Plugin, Register};
use ferromq::hook::{self, Type, ReturnType, Handler};
use serde::Deserialize;

// --- Plugin Struct ---

pub struct MyPlugin {
    scx: ServerContext,
    register: Box<dyn Register>,
    cfg: Arc<RwLock<PluginConfig>>,
}

// The register! macro generates register_named() and register() functions
register!(MyPlugin::new);

impl MyPlugin {
    pub async fn new<S: Into<String>>(scx: ServerContext, name: S) -> ferromq::Result<Self> {
        // Load configuration
        let cfg: PluginConfig = scx.plugins.load_config_default(name.into().as_str())?;
        let cfg = Arc::new(RwLock::new(cfg));

        // Get register handle
        let register = scx.plugins.register(name).await?;

        Ok(Self { scx, register, cfg })
    }
}

// --- Configuration ---

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PluginConfig {
    pub setting_one: String,
    pub setting_two: u32,
    pub enable_feature: bool,
}

impl Default for PluginConfig {
    fn default() -> Self {
        Self {
            setting_one: "default_value".into(),
            setting_two: 42,
            enable_feature: true,
        }
    }
}

// --- Handler ---

struct MyHandler {
    cfg: Arc<RwLock<PluginConfig>>,
}

#[async_trait]
impl Handler for MyHandler {
    async fn hook(&self, param: &Type, acc: Option<()>) -> ReturnType {
        match param {
            Type::MessagePublish => {
                // Handle publish event
                log::info!("Message published");
                (true, None) // (continue, no result modification)
            }
            Type::ClientConnected => {
                log::info!("Client connected");
                (true, None)
            }
            _ => (true, None),
        }
    }
}

// --- Plugin Implementation ---

#[async_trait]
impl Plugin for MyPlugin {
    async fn init(&mut self) -> ferromq::Result<()> {
        // Register handlers with hook system
        self.register.add(Type::MessagePublish, MyHandler {
            cfg: self.cfg.clone(),
        }).await;

        self.register.add(Type::ClientConnected, MyHandler {
            cfg: self.cfg.clone(),
        }).await;

        Ok(())
    }

    async fn start(&mut self) -> ferromq::Result<()> {
        // Activate registered handlers
        self.register.start().await
    }

    async fn stop(&mut self) -> Result<bool, Box<dyn std::error::Error>> {
        // Deactivate; return true if stoppable
        self.register.stop().await
    }

    async fn get_config(&self) -> ferromq::Result<serde_json::Value> {
        let cfg = self.cfg.read().await;
        Ok(serde_json::to_value(&*cfg)?)
    }

    async fn load_config(&mut self) -> ferromq::Result<()> {
        let cfg: PluginConfig = self.scx.plugins.load_config_default("ferromq-my-plugin")?;
        *self.cfg.write().await = cfg;
        Ok(())
    }

    async fn attrs(&self) -> serde_json::Value {
        serde_json::json!({
            "name": "my-plugin",
            "version": env!("CARGO_PKG_VERSION"),
        })
    }

    async fn send(&self, _msg: serde_json::Value) -> ferromq::Result<serde_json::Value> {
        Err(anyhow::anyhow!("not implemented"))
    }
}
```

### Step 4: Plugin Configuration (ferromq-my-plugin.toml)

```toml
# ferromq-plugins/ferromq-my-plugin.toml
setting_one = "custom_value"
setting_two = 100
enable_feature = true
```

---

## Registering the Plugin

### Add to ferromq-plugins meta-crate

In `ferromq-plugins/Cargo.toml`:

```toml
[dependencies]
ferromq-my-plugin = { path = "ferromq-my-plugin", optional = true }

[features]
my-plugin = ["ferromq-my-plugin"]
```

In `ferromq-plugins/src/lib.rs`:

```rust
#[cfg(feature = "my-plugin")]
pub use ferromq_my_plugin as my_plugin;
```

### Add to ferromqd binary

In `ferromq-bin/Cargo.toml`:

```toml
ferromq-my-plugin = "0.1"
```

Add to `[package.metadata.plugins]` in `ferromq-bin/Cargo.toml` for auto-registration:

```toml
[package.metadata.plugins]
ferromq-my-plugin = { default_startup = true }
```

### Alternative: Manual Registration

```rust
ferromq_my_plugin::register(&scx, true, false).await?;
```

Parameters: `(scx, default_startup, immutable)`.

---

## Hook Reference

All available hook types:

| Hook Type | Trigger Condition | Handler Returns |
|-----------|-----------------|-----------------|
| `BeforeStartup` | Broker initialization | — |
| `ClientConnect` | CONNECT received | `(bool, Option<ConnAckReason>)` |
| `ClientAuthenticate` | Before CONNACK | `(bool, Option<ConnAckReason>)` |
| `ClientConnack` | CONNACK sent | — |
| `ClientConnected` | Session established | — |
| `ClientDisconnected` | Session ended | — |
| `ClientSubscribe` | SUBSCRIBE received | — |
| `ClientSubscribeCheckAcl` | Subscribe ACL check | `(bool, Option<SubscribeAclResult>)` |
| `ClientUnsubscribe` | UNSUBSCRIBE received | — |
| `MessagePublish` | PUBLISH received | `(bool, Option<MessagePublishResult>)` |
| `MessagePublishCheckAcl` | Publish ACL check | `(bool, Option<PublishAclResult>)` |
| `MessageDelivered` | Message sent to client | — |
| `MessageAcked` | Client acknowledged | — |
| `MessageDropped` | Message dropped | — |
| `SessionCreated` | Session created | — |
| `SessionTerminated` | Session destroyed | — |
| `SessionSubscribed` | Subscription added | — |
| `SessionUnsubscribed` | Subscription removed | — |
| `OfflineMessage` | Offline message stored | — |
| `GrpcMessageReceived` | Cross-node gRPC | `(bool, Option<Vec<u8>>)` |

### Handler Priority

```rust
// Register with specific priority (higher = earlier execution)
register.add_priority(Type::MessagePublish, handler, 100).await
```

The `counter` plugin uses `Priority::MAX`, so it runs first in each hook chain.

---

## Configuration Loading

Plugins load configuration via the `Plugins` API:

```rust
// Config file required, returns error if missing
let cfg: MyConfig = scx.plugins.load_config("my-plugin")?;

// Config file optional, uses Default if missing
let cfg: MyConfig = scx.plugins.load_config_default("my-plugin")?;

// With environment variable list keys
let cfg: MyConfig = scx.plugins.load_config_with("my-plugin", &["my_list_key"])?;

// Optional file + env list keys
let cfg: MyConfig = scx.plugins.load_config_default_with("my-plugin", &["my_list_key"])?;
```

Configuration sources (in priority order):
1. `{plugins.dir}/{name}.toml`
2. `ferromq_plugin_{name}_*` environment variables
3. Inline config via `ServerContext::plugins_config_map_add()`

---

## Best Practices

### 1. Handler Pass-Through

Unless your plugin needs to modify the result, always return `(true, None)` from handlers:

```rust
async fn hook(&self, _param: &Type, _acc: Option<()>) -> ReturnType {
    (true, None)  // continue processing, no modification
}
```

### 2. Configuration Defaults

Always implement `Default` for your config struct and use `load_config_default`:

```rust
#[derive(Deserialize)]
#[serde(default)]  // use Default::default() for missing fields
struct MyConfig { ... }

impl Default for MyConfig { ... }
```

### 3. Thread Safety

The plugin struct must be `Send + Sync`. Use `Arc<RwLock<T>>` for shared mutable state:

```rust
pub struct MyPlugin {
    cfg: Arc<RwLock<PluginConfig>>,
    counter: Arc<AtomicUsize>,
}
```

### 4. Hook Registration

Register hooks in `init()`, not in `new()`. The hook system is not ready during construction.

```rust
async fn init(&mut self) -> Result<()> {
    self.register.add(Type::X, handler).await;
    Ok(())
}
```

### 5. Storage Plugin Pattern

If your plugin provides storage, inject it into `scx.extends` during `start()`:

```rust
async fn start(&mut self) -> Result<()> {
    *self.scx.extends.retain_mut() = Some(Box::new(my_storage));
    self.register.start().await
}
```

### 6. Stop Behavior

- Return `true` if the plugin can be stopped and restarted at runtime
- Return `false` for core plugins that must remain active (ACL, retainer, cluster)

---

## Existing Plugin Examples

| Plugin | Lines | Key Pattern |
|--------|-------|-------------|
| `ferromq-counter` | ~80 | Simplest plugin, no config, 15 hook handlers, `Priority::MAX` |
| `ferromq-acl` | ~300 | ACL with `ClientSubscribeCheckAcl` + `MessagePublishCheckAcl` |
| `ferromq-retainer` | ~200 | Storage injection via `scx.extends`, background cleanup task |
| `ferromq-web-hook` | ~400 | Async producer-consumer with mpsc channel, exponential backoff |
| `ferromq-cluster-raft` | ~500+ | Dedicated OS thread with independent Tokio runtime |

---

## License

MIT OR Apache-2.0
