[**English**](README.md) | [简体中文](README-CN.md)

# ferromq-acl

[![crates.io](https://img.shields.io/crates/v/ferromq-acl.svg)](https://crates.io/crates/ferromq-acl)

File-based Access Control List plugin. Evaluates allow/deny rules to control client publish and subscribe access by user, IP address, client ID, and topic filter patterns.

## Overview

Rules are evaluated in order — the first matching rule determines the result. If no rule matches, access is denied by default. The plugin registers 5 hook callbacks covering authentication, ACL checks, and client lifecycle events.

## Usage

### Build

Add the dependency in `ferromqd/Cargo.toml`:

```toml
ferromq-acl = "0.21"
```

Or enable via the `ferromq-plugins` meta-crate:

```toml
ferromq-plugins = { version = "0.21", features = ["acl"] }
```

### Register

```rust
ferromq_acl::register(&scx, true, false).await?;
// or with explicit name:
ferromq_acl::register_named(&scx, "ferromq-acl", true, false).await?;
```

Parameters: `(scx, default_startup, immutable)`.

### Configuration

File: `ferromq-acl.toml` (in the plugin config directory)

```toml
# Disconnect if publishing is rejected
disconnect_if_pub_rejected = true

rules = [
    ["allow", { user = "dashboard" }, "subscribe", ["$SYS/#"]],
    ["allow", { ipaddr = "127.0.0.1" }, "pubsub", ["$SYS/#", "#"]],
    ["deny", "all", "subscribe", ["$SYS/#", { eq = "#" }]],
    ["allow", "all"]
]
```

### Rule Format

```
["allow" | "deny", <who>, <action>, <topics>]
```

**`<who>`** (matcher):

| Format | Description |
|--------|-------------|
| `"all"` | Match any client |
| `{ user = "username" }` | Match by username |
| `{ ipaddr = "127.0.0.1" }` | Match by IP address |
| `{ clientid = "client123" }` | Match by client ID |

**`<action>`**:

| Value | Description |
|-------|-------------|
| `"subscribe"` | Subscribe only |
| `"publish"` | Publish only |
| `"pubsub"` | Both subscribe and publish |

**`<topics>`**: A list of topic filters. Supports `{ eq = "exact/topic" }` for exact match.

### Config Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `disconnect_if_pub_rejected` | `bool` | `true` | Disconnect client when publish is denied |
| `rules` | `array` | — | Ordered list of ACL rules |

## Configuration Source

The plugin loads config via `scx.plugins.load_config_default::<PluginConfig>("ferromq-acl")`, supporting:

1. `{plugins.dir}/ferromq-acl.toml` (file, optional — uses defaults if missing)
2. `ferromq_plugin_ferromq_acl_*` environment variables
3. Inline config via `ServerContext::plugins_config_map_add()`

## Hook Callbacks

| Hook Type | Purpose |
|-----------|---------|
| `ClientConnected` | Pre-compute topic placeholders (`%c`, `%u`) and ACL rules per client |
| `ClientDisconnected` | Clean up per-client cached topic filters |
| `ClientAuthenticate` | Verify username/password against ACL rules |
| `ClientSubscribeCheckAcl` | Check subscribe ACL, return `SubscribeAclResult` |
| `MessagePublishCheckAcl` | Check publish ACL, return `PublishAclResult` |

## Dependencies

`ferromq` (feature `plugin`), `serde`, `tokio`, `async-trait`, `log`, `serde_json`, `ahash`, `bytestring`

## License

MIT OR Apache-2.0
