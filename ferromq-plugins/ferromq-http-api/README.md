[**English**](README.md) | [简体中文](README-CN.md)

# ferromq-http-api

[![crates.io](https://img.shields.io/crates/v/ferromq-http-api.svg)](https://crates.io/crates/ferromq-http-api)

RESTful HTTP API plugin. Provides broker management endpoints, health checks, node info, client/subscription/route listing, MQTT operations, plugin management, statistics, and Prometheus metrics.

## Overview

Uses the `salvo` HTTP framework to serve API endpoints. Starts the HTTP server during plugin construction (`new()`). Supports hot-reload: when config changes require a restart (listen address changed), the old server is shut down after the new one starts. Forwards cluster-wide queries via gRPC.

## Usage

### Build

Add the dependency in `ferromqd/Cargo.toml`:

```toml
ferromq-http-api = "0.21"
```

Requires `ferromq` features: `plugin`, `metrics`, `stats`, `grpc`, `shared-subscription`.

### Register

```rust
ferromq_http_api::register(&scx, true, false).await?;
// or with explicit name:
ferromq_http_api::register_named(&scx, "ferromq-http-api", true, false).await?;
```

Parameters: `(scx, default_startup, immutable)`.

## Configuration

File: `ferromq-http-api.toml` (in the plugin config directory). Loaded via `scx.plugins.read_config_default::<PluginConfig>("ferromq-http-api")`.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_row_limit` | `usize` | `10000` | Maximum number of rows returned by list endpoints |
| `http_laddr` | `string` | `"0.0.0.0:6060"` | HTTP server listen address |
| `http_request_log` | `bool` | `false` | Whether to print HTTP request logs |
| `http_reuseaddr` | `bool` | `true` | Enable `SO_REUSEADDR` socket option (Unix only) |
| `http_reuseport` | `bool` | `false` | Enable `SO_REUSEPORT` socket option (Unix only) |
| `http_bearer_token` | `string` | — | Bearer token for HTTP API authentication (optional; treated as admin/operator) |
| `dashboard_admin_username` | `string` | `"admin"` | Bootstrap admin username when no dashboard users exist |
| `dashboard_admin_password` | `string` | — | Bootstrap admin password (hashed with bcrypt on first login / `/auth/init`; never stored as plaintext) |
| `dashboard_viewer_username` | `string` | — | Optional bootstrap viewer username |
| `dashboard_viewer_password` | `string` | — | Optional bootstrap viewer password |
| `dashboard_cookie_secure` | `bool` | `false` | Set `Secure` on the session cookie (enable behind HTTPS) |
| `dashboard_session_idle_timeout` | `string` | `"30m"` | Idle session expiry |
| `dashboard_session_max_age` | `string` | `"12h"` | Absolute session lifetime |
| `dashboard_login_rate_limit` | `u32` | `10` | Max login attempts per IP per window |
| `dashboard_login_rate_window` | `string` | `"1m"` | Login rate-limit window |
| `audit_max_events` | `usize` | `10000` | In-memory audit ring-buffer size |
| `audit_file` | `string` | — | Optional JSONL file for durable audit events |
| `config_history_keep` | `usize` | `10` | Last-N backups kept when writing plugin or `ferromq.toml` config |
| `broker_config_file` | `string` | — | Path to this node's `ferromq.toml` for `/api/v1/broker/config` |
| `message_type` | `u8` | `99` | gRPC message type identifier for plugin communication |
| `message_expiry_interval` | `string` | `"5m"` | Default message expiration interval for publish operations |
| `metrics_sample_interval` | `string` | `"5s"` | Metrics sampling interval |
| `prometheus_metrics_cache_interval` | `string` | `"5s"` | Prometheus metrics data caching interval |
| `storage` | `object` | — | History data storage config (optional; omit to disable history) |
| `flush_interval` | `string` | `"5s"` | History flush interval |
| `history_retention` | `string` | `"7d"` | History data retention. During warmup, each data point's timestamp is checked against this duration — expired entries are discarded from the cache and removed from storage. |

### Configuration Source

Supports the standard FerroMQ plugin config chain:

1. `{plugins.dir}/ferromq-http-api.toml` (file, optional — uses defaults if missing)
2. `ferromq_plugin_ferromq_http_api_*` environment variables
3. Inline config via `ServerContext::plugins_config_map_add()`

### Example

```toml
# ferromq-http-api.toml
max_row_limit = 10_000
http_laddr = "0.0.0.0:6060"
http_request_log = false
message_expiry_interval = "5m"
prometheus_metrics_cache_interval = "5s"
```

## API Endpoints

All endpoints are prefixed with `/api/v1`.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | List all available API endpoints |
| POST | `/auth/login` | Dashboard login; sets `ferromq_session` cookie |
| POST | `/auth/logout` | Clear the session cookie |
| GET | `/auth/me` | Current user (`session` / `bearer` / `anonymous`) |
| POST | `/auth/change-password` | Change the current session user's password |
| POST | `/auth/init` | One-time bootstrap of the configured admin |
| GET | `/openapi.json` | OpenAPI 3 document for `/api/v1` |
| GET | `/docs` | Swagger UI for the OpenAPI document |
| **Brokers** | | |
| GET | `/brokers` | Return basic information of all nodes in the cluster |
| GET | `/brokers/{id}` | Return basic information of a specific node |
| **Nodes** | | |
| GET | `/nodes` | Return status of all nodes in the cluster |
| GET | `/nodes/{id}` | Return status of a specific node |
| **Features** | | |
| GET | `/features` | Return feature support state of all cluster nodes |
| GET | `/features/{id}` | Return feature support state of a specific node |
| **Health** | | |
| GET | `/health/check` | Health check for the cluster |
| GET | `/health/check/{id}` | Health check for a specific node |
| **Clients** | | |
| GET | `/clients` | Search client information from the cluster |
| GET | `/clients/{clientid}` | Get specific client information |
| DELETE | `/clients/{clientid}` | Kick client from the cluster |
| GET | `/clients/{clientid}/online` | Check if a client is online |
| GET | `/clients/offlines` | Search offline client information |
| DELETE | `/clients/offlines` | Kick offline clients from the cluster |
| **Subscriptions** | | |
| GET | `/subscriptions` | Query subscription information from the cluster |
| GET | `/subscriptions/{clientid}` | Get subscriptions for a specific client |
| **Routes** | | |
| GET | `/routes` | Return all routing information from the cluster |
| GET | `/routes/{topic}` | Get routing information for a specific topic |
| **Retained Messages** | | |
| GET | `/retains` | Query retained messages with `topic_filter` / `offset` / `limit` params |
| DELETE | `/retains?topic=` | Delete a retained message by exact topic |
| **MQTT Operations** | | |
| POST | `/mqtt/publish` | Publish an MQTT message |
| POST | `/mqtt/subscribe` | Subscribe to an MQTT topic for a session |
| POST | `/mqtt/unsubscribe` | Unsubscribe from MQTT topics |
| **Plugins** | | |
| GET | `/plugins` | Returns information of all plugins in the cluster |
| GET | `/plugins/{node}` | Returns plugin information for a specific node |
| GET | `/plugins/{node}/{plugin}` | Get a specific plugin's info |
| GET | `/plugins/{node}/{plugin}/config` | Get a plugin's configuration (secrets redacted unless `?reveal=1` + admin) |
| PUT | `/plugins/{node}/{plugin}/config` | Write a plugin config (`?apply=reload\|none`); returns diff + `effective` |
| POST | `/plugins/{node}/{plugin}/config/validate` | Dry-run validate a plugin config |
| GET | `/plugins/{node}/{plugin}/config/versions` | List last-N plugin config backups |
| POST | `/plugins/{node}/{plugin}/config/rollback/{version}` | Restore a plugin config backup |
| PUT | `/plugins/{node}/{plugin}/config/reload` | Reload a plugin's configuration |
| GET | `/broker/config` | Read-only `ferromq.toml` overview (mqtt/listener/log) |
| PUT | `/broker/config/{mqtt\|listener\|log}` | Write a broker section (admin; always `restart_required`) |
| PUT | `/plugins/{node}/{plugin}/load` | Load/start a plugin on a node |
| PUT | `/plugins/{node}/{plugin}/unload` | Unload/stop a plugin on a node |
| **Statistics** | | |
| GET | `/stats` | Returns all statistics from the cluster |
| GET | `/stats/sum` | Summarize all statistics from the cluster |
| GET | `/stats/{id}` | Returns statistics for a specific node |
| GET | `/stats/sys` | Returns all system statistics from the cluster |
| GET | `/stats/sys/sum` | Summarize all system statistics from the cluster |
| GET | `/stats/sys/{id}` | Returns system statistics for a specific node |
| **Metrics** | | |
| GET | `/metrics` | Returns all metrics from the cluster |
| GET | `/metrics/sum` | Summarize all metrics from the cluster |
| GET | `/metrics/{id}` | Returns metrics for a specific node |
| GET | `/metrics/prometheus` | Get Prometheus metrics from the cluster |
| GET | `/metrics/prometheus/sum` | Summarize Prometheus metrics |
| GET | `/metrics/prometheus/{id}` | Get Prometheus metrics for a specific node |

### Retained Message Query

`GET /retains` queries retained messages:

| Param | Type | Default | Description |
|---|---|---|---|
| `topic_filter` | string | `#` | Topic filter supporting `#` / `+` wildcards; empty or `#` uses the full-range pagination path |
| `offset` | usize | `0` | Pagination offset |
| `limit` | usize | `max_row_limit` | Page size, capped at `max_row_limit` |

Response example:

```json
{
  "items": [
    {
      "topic": "/iot/b/x",
      "msg_id": 1024,
      "from": { "typ": "client", "id": { "node_id": 1, "client_id": "c1" } },
      "publish": {
        "topic": "/iot/b/x",
        "qos": 1,
        "retain": true,
        "dup": false,
        "payload": "<base64 encoded>",
        "create_time": 1780000000000,
        "properties": null
      },
      "remaining_ttl": 3599
    }
  ],
  "has_more": false
}
```

> Note: the `topic_filter=#` (full-range) path paginates at the storage layer and includes `remaining_ttl` (seconds); the filtered path paginates in memory with `remaining_ttl` as `null`. Retained messages are broadcast-synced across cluster nodes, so a single-node query already covers the whole cluster.

### Feature Support Query

`GET /features` returns the feature support state of every cluster node plus a cluster-wide consistency summary:

```json
{
  "consistent": false,
  "node_count": 3,
  "conflicts": [
    {
      "feature": "retain",
      "values": [
        { "value": true,  "node_ids": [1, 2] },
        { "value": false, "node_ids": [3] }
      ]
    }
  ],
  "nodes": [
    {
      "node_id": 1,
      "node_name": "1@127.0.0.1",
      "features": {
        "retain": true,
        "message_storage": false,
        "session_storage": false,
        "delayed": true,
        "shared_subscription": true,
        "auto_subscription": false
      }
    }
  ]
}
```

- `consistent`: whether all reachable nodes report identical feature flags; `false` indicates config drift or a partially-failed plugin on some node
- `failed_count` / `partial`: unreachable nodes (HTTP 200 with structured `{ ok, error }` entries)
- `enabled`: OR of each flag across reachable nodes — use this for dashboard menu gating
- `conflicts`: feature fields whose values differ, grouped by value with node lists (empty when `consistent` is `true`)
- `nodes`: per-node `{ ok, node_id, features? / error? }` (no bare error strings)
- A `features inconsistent across cluster` warning log is emitted when an inconsistency is detected

OpenAPI 3: `GET /api/v1/openapi.json`. Optional list envelope: `?format=page` → `{ items, offset, limit, truncated, total? }` (default remains a bare array). Errors: `{ code, message, details?, request_id }`.

### Authentication

Two credentials are accepted (MQTT client auth plugins are not involved):

- **Session cookie** — `POST /api/v1/auth/login` with `{ username, password }`. Cookie `ferromq_session` is `HttpOnly`, `SameSite=Lax`, and `Secure` when `dashboard_cookie_secure` is true. Roles: `admin` (users / keys / audit + writes), `operator` (kick / publish / plugins), `viewer` (read-only).
- **Bearer token** — `Authorization: Bearer <http_bearer_token>` remains a superuser admin credential for automation (username `operator`).
- **API keys** — `POST /api/v1/api-keys` (admin). Secret is SHA-256 hashed and shown once; authenticate as `Authorization: Bearer <secret>` with the bound role.
- **Open access** — if neither a bearer token nor `dashboard_admin_password` is set (and no users/keys exist yet), the API stays open (anonymous admin).

Bootstrap: when no dashboard users exist, the first matching login or `POST /api/v1/auth/init` creates the configured admin (and optional viewer). Passwords are bcrypt hashes in **process memory** (lost on restart; cluster nodes do not share sessions — use sticky sessions). Health, OpenAPI, login, logout, and init stay public. Admin-only: `GET/POST /users`, `GET/POST/DELETE /api-keys`, `GET /audit`.

## Dependencies

`ferromq` (features: `plugin`, `metrics`, `stats`, `grpc`, `shared-subscription`), `salvo`, `tokio`, `serde`, `serde_json`, `anyhow`, `base64`, `futures`

## License

MIT OR Apache-2.0
