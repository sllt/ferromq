[**English**](http-api.md) | [简体中文](../../zh_CN/reference/http-api.md)

# HTTP API Reference

The FerroMQ HTTP API provides RESTful management endpoints for broker monitoring, client management, subscriptions, messaging, and plugin control.

## Configuration

The HTTP API is provided by the `ferromq-http-api` plugin.

```toml
# ferromq-http-api.toml
# Listen address
http_laddr = "0.0.0.0:6060"

# Maximum number of rows returned in list queries
max_row_limit = 10_000

# Log HTTP requests
http_request_log = false

# Message expiry interval
message_expiry_interval = "5m"

# Prometheus metrics cache interval
prometheus_metrics_cache_interval = "5s"

# Optional Bearer token authentication (operator / automation)
# http_bearer_token = "your-secret-token"

# Dashboard session login (P3a). First login or POST /api/v1/auth/init
# bootstraps the admin (bcrypt, in-memory).
# dashboard_admin_username = "admin"
# dashboard_admin_password = "change-me"
# audit_max_events = 10000
# audit_file = "/var/log/ferromq/http-api-audit.jsonl"
```

### Authentication

- **Session:** `POST /api/v1/auth/login` `{ username, password }` → `ferromq_session` cookie (`HttpOnly`, `SameSite=Lax`). Also `POST /logout`, `GET /me`, `POST /change-password`, `POST /init`.
- **Bearer:** `Authorization: Bearer <http_bearer_token>` is still a superuser admin credential (username `operator`). Created API keys also use Bearer with a bound role.
- **Open access:** if neither token nor `dashboard_admin_password` is set (and no users/keys), the API stays open.
- **Roles:** `admin` manages users / keys / audit / broker config write / `?reveal=1`; `operator` can kick / publish / plugin config write+reload; `viewer` is read-only (`403`, secrets redacted).
- **Scope:** http-api only. MQTT client auth plugins are unchanged. Users/sessions/keys are in-memory (sticky sessions in a cluster).

## Base URL

All endpoints are under `/api/v1/`.

---

## 1. API Introspection

List all available endpoints.

```
GET /api/v1
GET /api/v1/openapi.json
GET /api/v1/docs
```

`GET /api/v1/openapi.json` is the OpenAPI 3 contract (valid for `openapi-typescript` / orval). `GET /api/v1/docs` is a Swagger UI shell.

---

## 2. Broker & Node Info

### List Brokers

```
GET /api/v1/brokers
GET /api/v1/brokers/{id}
```

Returns cluster node information.

### List Nodes

```
GET /api/v1/nodes
GET /api/v1/nodes/{id}
```

Returns node status.

### Health Check

```
GET /api/v1/health/check
GET /api/v1/health/check/{id}
```

Returns `{"code": 0, "msg": "ok"}` if healthy.

### Feature Support

```
GET /api/v1/features
GET /api/v1/features/{id}
```

Returns the supported feature state of every cluster node (`retain`, `message_storage`, `session_storage`, `delayed`, `shared_subscription`, `auto_subscription`), plus a cluster-wide consistency summary:

- `consistent`: whether all reachable nodes agree on every feature flag
- `failed_count` / `partial`: unreachable nodes (HTTP 200, structured per-node `{ ok, error }`)
- `enabled`: OR of each flag across reachable nodes (dashboard menu gating)
- `conflicts`: fields with inconsistent values, grouped by value with the affected node ids
- `nodes`: per-node `{ ok, node_id, features? / error? }`

Machine-readable contract: `GET /api/v1/openapi.json` (UI: `GET /api/v1/docs`).

A `features inconsistent across cluster` warning log is emitted when an inconsistency is detected.

---

## 2b. Users, API keys, audit (admin)

```
GET    /api/v1/users
POST   /api/v1/users
POST   /api/v1/users/{username}/disable
POST   /api/v1/users/{username}/enable
GET    /api/v1/api-keys
POST   /api/v1/api-keys          # secret returned once
GET    /api/v1/api-keys/{id}
DELETE /api/v1/api-keys/{id}
GET    /api/v1/audit             # ?action=&username=&success=&_limit=&_offset=&format=page
```

See the full [HTTP API](../http-api.md) document for curl examples.

## 2c. Broker config (P4)

```
GET    /api/v1/broker/config
GET    /api/v1/broker/config/{mqtt|listener|log}
PUT    /api/v1/broker/config/{mqtt|listener|log}     # admin; file only
POST   /api/v1/broker/config/{section}/validate
GET    /api/v1/broker/config/versions
POST   /api/v1/broker/config/rollback/{version}      # admin
```

Always `effective=restart_required`. ferromqd is not hot-restarted.

## 2d. Access control & integrations (P5)

```
GET/PUT     /api/v1/acl
GET/POST    /api/v1/acl/rules
PUT/DELETE  /api/v1/acl/rules/{index}
GET         /api/v1/auth-providers
GET/PUT     /api/v1/auth-providers/{http|jwt}
POST        /api/v1/auth-providers/{name}/test
GET         /api/v1/blacklist          # available=false (no plugin)
GET/POST    /api/v1/auto-subscriptions
PUT/DELETE  /api/v1/auto-subscriptions/{index}
GET/POST    /api/v1/topic-rewrites
PUT/DELETE  /api/v1/topic-rewrites/{index}
GET/PUT     /api/v1/webhooks
POST        /api/v1/webhooks/urls | /rules | /test
GET         /api/v1/bridges
GET/PUT     /api/v1/bridges/{plugin}
PUT         /api/v1/bridges/{plugin}/load|unload
```

Writes reuse P4 plugin-config + `load_config`. Webhook/auth-http tests are TCP stubs with SSRF checks (no HTTP fetch).

## 3. Client Management

### Search Clients

```
GET /api/v1/clients
```

Query parameters:

| Parameter | Type | Description |
|-----------|------|-------------|
| `_limit` | `u64` | Max results |
| `clientid` | `string` | Exact client ID |
| `username` | `string` | Exact username |
| `ip_address` | `string` | Client IP |
| `connected` | `bool` | Connection status |
| `clean_start` | `bool` | Clean session flag |
| `session_present` | `bool` | Session present |
| `proto_ver` | `u8` | Protocol version (3, 4, 5) |
| `_like_clientid` | `string` | Client ID pattern match |
| `_like_username` | `string` | Username pattern match |
| `_gte_created_at` | `i64` | Created after (timestamp) |
| `_lte_created_at` | `i64` | Created before (timestamp) |
| `_gte_connected_at` | `i64` | Connected after (timestamp) |
| `_lte_connected_at` | `i64` | Connected before (timestamp) |
| `_gte_mqueue_len` | `usize` | Message queue length >= |
| `_lte_mqueue_len` | `usize` | Message queue length <= |

### Get Client

```
GET /api/v1/clients/{clientid}
```

### Kick Client (Disconnect)

```
DELETE /api/v1/clients/{clientid}
```

Kicks a connected client from the broker.

### Check Online Status

```
GET /api/v1/clients/{clientid}/online
```

### Search Offline Clients

```
GET /api/v1/clients/offlines
```

### Kick All Offline Clients

```
DELETE /api/v1/clients/offlines
```

Returns the count of kicked clients.

---

## 4. Subscriptions

### Query Subscriptions

```
GET /api/v1/subscriptions
```

Lists all active subscriptions across the cluster.

### Get Client Subscriptions

```
GET /api/v1/subscriptions/{clientid}
```

---

## 5. Routes

### List Routes

```
GET /api/v1/routes
```

### Get Route for Topic

```
GET /api/v1/routes/{topic}
```

### List Retained Messages

```
GET /api/v1/retains
```

Query parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `topic_filter` | `string` | `#` | Topic filter, supports `#` / `+` wildcards; empty or `#` uses full pagination |
| `offset` | `usize` | `0` | Pagination offset |
| `limit` | `usize` | `max_row_limit` | Page size, clamped to `max_row_limit` |

Returns `{ "items": [...], "has_more": bool }`. The payload is base64-encoded. On the full pagination path (`topic_filter=#`) items include `remaining_ttl` (seconds); on the filter path `remaining_ttl` is `null`. Requires the `ferromq-retainer` plugin.

List endpoints also set `X-Row-Count` and `X-Truncated` response headers (see the full [HTTP API](../http-api.md) document). Failed calls return JSON `{ "code", "message" }`.

### Delete Retained Message

```
DELETE /api/v1/retains?topic={topic}
```

`topic` must be a concrete topic (wildcards `#` / `+` are rejected). Success body is the plain string `ok`.

---

## 6. MQTT Operations

### Publish Message

```
POST /api/v1/mqtt/publish
```

Request body (JSON):

```json
{
  "topic": "test/topic",
  "payload": "hello",
  "qos": 1,
  "retain": false,
  "encoding": "plain",
  "properties": {
    "user_properties": { "key": "value" }
  }
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `topic` | `string` | (required) | MQTT topic |
| `payload` | `string` | `""` | Message payload |
| `qos` | `u8` | `0` | QoS level (0, 1, 2) |
| `retain` | `bool` | `false` | Retain message |
| `encoding` | `string` | `"plain"` | `"plain"` or `"base64"` |
| `properties` | `object` | `null` | MQTT v5 properties |

### Subscribe

```
POST /api/v1/mqtt/subscribe
```

```json
{
  "clientid": "client1",
  "topic": "test/#",
  "qos": 1
}
```

### Unsubscribe

```
POST /api/v1/mqtt/unsubscribe
```

```json
{
  "clientid": "client1",
  "topic": "test/#"
}
```

---

## 7. Plugin Management

### List All Plugins

```
GET /api/v1/plugins
```

Returns all plugins across all cluster nodes.

### List Node Plugins

```
GET /api/v1/plugins/{node}
```

### Get Plugin Info

```
GET /api/v1/plugins/{node}/{plugin}
```

### Get Plugin Config

```
GET /api/v1/plugins/{node}/{plugin}/config
GET /api/v1/plugins/{node}/{plugin}/config?reveal=1   # admin only
```

Secrets (`password` / `token` / `private_key` / `secret` / `jwt`) are redacted unless `reveal=1` and admin. Body stays a bare JSON object.

### Write / validate / version plugin config (P4)

```
PUT  /api/v1/plugins/{node}/{plugin}/config            # ?apply=reload|none
POST /api/v1/plugins/{node}/{plugin}/config/validate
GET  /api/v1/plugins/{node}/{plugin}/config/versions
POST /api/v1/plugins/{node}/{plugin}/config/rollback/{version}
```

JSON object, `{ "toml": "..." }`, or raw TOML. Atomic write + last-N backups. `effective`: `hot` (applied via plugin `load_config`, not a ferromqd restart), `reload` (call PUT .../config/reload), `restart_required` (process restart). See the full [HTTP API](../http-api.md) document for curl examples.

### Reload Plugin Config

```
PUT /api/v1/plugins/{node}/{plugin}/config/reload
```

### Load Plugin

```
PUT /api/v1/plugins/{node}/{plugin}/load
```

### Unload Plugin

```
PUT /api/v1/plugins/{node}/{plugin}/unload
```

---

## 8. Statistics

### Node Stats

```
GET /api/v1/stats
GET /api/v1/stats/{id}
GET /api/v1/stats/sum
```

### System Stats

```
GET /api/v1/stats/sys
GET /api/v1/stats/sys/{id}
GET /api/v1/stats/sys/sum
```

---

## 9. Metrics

### JSON Metrics

```
GET /api/v1/metrics
GET /api/v1/metrics/{id}
GET /api/v1/metrics/sum
```

### Prometheus Metrics

```
GET /api/v1/metrics/prometheus
GET /api/v1/metrics/prometheus/{id}
GET /api/v1/metrics/prometheus/sum
```

Returns metrics in Prometheus text format (`text/plain`).

---

## License

MIT OR Apache-2.0
