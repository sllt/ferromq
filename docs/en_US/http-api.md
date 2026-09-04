English | [简体中文](../zh_CN/http-api.md)

# HTTP API

FerroMQ Broker provides HTTP APIs for integration with external systems, such as querying client information, publishing messages.

FerroMQ Broker's HTTP API service listens on port 6060 by default. You can modify the listening port through the configuration file of `plugins/ferromq-http-api.toml`. All API calls start with `api/v1`.

#### Plugins:

```bash
ferromq-http-api
```

#### Plugin configuration file:

```bash
plugins/ferromq-http-api.toml
```

#### Plugin configuration options:

```bash
##--------------------------------------------------------------------
## ferromq-http-api
##--------------------------------------------------------------------

# See more keys and their definitions at https://github.com/rmqtt/rmqtt/blob/master/docs/en_US/http-api.md

## Max Row Limit
max_row_limit = 10_000
## HTTP Listener address
http_laddr = "0.0.0.0:6060"
## HTTP bearer token for API authentication.
## When set, all HTTP API requests must include an `Authorization: Bearer <token>` header.
## When not set (default), no authentication is required.
#http_bearer_token = "public"

## Dashboard session login (P3a). When `dashboard_admin_password` is set,
## the first `POST /api/v1/auth/login` (or `POST /api/v1/auth/init`) creates
## an admin user and stores a durable bcrypt hash. The raw `http_bearer_token`
## remains a superuser/operator credential for automation.
# dashboard_admin_username = "admin"
# dashboard_admin_password = "change-me"
# dashboard_viewer_username = "viewer"
# dashboard_viewer_password = "change-me-too"
# dashboard_auth_file = "/var/lib/ferromq/dashboard-auth.json"
# dashboard_allow_anonymous_admin = false  ## unsafe legacy opt-in
# dashboard_cookie_secure = false          ## set true when serving HTTPS
# dashboard_session_idle_timeout = "30m"
# dashboard_session_max_age = "12h"
# dashboard_login_rate_limit = 10
# dashboard_login_rate_window = "1m"
## Cookie CSRF compares Origin/Referer to Host. Reverse proxies must
## preserve the original public Host. X-Forwarded-Host is not trusted.
## If Host is rewritten, use Bearer / API key for non-browser clients.
## In-memory audit ring buffer (newest first). Optional JSONL file:
# audit_max_events = 10000
# audit_file = "/var/log/ferromq/http-api-audit.jsonl"
# config_history_keep = 10
# broker_config_file = "ferromq.toml"
## Users/API-key hashes are persisted; sessions remain process-local.
## Cluster: each node has its own auth store and needs sticky sessions.

## Enable TCP SO_REUSEADDR on the HTTP listener.
## Default: true
# http_reuseaddr = true

## Enable TCP SO_REUSEPORT on the HTTP listener.
## Default: false
# http_reuseport = false

## Print HTTP request method/path. Bodies on /auth/* are omitted; other
## JSON/TOML bodies have secret fields and URL userinfo redacted.
## Authorization / Cookie headers are never logged.
http_request_log = false

## Metrics sample interval for collecting and caching internal metrics.
## Default: "5s"
# metrics_sample_interval = "5s"

## gRPC message type identifier for HTTP API messages.
## Default: 99
# message_type = 99

##Message expiration time, 0 means no expiration
message_expiry_interval = "5m"

## Prometheus metrics data caching interval.
## Default: "5s"
prometheus_metrics_cache_interval = "5s"

## Dashboard static directory (optional).
## By default the React console is rust-embedded from crate-local
## dashboard-dist/ (https://github.com/sllt/ferromq-dashboard).
## If set AND the path exists, that directory is served at `/dashboard/`
## instead (typically a local Vite dist/). Missing path → embed.
# dashboard_static_dir = "/path/to/ferromq-dashboard/dist"

##─── Stats/Metrics History Persistence (optional) ───────────────────────
## When `storage` is configured, the plugin periodically snapshots Stats
## and Metrics, converts them to JSON, and writes them to the backend with
## TTL-based expiration. History query APIs
## (`/api/v1/stats/history`, `/api/v1/metrics/history`, etc.) become available.
## To disable, omit the entire `storage` section.

##─── Redb backend ──────────────────────────────────────────────────────
storage.type = "redb"
storage.redb.path = "/var/log/ferromq/.cache/http-api-history/{node}.redb"

##─── Sled backend ──────────────────────────────────────────────────────
#storage.type = "sled"
#storage.sled.path = "/var/log/ferromq/.cache/http-api-history/{node}.sled"
#storage.sled.cache_capacity = "1G"

##─── Redis backend ──────────────────────────────────────────────────────
# storage.type = "redis"
# storage.redis.url = "redis://127.0.0.1:6379/"
# storage.redis.prefix = "http-api-history-{node}"

##─── Redis Cluster backend ──────────────────────────────────────────────
# storage.type = "redis-cluster"
# storage.redis-cluster.urls = ["redis://127.0.0.1:6380/", "redis://127.0.0.1:6381/"]
# storage.redis-cluster.prefix = "http-api-history-{node}"

##─── Flush interval (how often to snapshot Stats/Metrics) ───────────────
## Default: "5s"
# flush_interval = "5s"

##─── History retention (TTL for each data point) ────────────────────────
## Default: "7d"
# history_retention = "7d"
```

## Response code

### HTTP status codes

The FerroMQ Broker interface always returns 200 OK when the call is successful, and the response content is returned in JSON format.

The possible status codes are as follows:

| Status Code | Description |
| ---- | ----------------------- |
| 200  | Succeed, and the returned JSON data will provide more information |
| 400  | Invalid client request, such as wrong request body or parameters |
| 401  | Client authentication failed, maybe because of invalid authentication credentials |
| 403  | Authenticated but the role cannot perform this action (`viewer` / `operator` / `admin`) |
| 404  | The requested path cannot be found or the requested object does not exist |
| 409  | Dashboard users already initialized (`POST /auth/init`) |
| 429  | Login rate limit exceeded |
| 500  | An internal error occurred while the server was processing the request |

Failed API calls return **JSON** (not plain text / HTML):

```json
{"code": 404, "message": "plugin not found: ferromq-web-hook", "request_id": "19c3e2a1b-5a5a"}
```

| Field   | Type    | Description                                      |
|---------|---------|--------------------------------------------------|
| `code`  | Integer | Same value as the HTTP status code               |
| `message` | String | Human-readable error description               |
| `details` | Any   | Optional structured extras (omitted when unused) |
| `request_id` | String | Correlation id (also sent as `X-Request-Id`)  |

Send `X-Request-Id` on the request to have it echoed; otherwise the server generates one. Successful `2xx` bodies are unchanged (including existing plain-text successes such as `ok`).

Machine-readable contract: `GET /api/v1/openapi.json` (Swagger UI at `GET /api/v1/docs`).

## Authentication (P3a session + P3b API keys)

Credentials accepted on the HTTP API. **MQTT client auth plugins are unchanged.**

| Mechanism | How | Role |
|-----------|-----|------|
| Session cookie | `POST /api/v1/auth/login` with `{ "username", "password" }` sets `ferromq_session` (`HttpOnly`, `SameSite=Lax`, `Secure` when `dashboard_cookie_secure`). Each request uses the **live** user role (and `enabled`); a deleted user invalidates the session. Cookie-authenticated `POST`/`PUT`/`PATCH`/`DELETE` require `Origin`/`Referer` to match `Host` when those headers are present (missing Origin is allowed for non-browser clients). Bearer / API keys skip this check. Reverse proxies must **preserve the original public `Host`**; FerroMQ does **not** trust `X-Forwarded-Host`. If the proxy rewrites `Host`, use Bearer / API key for non-browser clients. | `admin`, `operator`, or `viewer` from the **current** user record |
| Static Bearer | `Authorization: Bearer <http_bearer_token>` | Always `admin` (username `operator`) for automation |
| API key Bearer | `Authorization: Bearer <fmqk_…>` created via `POST /api/v1/api-keys` | Bound role (`admin` / `operator` / `viewer`) |
| Open access | Neither bearer, API keys, nor `dashboard_admin_password` is set, and no persisted users exist | Anonymous `viewer` (read-only). Anonymous admin requires the unsafe compatibility option |

Public without a session/bearer: `POST /auth/login`, `POST /auth/logout`, `POST /auth/init`, `GET /health/check`, `GET /openapi.json`, `GET /docs`.

### Roles

| Role | Read | Kick / publish / plugin config / P5 integrations / P6 alarm ack + cluster writes | Users / API keys / audit / broker write / `?reveal=1` / **`ferromq-http-api` config** |
|------|------|--------------------------------|------------------------------------------------------|
| `admin` | yes | yes | yes |
| `operator` | yes | yes (except `ferromq-http-api` write/validate/rollback/reload) | no (`403`, `required_role: admin`) |
| `viewer` | yes (secrets redacted) | no (`403`, `required_role: operator`) | no |

`PUT`/`POST` of plugin `ferromq-http-api` (including validate / rollback / reload) is **admin-only**: that file can set `http_bearer_token`, which authenticates as admin. Generic plugin PUTs **deep-merge** into the existing TOML so omitted or `***` secrets (`hmac_secret`, `password`, …) are preserved and never written as the literal `***`.

Passwords are **bcrypt** hashes; API key secrets are **SHA-256** hashes. User/key hashes are stored in `dashboard_auth_file`, or `.ferromq-dashboard-auth.json` under the plugin config directory when unset. Sessions remain process-local. Each cluster node has its own auth store, so use sticky sessions for browser traffic.

### How to test

```bash
# One-time bootstrap from ferromq-http-api.toml (optional; first login also bootstraps)
curl -sS -X POST http://127.0.0.1:6060/api/v1/auth/init

# Login — save the session cookie
curl -sS -c cookie.txt -X POST http://127.0.0.1:6060/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"change-me"}'

# Current user
curl -sS -b cookie.txt http://127.0.0.1:6060/api/v1/auth/me

# Create an operator user
curl -sS -b cookie.txt -X POST http://127.0.0.1:6060/api/v1/users \
  -H 'Content-Type: application/json' \
  -d '{"username":"ops","password":"ops-secret-1","role":"operator"}'

# Create an API key (secret is shown once)
curl -sS -b cookie.txt -X POST http://127.0.0.1:6060/api/v1/api-keys \
  -H 'Content-Type: application/json' \
  -d '{"name":"ci","role":"operator"}'
# {"id":"…","name":"ci","role":"operator","secret":"fmqk_…",…}

# Use the API key as Bearer (operator can kick, cannot list users)
curl -sS -H 'Authorization: Bearer fmqk_…' \
  -X DELETE http://127.0.0.1:6060/api/v1/clients/demo

# Audit log (admin)
curl -sS -b cookie.txt 'http://127.0.0.1:6060/api/v1/audit?format=page&_limit=20'

# Static Bearer token still works as superuser admin
curl -sS -H 'Authorization: Bearer public' http://127.0.0.1:6060/api/v1/brokers

# Logout
curl -sS -b cookie.txt -c cookie.txt -X POST http://127.0.0.1:6060/api/v1/auth/logout
```

### Dashboard frontend

1. Prefer `POST /api/v1/auth/login` and send `credentials: 'include'` on every `/api/v1` `fetch`.
2. On boot, call `GET /api/v1/auth/me`. `200` means the user is in. `401` → login page.
3. Keep `Authorization: Bearer <token>` as an optional fallback (static token or API key).
4. Gate kick / publish / plugin-load UI on `role !== 'viewer'`. Gate users / API keys / audit on `role === 'admin'`.
5. Do not store the password. The session id lives only in the HttpOnly cookie. Show an API-key secret only once after create.

## List pagination metadata

List endpoints used by the dashboard (`GET /clients`, `/clients/offlines`, `/subscriptions`, `/routes`, `/retains`, `/plugins`, `/plugins/{node}`) keep their existing JSON schema **by default**.

- **Default body:** a bare JSON array (except `/retains`, which already returns `{ items, has_more }`).
- **`?format=page` (optional, non-breaking):** wrap the same rows as `{ "items": [...], "offset": N, "limit": N, "truncated": bool, "total": N? }`. `total` is omitted when the backend does not know the full count. `/retains?format=page` keeps `has_more` and adds `offset` / `limit` / `truncated`.
- **`_limit` / `limit`:** maximum rows returned. If omitted or `0`, the plugin `max_row_limit` is used (`10000` by default). `/retains` uses `limit` (no underscore).
- **`_offset` / `offset`:** optional skip count applied after the backend fetch. `/retains` already documents `offset`.
- **Response headers (non-breaking):**
  - `X-Row-Count`: number of rows in this response (`items.length` for `/retains` and `format=page`).
  - `X-Truncated`: `true` when the result was cut off by `_limit` / `max_row_limit` (or `has_more` for `/retains`); otherwise `false`.
  - `X-Request-Id`: correlation id.

Old clients that ignore unknown headers and do not send `format=page` continue to work.

## Cluster partial failure

Cluster-aggregating endpoints (`/brokers`, `/nodes`, `/features`, `/plugins`, `/stats`, `/metrics`, and their `/sum` variants) return **HTTP 200** with a per-node success/failure object when some peers are unreachable. They do **not** collapse the cluster view into a single boolean.

Success item (extra `ok: true` is additive):

```json
{ "ok": true, "node_id": 1, "...": "endpoint-specific fields" }
```

Failure item (replaces the old bare error string):

```json
{ "ok": false, "node_id": 2, "error": "connection refused" }
```

`/plugins` uses `{ ok, node, plugins, error? }` (`plugins` is `[]` on failure). `/stats` and `/metrics` use `{ ok, node: { id }, stats|metrics?, error? }`. `/features` uses `FeaturesNodeResult` (see below) plus `failed_count` / `partial` / `enabled`.

## API Endpoints

## /api/v1

### GET /api/v1

Return all Endpoints supported by FerroMQ Broker.

**Parameters:** None

**Success Response Body (JSON):**

| Name             | Type |  Description   |
|------------------| --------- | -------------- |
| []             | Array     | Endpoints list |
| - [0].path   | String    | Endpoint       |
| - [0].name   | String    | Endpoint name    |
| - [0].method | String    | HTTP Method    |
| - [0].descr  | String    | Description      |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1"

[{"descr":"Return the basic information of all nodes in the cluster","method":"GET","name":"get_brokers","path":"/brokers/{node}"}, ...]

```

## Broker Basic Information

### GET /api/v1/brokers/{node}

Return basic information of all nodes in the cluster.

**Path Parameters:**

| Name | Type | Required | Description                                                                  |
| ---- | --------- | ------------|------------------------------------------------------------------------------|
| node | Integer    | False       | Node ID, such as 1. <br/>If not specified, returns all node basic information |

**Success Response Body (JSON):**

| Name           | Type | Description                                                                                                              |
|----------------| --------- |--------------------------------------------------------------------------------------------------------------------------|
| {}/[]          | Object/Array of Objects | Returns the information of the specified node when the parameter exists, <br/>otherwise, returns the information of all nodes |
| .datetime      | String    | Current time, in the format of "YYYY-MM-DD HH:mm:ss"                        |
| .node_id       | Integer    | Node ID                                                                      |
| .node_name     | String    | Node name                                                                        |
| .running       | Bool    | Node is healthy                                                  |
| .sysdescr      | String    | Software description                                                               |
| .uptime        | String    | FerroMQ Broker runtime, in the format of "D days, H hours, m minutes, s seconds"     |
| .version       | String    | FerroMQ Broker version                                                 |
| .rustc_version | String    | RUSTC version                                         |
| .cluster       | Object    | Additive P6 topology (`mode`, `plugin`, `leader_id`, `role`, `peers`). Absent on failed peer objects. |

**Examples:**

Get the basic information of all nodes:

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/brokers"

[{"datetime":"2022-07-24 23:01:31","node_id":1,"node_name":"1@127.0.0.1","running":true,"sysdescr":"FerroMQ Broker","uptime":"5 days 23 hours, 16 minutes, 3 seconds","version":"ferromq/0.21.0"}]
```

Get the basic information of node 1 :

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/brokers/1"

{"datetime":"2022-07-24 23:01:31","node_id":1,"node_name":"1@127.0.0.1","running":true,"sysdescr":"FerroMQ Broker","uptime":"5 days 23 hours, 17 minutes, 15 seconds","version":"ferromq/0.21.0"}
```

## Node

### GET /api/v1/nodes/{node}

Return the status of the node.

**Path Parameters:**

| Name | Type | Required | Description                                                             |
| ---- | --------- | ------------|-------------------------------------------------------------------------|
| node | Integer    | False       | Node ID, such as 1. <br/>If not specified, returns all node information |

**Success Response Body (JSON):**

| Name           | Type                    | Description                                                                                                         |
|----------------|-------------------------|---------------------------------------------------------------------------------------------------------------------|
| {}/[]          | Object/Array of Objects | Returns node information when node parameter exists,<br/>otherwise, returns information about all nodes in an Array |
| .boottime      | String                  | OS startup time                                                                                                     |
| .connections   | Integer                 | Number of clients currently connected to this node                                                                  |
| .disk_free     | Integer                 | Disk usable capacity (bytes)                                                                                        |
| .disk_total    | Integer                 | Total disk capacity (bytes)                                                                                         |
| .load1         | Float                   | CPU average load in 1 minute                                                                                        |
| .load5         | Float                   | CPU average load in 5 minute                                                                                        |
| .load15        | Float                   | CPU average load in 15 minute                                                                                       |
| .memory_free   | Integer                 | System free memory size (bytes)                                                                                     |
| .memory_total  | Integer                 | Total system memory size (bytes)                                                                                    |
| .memory_used   | Integer                 | Used system memory size (bytes)                                                                                     |
| .node_id       | Integer                 | Node ID                                                                                                             |
| .node_name     | String                  | Node name                                                                                                           |
| .running       | Bool                    | Node is healthy                                                                                                   |
| .uptime        | String                  | FerroMQ Broker runtime, in the format of "D days, H hours, m minutes, s seconds"                                                                                                          |
| .version       | String                  | FerroMQ Broker version                                                                                                            |
| .rustc_version | String                  | RUSTC version                                         |
| .cluster       | Object                  | Additive P6 topology (`mode`, `plugin`, `leader_id`, `role`, `peers`) |

**Examples:**

Get the status of all nodes:

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/nodes"

[{"boottime":"2022-06-30 05:20:24 UTC","connections":1,"disk_free":77382381568,"disk_total":88692346880,"load1":0.0224609375,"load15":0.0,"load5":0.0263671875,"memory_free":1457954816,"memory_total":2084057088,"memory_used":626102272,"node_id":1,"node_name":"1@127.0.0.1","running":true,"uptime":"5 days 23 hours, 33 minutes, 0 seconds","version":"ferromq/0.21.0","rustc_version":"1.85.0"}]
```

Get the status of the specified node:

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/nodes/1"

{"boottime":"2022-06-30 05:20:24 UTC","connections":1,"disk_free":77382381568,"disk_total":88692346880,"load1":0.0224609375,"load15":0.0,"load5":0.0263671875,"memory_free":1457954816,"memory_total":2084057088,"memory_used":626102272,"node_id":1,"node_name":"1@127.0.0.1","running":true,"uptime":"5 days 23 hours, 33 minutes, 0 seconds","version":"ferromq/0.21.0","rustc_version":"1.85.0"}
```

## Feature Support

### GET /api/v1/features

Returns the feature support state of every cluster node plus a cluster-wide consistency summary. The support state is determined by each feature's trait implementation (`enable()` / `is_supported()`).

**Parameters:** none

**Success Response Body (JSON):**

| Name          | Type | Description |
|---------------|------|-------------|
| consistent    | Bool | Whether all reachable nodes have identical feature states; `false` indicates node configuration drift or plugin load failure |
| node_count    | Integer | Number of nodes that successfully reported features |
| failed_count  | Integer | Number of nodes that failed to report |
| partial       | Bool | `true` when `failed_count > 0` (HTTP 200 with a partial cluster view) |
| enabled       | Object | OR of each flag across reachable nodes. Intended for dashboard **menu gating** (show a page when any node supports it) |
| - enabled.retain | Bool | Retained messages (`ferromq-retainer`). Gates the Retains menu |
| - enabled.message_storage | Bool | Persistent message storage |
| - enabled.session_storage | Bool | Persistent session storage |
| - enabled.delayed | Bool | Delayed publish (`$delayed/...`) |
| - enabled.shared_subscription | Bool | Shared subscriptions `$share` |
| - enabled.auto_subscription | Bool | Automatic subscriptions |
| conflicts     | Array | Fields with inconsistent values (grouped by value with node ids); empty when `consistent` is `true` |
| - conflicts[i].feature | String | Feature name, e.g. `retain` |
| - conflicts[i].values  | Array | Value groups, each containing `value` (Bool) and `node_ids` (Integer Array) |
| nodes         | Array | Per-node **structured** success/failure (`FeaturesNodeResult`) |
| - nodes[i].ok         | Bool | `true` on success |
| - nodes[i].node_id    | Integer | Node ID |
| - nodes[i].node_name  | String | Node name (success only) |
| - nodes[i].features   | Object | Six feature flags (success only) |
| - nodes[i].error      | String | Failure reason (failure only) |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/features"
```

```json
{
  "consistent": false,
  "node_count": 3,
  "failed_count": 0,
  "partial": false,
  "enabled": {
    "retain": true,
    "message_storage": false,
    "session_storage": false,
    "delayed": true,
    "shared_subscription": true,
    "auto_subscription": false
  },
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
      "ok": true,
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

> Note: when an inconsistency is detected, the backend emits a `features inconsistent across cluster` warning log. For a single node, use `GET /api/v1/features/{node}`, which returns the node's `FeaturesInfo` object directly (without the consistency summary).

### GET /api/v1/features/{node}

Returns the feature support state of a specific node.

**Path Parameters:**

| Name | Type | Required | Description |
| ---- | --------- | --------|-------------|
| node | Integer    | True     | Node ID, e.g. 1 |

**Success Response Body (JSON):**

| Name          | Type | Description |
|---------------|------|-------------|
| node_id       | Integer | Node ID |
| node_name     | String | Node name |
| features      | Object | Six feature flags: `retain`, `message_storage`, `session_storage`, `delayed`, `shared_subscription`, `auto_subscription` |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/features/1"

{"node_id":1,"node_name":"1@127.0.0.1","features":{"retain":true,"message_storage":false,"session_storage":false,"delayed":true,"shared_subscription":true,"auto_subscription":false}}
```

## Health Check

### GET /api/v1/health/check

Returns the health status of all nodes in the cluster.

**Parameters:** None

**Success Response Body (JSON):**

| Name                      | Type             | Description          |
|---------------------------|------------------|----------------------|
| {}                        | Object           | Health check information |
| {}.status                 | String           | Overall cluster status: "Running" or "Degraded" |
| {}.nodes                  | Object           | Per-node health status, key is Node ID |
| {}.nodes.{id}             | Json Object      | Detailed health status for the node |
| {}.nodes.{id}.name        | String           | Node name               |
| {}.nodes.{id}.running     | Bool             | Whether the node is running |
| {}.nodes.{id}.uptime      | String           | Node uptime             |
| {}.nodes.{id}.status      | String           | Node status             |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/health/check"

{"status":"Running","nodes":{"1":{"name":"1@127.0.0.1","running":true,"uptime":"5d 23h 33m","status":"Running"}}}
```

### GET /api/v1/health/check/{node}

Queries the health status of a specific node.

**Path Parameters:**

| Name | Type    | Required | Description        |
|------|---------|----------|--------------------|
| node | Integer | True     | Node ID, e.g., 1 |

**Success Response Body (JSON):**

| Name    | Type    | Description        |
|---------|---------|--------------------|
| {}      | Object  | Node health status |
| .name   | String  | Node name          |
| .running| Bool    | Whether the node is running |
| .uptime | String  | Node uptime        |
| .status | String  | Node status        |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/health/check/1"

{"name":"1@127.0.0.1","running":true,"uptime":"5d 23h 33m","status":"Running"}
```

## Client

### GET /api/v1/clients

<span id = "get-clients" />

Returns the information of all clients under the cluster.

**Query String Parameters:**

| Name   | Type | Required | Default | Description                                                                                                                                                             |
| ------ | --------- | -------- | ------- |-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| _limit | Integer   | False | 10000   | The maximum number of data items returned at one time. Alias: `limit`. If omitted or `0`, uses `max_row_limit` from `ferromq-http-api.toml` |
| _offset | Integer  | False | 0       | Number of matching rows to skip. Alias: `offset`. Applied after the backend fetch (capped by `max_row_limit`) |

| Name            | Type   | Required | Description                     |
| --------------- | ------ | -------- |---------------------------------|
| clientid        | String | False    | Client identifier, exact match                    |
| username        | String | False    | Client username, exact match                         |
| ip_address      | String | False    | Client IP address, exact match                      |
| connected       | Bool   | False    | The current connection status of the client     |
| clean_start     | Bool   | False    | Whether the client uses a new session            |
| session_present | Bool   | False    | Whether the client is connected to an existing session    |
| proto_ver       | Integer| False    | Client protocol version, 3/4/5             |
| _like_clientid  | String | False    | Fuzzy search of client identifier by substring method                  |
| _like_username  | String | False    | Client user name, fuzzy search by substring                 |
| _gte_created_at | String | False    | Search client session creation time by greater than or equal method.<br/>Format: `"YYYY-MM-DD HH:mm:ss"` (e.g. `"2026-07-29 21:25:37"`),<br/>also accepts a Unix timestamp in seconds (e.g. `1690000000`) |
| _lte_created_at | String | False    | Search client session creation time by less than or equal method.<br/>Format: `"YYYY-MM-DD HH:mm:ss"` (e.g. `"2026-07-29 21:25:37"`),<br/>also accepts a Unix timestamp in seconds (e.g. `1690000000`) |
| _gte_connected_at | String | False    | Search client connection creation time by greater than or equal method.<br/>Format: `"YYYY-MM-DD HH:mm:ss"` (e.g. `"2026-07-29 21:25:37"`),<br/>also accepts a Unix timestamp in seconds (e.g. `1690000000`) |
| _lte_connected_at | String | False    | Search client connection creation time by less than or equal method.<br/>Format: `"YYYY-MM-DD HH:mm:ss"` (e.g. `"2026-07-29 21:25:37"`),<br/>also accepts a Unix timestamp in seconds (e.g. `1690000000`) |
| _gte_mqueue_len | Integer| False    | Current length of message queue by greater than or equal method  |
| _lte_mqueue_len | Integer| False    | Current length of message queue by less than or equal method |

**Success Response Body (JSON):**

| Name                    | Type             | Description                                                                                                                       |
|-------------------------|------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| []                      | Array of Objects | Information for all clients                                                                                                       |
| [0].node_id             | Integer          | ID of the node to which the client is connected                                                                                   |
| [0].clientid            | String           | Client identifier                                                                                                                 |
| [0].username            | String           | User name of client when connecting                                                                                               |
| [0].superuser           | Boolean          | Whether the client is a superuser                                                                                                 |
| [0].proto_ver           | Integer          | Protocol version used by the client                                                                                               |
| [0].ip_address          | String           | Client's IP address                                                                                                               |
| [0].port                | Integer          | Client port                                                                                                                       | 
| [0].connected_at        | String           | Client connection time, in the format of "YYYY-MM-DD HH:mm:ss"                                                                    |
| [0].disconnected_at     | String           | Client offline time, in the format of "YYYY-MM-DD HH:mm:ss"，<br/>This field is only valid and returned when `connected` is `false` |
| [0].disconnected_reason | String           | Client offline reason                                                                                                             |
| [0].connected           | Boolean          | Whether the client is connected                                                                                                   |
| [0].keepalive           | Integer          | keepalive time, with the unit of second                                                                                           |
| [0].clean_start         | Boolean          | Indicate whether the client is using a brand new session                                                                          |
| [0].session_present     | Boolean          | Whether the client is connected to an existing session                                                                            |
| [0].expiry_interval     | Integer          | Session expiration interval, with the unit of second                                                                              |
| [0].created_at          | String           | Session creation time, in the format "YYYY-MM-DD HH:mm:ss"                                                                        |
| [0].subscriptions_cnt   | Integer          | Number of subscriptions established by this client                                                                                |
| [0].max_subscriptions   | Integer          | Maximum number of subscriptions allowed by this client                                                                            |
| [0].inflight            | Integer          | Current length of inflight                                                                                                        |
| [0].max_inflight        | Integer          | Maximum length of inflight                                                                                                        |
| [0].mqueue_len          | Integer          | Current length of message queue                                                                                                   |
| [0].max_mqueue          | Integer          | Maximum length of message queue                                                                                                   |
| [0].last_will           | Json             | Last Will Message, for example: { "message": "dGVzdCAvdGVzdC9sd3QgLi4u", "qos": 1, "retain": false, "topic": "/test/lwt" }        |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/clients?_limit=10"

[{"clean_start":true,"session_present":true,"clientid":"be82ee31-7220-4cad-a724-aaad9a065012","connected":true,"connected_at":"2022-07-30 18:14:08","created_at":"2022-07-30 18:14:08","disconnected_at":"","expiry_interval":7200,"inflight":0,"ip_address":"183.193.169.110","keepalive":60,"max_inflight":16,"max_mqueue":1000,"max_subscriptions":0,"mqueue_len":0,"node_id":1,"port":10839,"proto_ver":4,"subscriptions_cnt":0,"superuser":false,"username":"undefined"}]
```

### GET /api/v1/clients/{clientid}

Returns information for the specified client

**Path Parameters:**

| Name   | Type | Required | Description |
| ------ | --------- | -------- |  ---- |
| clientid  | String | True | ClientID |

**Success Response Body (JSON):**

| Name | Type | Description |
|------| --------- | ----------- |
| {}   | Object | Client information, for details, see<br/>[GET /api/v1/clients](#get-clients)|

**Examples:**

Query the specified client

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/clients/example1"

{"clean_start":true,"session_present":true,"clientid":"example1","connected":true,"connected_at":"2022-07-30 23:30:43","created_at":"2022-07-30 23:30:43","disconnected_at":"","expiry_interval":7200,"inflight":0,"ip_address":"183.193.169.110","keepalive":60,"max_inflight":16,"max_mqueue":1000,"max_subscriptions":0,"mqueue_len":0,"node_id":1,"port":11232,"proto_ver":4,"subscriptions_cnt":0,"superuser":false,"username":"undefined"}
```

### GET /api/v1/clients/offlines

Returns information of all offline clients under the cluster. Parameters and response are identical to [GET /api/v1/clients](#get-clients), but only returns clients where `connected` is `false`.

**Query String Parameters:** Same as [GET /api/v1/clients](#get-clients)

**Success Response Body (JSON):** Same as [GET /api/v1/clients](#get-clients)

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/clients/offlines"

[{"clean_start":false,"session_present":false,"clientid":"example1","connected":false,"connected_at":"","created_at":"2022-07-30 18:14:08","disconnected_at":"2022-07-30 23:30:43","disconnected_reason":"normal","expiry_interval":7200,"inflight":0,"ip_address":"183.193.169.110","keepalive":60,"max_inflight":16,"max_mqueue":1000,"max_subscriptions":0,"mqueue_len":0,"node_id":1,"port":10839,"proto_ver":4,"subscriptions_cnt":0,"superuser":false,"username":"undefined"}]
```

### DELETE /api/v1/clients/{clientid}

Kick out the specified client. Note that this operation will terminate the connection with the session.

**Path Parameters:**

| Name   | Type | Required | Description |
| ------ | --------- | -------- |  ---- |
| clientid  | String | True | ClientID |

**Success Response Body (String):**

Returns a raw string representing the connection unique ID, formatted as `{node_id}@{ip}:{port}/{clientid}/{username}`.

**Examples:**

Kick out the specified client

```bash
$ curl -i -X DELETE "http://localhost:6060/api/v1/clients/example1"

1@10.0.4.6:1883/183.193.169.110:10876/example1/dashboard
```

### DELETE /api/v1/clients/offlines

Batch kick all offline clients matching the search criteria.

**Query String Parameters:** Same as [GET /api/v1/clients](#get-clients) (note: `connected` parameter will be forced to `false`)

**Success Response Body (JSON):**

| Name    | Type    | Description          |
|---------|---------|----------------------|
| count   | Integer | Number of clients kicked out |

**Examples:**

```bash
$ curl -i -X DELETE "http://localhost:6060/api/v1/clients/offlines?clientid=example1"

{"count":1}
```

### GET /api/v1/clients/{clientid}/online

Check if the client is online

**Path Parameters:**

| Name   | Type | Required | Description |
| ------ | --------- | -------- |  ---- |
| clientid  | String | True | ClientID |

**Success Response Body (JSON):**

Returns a raw boolean value `true` or `false` indicating whether the client is online.

**Examples:**

Check if the client is online

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/clients/example1/online"

false
```

## Subscription Information

### GET /api/v1/subscriptions

Returns all subscription information under the cluster.

**Query String Parameters:**

| Name   | Type | Required | Default | Description                                                                                                  |
| ------ | --------- | -------- | ------- |--------------------------------------------------------------------------------------------------------------|
| _limit | Integer   | False | 10000   | The maximum number of data items returned at one time. Alias: `limit`. If omitted or `0`, uses `max_row_limit` from `ferromq-http-api.toml` |
| _offset | Integer  | False | 0       | Number of matching rows to skip. Alias: `offset`. Applied after the backend fetch (capped by `max_row_limit`) |

| Name         | Type    | Description |
| ------------ | ------- | ----------- |
| clientid     | String  | Client identifier, exact match    |
| topic        | String  | Topic, exact match  |
| qos          | Enum    | Possible values are `0`,`1`,`2` |
| share        | String  | Shared subscription group name |
| _match_topic | String  | Topic, wildcard match query |

**Success Response Body (JSON):**

| Name            | Type             | Description |
|-----------------|------------------|-------------|
| []              | Array of Objects | All subscription information      |
| [0].node_id     | Integer          | Node ID     |
| [0].clientid    | String           | Client identifier      |
| [0].client_addr | String           | Client IP address and port  |
| [0].topic       | String           | Subscribe to topic        |
| [0].qos         | Integer          | QoS level (alias of `opts.qos`, added for dashboard compatibility) |
| [0].share       | String           | Shared subscription group name (alias of `opts.group`) |
| [0].opts        | Object           | Subscription options; the embedded dashboard reads `opts.qos` / `opts.group` |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/subscriptions?_limit=10"

[{"node_id":1,"clientid":"example1","topic":"foo/#","qos":2,"share":null},{"node_id":1,"clientid":"example1","topic":"foo/+","qos":2,"share":"test"}]
```

### GET /api/v1/subscriptions/{clientid}

Return the subscription information of the specified client in the cluster.

**Path Parameters:**

| Name   | Type | Required | Description |
| ------ | --------- | -------- |  ---- |
| clientid  | String | True | ClientID |

**Success Response Body (JSON):**

| Name            | Type             | Description |
|-----------------|------------------|-------------|
| []              | Array of Objects | All subscription information      |
| [0].node_id     | Integer          | Node ID     |
| [0].clientid    | String           | Client identifier      |
| [0].client_addr | String           | Client IP address and port  |
| [0].topic       | String           | Subscribe to topic        |
| [0].qos         | Integer          | QoS level      |
| [0].share       | String           | Shared subscription group name    |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/subscriptions/example1"

[{"node_id":1,"clientid":"example1","topic":"foo/+","qos":2,"share":"test"},{"node_id":1,"clientid":"example1","topic":"foo/#","qos":2,"share":null}]
```

## Routes

### GET /api/v1/routes

List all routes

**Query String Parameters:**

| Name   | Type | Required | Default | Description |
| ------ | --------- | -------- | ------- |  ---- |
| _limit | Integer   | False | 10000   | The maximum number of data items returned at one time. Alias: `limit`. If omitted or `0`, uses `max_row_limit` from `ferromq-http-api.toml` |
| _offset | Integer  | False | 0       | Number of matching rows to skip. Alias: `offset`. Applied after the backend fetch (capped by `max_row_limit`) |

**Success Response Body (JSON):**

| Name          | Type | Description |
|---------------| --------- |-------------|
| []            | Array of Objects | All routes information      |
| [0].topic | String    | MQTT Topic  |
| [0].node_id  | Integer    | Node ID     |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/routes"

[{"node_id":1,"topic":"foo/#"},{"node_id":1,"topic":"foo/+"}]
```

### GET /api/v1/routes/{topic}

List all routes of a topic.

**Path Parameters:**

| Name   | Type | Required | Description |
| ------ | --------- | -------- |-------------|
| topic  | String   | True | Topic       |

**Success Response Body (JSON):**

| Name      | Type | Description            |
|-----------| --------- |------------------------|
| []        | Array of Objects | All routes information |
| [0].topic | String    | MQTT Topic             |
| [0].node_id | Integer    | Node ID                |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/routes/foo%2f1"

[{"node_id":1,"topic":"foo/#"},{"node_id":1,"topic":"foo/+"}]
```

## Retained Messages

### GET /api/v1/retains

Queries retained messages. Retained messages are kept synchronized across the cluster via broadcast, so querying a single node covers the whole cluster.

**Query String Parameters:**

| Name          | Type    | Required | Default       | Description |
|---------------|---------|----------|---------------|-------------|
| topic_filter  | String  | False    | `#`           | Topic filter, supports `#` / `+` wildcards; when empty or `#`, uses full pagination |
| offset        | Integer | False    | 0             | Pagination offset |
| limit         | Integer | False    | `max_row_limit` | Page size; clamped to `max_row_limit` when exceeded |

**Success Response Body (JSON):**

| Name                     | Type | Description |
|--------------------------|------|-------------|
| items                    | Array | Retained message list |
| - items[i].topic         | String | Topic |
| - items[i].msg_id        | Integer | Message ID |
| - items[i].from          | Object | Publisher info (`id.node_id` / `id.client_id`) |
| - items[i].publish       | Object | Message content, `payload` is base64-encoded |
| - items[i].publish.qos   | Integer | QoS level |
| - items[i].publish.retain | Bool | Retain flag |
| - items[i].publish.create_time | Integer | Publish time (millisecond timestamp) |
| - items[i].remaining_ttl | Integer/Null | Remaining TTL in seconds; returned by the full pagination path, `null` on the filter path |
| has_more                 | Bool | Whether more data is available |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/retains?topic_filter=%2Fiot%2Fb%2Fx&offset=0&limit=10"
```

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

> Note: the `topic_filter=#` (full) path is paginated at the storage layer and includes `remaining_ttl` (remaining seconds); the filter path is paginated in memory and `remaining_ttl` is `null`. Requires the `ferromq-retainer` plugin to be enabled.

Also returns `X-Row-Count` (length of `items`) and `X-Truncated` (same as `has_more`).

### DELETE /api/v1/retains

Delete a retained message by **exact topic**. Wildcards `#` / `+` are not allowed. Deletion follows MQTT semantics (empty-payload retain) and is broadcast to cluster peers.

**Query String Parameters:**

| Name  | Type   | Required | Description |
|-------|--------|----------|-------------|
| topic | String | True     | Concrete topic name, e.g. `/iot/b/x` |

**Responses:**

| Status | Body | Description |
|--------|------|-------------|
| 200 | `ok` (plain text) | Deleted successfully |
| 400 | `{ "code": 400, "message": "..." }` | Missing `topic`, or topic contains wildcards |
| 404 | `{ "code": 404, "message": "..." }` | No retained message for the topic |
| 503 | `{ "code": 503, "message": "..." }` | Retain storage is not enabled or unavailable |

**Examples:**

```bash
$ curl -i -X DELETE "http://localhost:6060/api/v1/retains?topic=%2Fiot%2Fb%2Fx"

ok
```

## Publish message

### POST /api/v1/mqtt/publish

Publish MQTT message.

**Parameters (json):**

| Name     | Type | Required | Default | Description                             |
| -------- | --------- | -------- |--------|-----------------------------------------|
| topic    | String    | Optional |        | For topic and topics, with at least one of them specified                  |
| topics   | String    | Optional |        | Multiple topics separated by `,`. This field is used to publish messages to multiple topics at the same time       |
| clientid | String    | Optional | system | Client identifier                            |
| payload  | String    | Required |        | Message body                                    |
| encoding | String    | Optional | plain  | The encoding used in the message body. Currently only plain and base64 are supported |
| qos      | Integer   | Optional | 0      | QoS level                                  |
| retain   | Boolean   | Optional | false  | Whether it is a retained message                                 |
| properties | Object   | Optional |        | Publish properties (MQTT v5)<br/>Optional sub-fields:<br/>- `message_expiry_interval`: Integer, message expiry interval (seconds)<br/>- `topic_alias`: Integer<br/>- `response_topic`: String<br/>- `correlation_data`: String (Base64)<br/>- `user_properties`: Object |

**Success Response Body (String):**

Returns the string `ok` on success.

**Examples:**

```bash
$ curl -i -X POST "http://localhost:6060/api/v1/mqtt/publish" --header 'Content-Type: application/json' -d '{"topic":"foo/1","payload":"Hello World","qos":1,"retain":false,"clientid":"example"}'

ok

$ curl -i -X POST "http://localhost:6060/api/v1/mqtt/publish" --header 'Content-Type: application/json' -d '{"topic":"foo/1","payload":"SGVsbG8gV29ybGQ=","qos":1,"encoding":"base64"}'

ok

$ curl -i -X POST "http://localhost:6060/api/v1/mqtt/publish" --header 'Content-Type: application/json' -d '{"topics":"foo/1,foo/2,foo/3","payload":"Hello","qos":0}'

ok

$ curl -i -X POST "http://localhost:6060/api/v1/mqtt/publish" --header 'Content-Type: application/json' -d '{"topic":"foo/1","payload":"Hello","qos":2,"retain":true,"properties":{"message_expiry_interval":3600,"response_topic":"res/foo","user_properties":{"key1":"val1"}}}'

ok
```

## Subscribe to topic

### POST /api/v1/mqtt/subscribe

Subscribe to MQTT topic

**Parameters (json):**

| Name     | Type | Required | Default | Description |
| -------- | --------- | -------- | ------- | ------------ |
| topic    | String    | Optional |         | For topic and topics, with at least one of them specified |
| topics   | String    | Optional |         | Multiple topics separated by `,`. This field is used to subscribe to multiple topics at the same time |
| clientid | String    | Required |         | Client identifier |
| qos      | Integer   | Optional | 0       | QoS level |

**Success Response Body (JSON):**

| Name    | Type       | Description                                                        |
|---------|------------|--------------------------------------------------------------------|
| {}      | Object     |                                                                    |
| {topic} | Bool / String | Key is topic name, value is the subscription result: `true`(success) / `false`(failure)<br/>When subscription fails, the value may be an error description string |

**Examples:**

Subscribe to the three topics `foo/a`, `foo/b`, `foo/c`

```bash
$ curl -i -X POST "http://localhost:6060/api/v1/mqtt/subscribe" --header 'Content-Type: application/json' -d '{"topics":"foo/a,foo/b,foo/c","qos":1,"clientid":"example1"}'

{"foo/a":true,"foo/c":true,"foo/b":true}
```

### POST /api/v1/mqtt/unsubscribe

Unsubscribe.

**Parameters (json):**

| Name     | Type | Required | Default | Description |
| -------- | --------- | -------- | ------- |-------------|
| topic    | String    | Required |         | Topic       |
| clientid | String    | Required |         | Client identifier      |

**Success Response Body:**

Returns JSON `true` when the session is on the local node; returns text `ok` when the session is on a remote node.

**Examples:**

Unsubscribe from `foo/a` topic

```bash
$ curl -i -X POST "http://localhost:6060/api/v1/mqtt/unsubscribe" --header 'Content-Type: application/json' -d '{"topic":"foo/a","clientid":"example1"}'

true
```

## plugins

### GET /api/v1/plugins

Returns information of all plugins in the cluster.

**Path Parameters:** None

**Success Response Body (JSON):**

| Name                  | Type             | Description                                                                                                         |
|-----------------------|------------------|---------------------------------------------------------------------------------------------------------------------|
| []                    | Array of Objects | All plugin information                                                                                              |
| [0].node              | Integer          | Node ID                                                                                                             |
| [0].plugins           | Array            | Plugin information, an array of objects, see below                                                                  |
| [0].plugins.name      | String           | Plugin name                                                                                                         |
| [0].plugins.version   | String           | Plugin version                                                                                                      |
| [0].plugins.descr     | String           | Plugin description                                                                                                  |
| [0].plugins.authors   | String           | Plugin authors                                                                                                      |
| [0].plugins.homepage  | String           | Plugin homepage                                                                                                     |
| [0].plugins.license   | String           | Plugin license                                                                                                      |
| [0].plugins.repository| String           | Plugin repository                                                                                                   |
| [0].plugins.active    | Boolean          | Whether the plugin is active (started). **There is no `running` field** — use `active`. |
| [0].plugins.inited    | Boolean          | Whether the plugin is initialized                                                                                   |
| [0].plugins.immutable | Boolean          | Whether the plugin is immutable, Immutable plugins will not be able to be stopped, config modified, restarted, etc. |
| [0].plugins.attrs     | Json             | Other additional properties of the plugin              |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/plugins"

[{"node":1,"plugins":[{"active":false,"attrs":null,"descr":null,"immutable":true,"inited":false,"name":"ferromq-cluster-raft","version":null},{"active":false,"attrs":null,"descr":null,"immutable":false,"inited":false,"name":"ferromq-auth-http","version":null},{"active":true,"attrs":null,"descr":"","immutable":true,"inited":true,"name":"ferromq-acl","version":"0.21.0"},{"active":true,"attrs":null,"descr":"","immutable":false,"inited":true,"name":"ferromq-counter","version":"0.21.0"},{"active":true,"attrs":null,"descr":"","immutable":false,"inited":true,"name":"ferromq-http-api","version":"0.21.0"},{"active":false,"attrs":null,"descr":null,"immutable":false,"inited":false,"name":"ferromq-web-hook","version":null},{"active":false,"attrs":null,"descr":null,"immutable":true,"inited":false,"name":"ferromq-cluster-broadcast","version":null}]}]
```

### GET /api/v1/plugins/{node}

Return the plugin information under the specified node

**Path Parameters:**

| Name | Type | Required | Description         |
| ---- | --------- |----------|---------------------|
| node | Integer    | True     | Node ID, Such as: 1 |

**Success Response Body (JSON):**

| Name           | Type             | Description                    |
|----------------|------------------|--------------------------------|
| []             | Array of Objects | Plugin information, an array of objects, see below   |
| [0].name       | String           | Plugin name                       |
| [0].version    | String           | Plugin version                      |
| [0].descr      | String           | Plugin description                  |
| [0].authors    | String           | Plugin authors                     |
| [0].homepage   | String           | Plugin homepage                    |
| [0].license    | String           | Plugin license                     |
| [0].repository | String           | Plugin repository                  |
| [0].active     | Boolean          | Whether the plugin is active                        |
| [0].inited     | Boolean          | Whether the plugin is initialized                 |
| [0].immutable  | Boolean          | Whether the plugin is immutable, Immutable plugins will not be able to be stopped, config modified, restarted, etc. |
| [0].attrs      | Json             | Other additional properties of the plugin       |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/plugins/1"

[{"active":false,"attrs":null,"descr":null,"immutable":true,"inited":false,"name":"ferromq-cluster-raft","version":null},{"active":false,"attrs":null,"descr":null,"immutable":false,"inited":false,"name":"ferromq-auth-http","version":null},{"active":true,"attrs":null,"descr":"","immutable":true,"inited":true,"name":"ferromq-acl","version":"0.21.0"},{"active":true,"attrs":null,"descr":"","immutable":false,"inited":true,"name":"ferromq-counter","version":"0.21.0"},{"active":true,"attrs":null,"descr":"","immutable":false,"inited":true,"name":"ferromq-http-api","version":"0.21.0"},{"active":false,"attrs":null,"descr":null,"immutable":false,"inited":false,"name":"ferromq-web-hook","version":null},{"active":false,"attrs":null,"descr":null,"immutable":true,"inited":false,"name":"ferromq-cluster-broadcast","version":null}]
```

### GET /api/v1/plugins/{node}/{plugin}

Returns the plugin information of the specified plugin name under the specified node.

**Path Parameters:**

| Name | Type | Required | Description |
| ---- | --------- | ------------|-------------|
| node | Integer    | True       | Node ID, Such as: 1    |
| plugin | String    | True       | Plugin name        |

**Success Response Body (JSON):**

| Name           | Type            | Description                    |
|----------------|-----------------|--------------------------------|
| {}             | Object | Plugin information      |
| {}.name       | String          | Plugin name     |
| {}.version    | String          | Plugin version                          |
| {}.descr      | String          | Plugin description               |
| {}.active     | Boolean         | Whether the plugin is active           |
| {}.inited     | Boolean         | Whether the plugin is initialized          |
| {}.immutable  | Boolean         | Whether the plugin is immutable, Immutable plugins will not be able to be stopped, config modified, restarted, etc. |
| {}.attrs      | Json            | Other additional properties of the plugin  |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/plugins/1/ferromq-web-hook"

{"active":false,"attrs":null,"descr":null,"immutable":false,"inited":false,"name":"ferromq-web-hook","version":null}
```

A missing plugin returns HTTP 404 with `{"code":404,"message":"plugin not found: ..."}` (not JSON `null`).

### GET /api/v1/plugins/{node}/{plugin}/config

Returns the plugin configuration of the specified plugin on the specified node.

Secret keys (`password`, `token`, `private_key`, `secret`, `jwt` and names that contain them) are replaced with `"***"` unless `?reveal=1` **and** the caller is `admin`. GET remains a bare JSON object (backward compatible).

**Query:** `reveal=1` — admin only (`403` otherwise).

**Examples:**

```bash
$ curl -sS "http://localhost:6060/api/v1/plugins/1/ferromq-http-api/config"
# {"http_laddr":"0.0.0.0:6060","http_bearer_token":"***",...}

$ curl -sS -b cookie.txt "http://localhost:6060/api/v1/plugins/1/ferromq-http-api/config?reveal=1"
# admin only; file contents when `{plugins.dir}/{plugin}.toml` exists
```

### PUT /api/v1/plugins/{node}/{plugin}/config

Write a plugin config file (operator+). Body is a JSON object, `{ "toml": "..." }`, or raw TOML (`Content-Type: application/toml`). The file `{plugins.dir}/{plugin}.toml` is written atomically; the previous file is copied to `{plugins.dir}/.config-history/{plugin}/{version}.toml` (last `config_history_keep`, default 10).

**Query:** `apply=reload` (default) calls the plugin `load_config` hook after the write. `apply=none` writes the file only.

**Success body:**

| Name | Type | Description |
|------|------|-------------|
| ok / written / applied | Bool | Write outcome |
| effective | String | `hot` \| `reload` \| `restart_required` (see below) |
| diff | Object | `{ added, removed, changed }` dotted keys |
| backup | String | Version id of the file replaced, if any |
| note | String | Human-readable effective-mode hint |

**`effective` semantics (honest — ferromqd is never hot-restarted):**

| Mode | Meaning |
|------|---------|
| `hot` | Already applied in this process via the plugin `load_config` hook. **Not** a `ferromqd` process restart. |
| `reload` | File is on disk. Call `PUT .../config/reload` (or re-PUT with `apply=reload`) so the plugin picks it up. |
| `restart_required` | File is on disk. The running process will not use it until `ferromqd` (or an immutable plugin on next start) is restarted. |

```bash
# Dry-run
curl -sS -X POST "http://localhost:6060/api/v1/plugins/1/ferromq-http-api/config/validate" \
  -H 'Content-Type: application/json' \
  -d '{"max_row_limit":5000,"http_laddr":"0.0.0.0:6060"}'

# Write + apply via plugin reload
curl -sS -X PUT "http://localhost:6060/api/v1/plugins/1/ferromq-http-api/config?apply=reload" \
  -H 'Content-Type: application/json' \
  -d '{"max_row_limit":5000,"http_laddr":"0.0.0.0:6060"}'

# Write only (then reload yourself)
curl -sS -X PUT "http://localhost:6060/api/v1/plugins/1/ferromq-http-api/config?apply=none" \
  -H 'Content-Type: application/toml' \
  --data-binary $'max_row_limit = 5000\nhttp_laddr = "0.0.0.0:6060"\n'

# Versions + rollback
curl -sS "http://localhost:6060/api/v1/plugins/1/ferromq-http-api/config/versions"
curl -sS -X POST "http://localhost:6060/api/v1/plugins/1/ferromq-http-api/config/rollback/1710000000000?apply=reload"
```

Audit actions: `plugin_config_update`, `plugin_config_rollback`. Existing `GET` / `PUT .../reload` / `load` / `unload` are unchanged.

### POST /api/v1/plugins/{node}/{plugin}/config/validate

Same body as PUT. Parses and checks the payload; does **not** write. Returns `{ valid, effective, diff, errors }`.

### GET /api/v1/plugins/{node}/{plugin}/config/versions

Last-N backups, newest first: `[{ "version", "ts", "size" }]`.

### POST /api/v1/plugins/{node}/{plugin}/config/rollback/{version}

Restore a backup (operator+). Same `apply` / `effective` rules as PUT.

### Broker / listener / log (`ferromq.toml`)

Read-only overview first. Writable `mqtt` / `listener` / `log` sections update the file only and **always** return `effective=restart_required`. FerroMQ does not hot-restart `ferromqd`.

| Method | Path | Role |
|--------|------|------|
| GET | `/api/v1/broker/config` | any authenticated (secrets redacted unless `?reveal=1` + admin) |
| GET | `/api/v1/broker/config/{mqtt\|listener\|log}` | same |
| PUT | `/api/v1/broker/config/{mqtt\|listener\|log}` | admin |
| POST | `/api/v1/broker/config/{section}/validate` | admin |
| GET | `/api/v1/broker/config/versions` | any authenticated |
| POST | `/api/v1/broker/config/rollback/{version}` | admin |

File path: `broker_config_file` in `ferromq-http-api.toml`, else `FERROMQ_CONFIG`, else `Settings` `-f` path, else `./ferromq.toml`.

```bash
curl -sS "http://localhost:6060/api/v1/broker/config"
curl -sS -b cookie.txt -X PUT "http://localhost:6060/api/v1/broker/config/mqtt" \
  -H 'Content-Type: application/json' \
  -d '{"max_sessions": 10000, "delayed_publish_max": 100000}'
# {"ok":true,"written":true,"applied":false,"effective":"restart_required",...}
```

Audit actions: `broker_config_update`, `broker_config_rollback`.

## Access control & integrations (P5)

Structured REST on top of P4 plugin-config write + `load_config`. Query `?node=` selects a node (default: the HTTP API local node). Writes default to `apply=reload` (in-process `load_config`, `effective=hot` — **not** a `ferromqd` restart). Secrets (`password` / `token` / `secret` / `jwt`, and URL userinfo) are `***` unless `?reveal=1` and admin. Role: viewer read; operator+ write.

There is **no** FerroMQ blacklist / connection-policy plugin. `GET /api/v1/blacklist` returns `available: false` and points at ACL `control=connect` rules.

### ACL (`ferromq-acl`)

```
GET    /api/v1/acl
PUT    /api/v1/acl                 # settings and/or full rules replace
GET    /api/v1/acl/rules
POST   /api/v1/acl/rules
PUT    /api/v1/acl/rules/{index}
DELETE /api/v1/acl/rules/{index}
```

```bash
# List rules (passwords redacted)
curl -sS http://127.0.0.1:6060/api/v1/acl/rules

# Add a deny-connect rule and hot-apply
curl -sS -X POST http://127.0.0.1:6060/api/v1/acl/rules \
  -H 'Content-Type: application/json' \
  -d '{"access":"deny","who":{"ipaddr":"10.1.2.3"},"control":"connect"}'
# {"ok":true,"written":true,"applied":true,"effective":"hot","rule":{"index":2,...}}

# Structured or raw array are both accepted
curl -sS -X POST http://127.0.0.1:6060/api/v1/acl/rules \
  -H 'Content-Type: application/json' \
  -d '["allow",{"user":"sensor"},"pubsub",["iot/%u/#"]]'
```

Audit: `acl_rule_add`, `acl_rule_update`, `acl_rule_delete`, `acl_config_update`.

### Auth providers (`ferromq-auth-http` / `ferromq-auth-jwt`)

MQTT **client** auth plugins — not dashboard login.

```
GET  /api/v1/auth-providers
GET  /api/v1/auth-providers/{http|jwt}
PUT  /api/v1/auth-providers/{http|jwt}
POST /api/v1/auth-providers/{name}/test
```

`POST .../test` is a stub: HTTP does a **TCP connect** after SSRF checks (no HTTP request). JWT checks that `hmac_secret` is present or `public_key` exists. Pass `allow_private=1` as admin to probe loopback.

### Auto-subscription / topic-rewrite

```
GET/POST          /api/v1/auto-subscriptions
PUT/DELETE        /api/v1/auto-subscriptions/{index}
GET/POST          /api/v1/topic-rewrites
PUT/DELETE        /api/v1/topic-rewrites/{index}
```

### Webhooks (`ferromq-web-hook`)

```
GET    /api/v1/webhooks
PUT    /api/v1/webhooks
POST   /api/v1/webhooks/urls
DELETE /api/v1/webhooks/urls/{index}
POST   /api/v1/webhooks/rules
PUT    /api/v1/webhooks/rules/{hook}/{index}
DELETE /api/v1/webhooks/rules/{hook}/{index}
POST   /api/v1/webhooks/test
```

```bash
curl -sS http://127.0.0.1:6060/api/v1/webhooks
# urls have userinfo redacted: https://***:***@hooks.example.com/mqtt

curl -sS -X POST http://127.0.0.1:6060/api/v1/webhooks/urls \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://hooks.example.com/mqtt"}'

curl -sS -X POST http://127.0.0.1:6060/api/v1/webhooks/test \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://hooks.example.com/mqtt"}'
# {"ok":true,"kind":"tcp_connect",...}  — no HTTP POST is sent (SSRF)
```

`file://` urls are allowed in config; test ping rejects them. `queue_capacity` / `concurrency_limit` still need a plugin restart (plugin docs).

### Bridges

```
GET /api/v1/bridges
GET /api/v1/bridges/{plugin}
PUT /api/v1/bridges/{plugin}           # P4 config write
PUT /api/v1/bridges/{plugin}/load
PUT /api/v1/bridges/{plugin}/unload
```

```bash
curl -sS http://127.0.0.1:6060/api/v1/bridges
curl -sS http://127.0.0.1:6060/api/v1/bridges/ferromq-bridge-egress-mqtt
# attrs come from the plugin attrs() hook when loaded (clients/errors if the plugin exposes them)

curl -sS -X PUT http://127.0.0.1:6060/api/v1/bridges/ferromq-bridge-egress-mqtt?apply=reload \
  -H 'Content-Type: application/json' \
  -d '{"bridges":[{"enable":true,"name":"b1","server":"tcp://127.0.0.1:2883"}]}'
```

## Diagnostics & cluster ops (P6)

What FerroMQ can actually support is exposed. Gaps return structured `available: false` (or HTTP 501 for writes) instead of fake UIs. Viewer can read; operator+ can acknowledge alarms and attempt cluster writes. Existing `/brokers`, `/nodes`, `/health/check`, `/features`, `/stats`, `/metrics` stay compatible; `/brokers` and `/nodes` gain an additive `cluster` object.

| Endpoint | Reality |
|----------|---------|
| `GET /api/v1/alarms` / `/alarms/history` | **Real (derived).** Thin in-memory bus fed from health, feature inconsistency / partial failures, and unreachable peers. Lost on restart. Not a native alarm plugin. |
| `POST /api/v1/alarms/{id}/acknowledge` | **Real.** Marks a current alarm. Audit: `alarm_acknowledge`. |
| `GET /api/v1/logs` | **Gap.** No log collector. Points at `GET /broker/config/log`. |
| `GET /api/v1/trace` (+ write) | **Gap.** No packet-trace plugin. Writes are 501. |
| `GET /api/v1/slow-subs` | **Gap.** No per-subscriber latency tracker. |
| `GET /api/v1/topic-metrics` | **Partial.** `available: true`, `kind: route_derived`. Subscriber counts from the topic router (same as `/routes`). **No per-topic rates.** `$SYS/brokers/{id}/stats\|metrics` listed only when `ferromq-sys-topic` is loaded. |
| `GET /api/v1/cluster` | **Real (read-only topology).** `mode`: `standalone` / `raft` / `broadcast`. `membership.join` / `leave` say whether write APIs exist. |
| `POST /api/v1/cluster/join` | **501.** `Raft::join` is consumed at `ferromq-cluster-raft` init (`raft_peer_addrs`). Always returns per-node results in `details.nodes`. |
| `POST /api/v1/cluster/leave` | **Raft only.** Forwards `Plugin::send({"op":"leave"})` → `Mailbox::leave` on the local node. Broadcast / standalone: 501. Per-node results always. Audit: `cluster_leave`. |

```bash
curl -sS http://127.0.0.1:6060/api/v1/alarms
# {"available":true,"source":"derived","items":[...]}

curl -sS http://127.0.0.1:6060/api/v1/logs
# {"available":false,"kind":"logs","gap":"...", "alternatives":[...]}

curl -sS http://127.0.0.1:6060/api/v1/topic-metrics
# {"available":true,"kind":"route_derived","items":[{"topic":"t","subscribers":1,"node_ids":[1]}],"sys_topic":{"active":false,...}}

curl -sS http://127.0.0.1:6060/api/v1/cluster
# {"mode":"standalone","membership":{"join":false,"leave":false,"reason":"..."},"nodes":[...]}

curl -sS -X POST http://127.0.0.1:6060/api/v1/cluster/join
# HTTP 501 {"code":501,"details":{"ok":false,"action":"join","nodes":[{"ok":false,"node_id":1,"error":"..."}]}}
```

`GET /brokers` / `GET /nodes` include `cluster: { mode, plugin, leader_id, role, peers }` without removing existing fields.

### PUT /api/v1/plugins/{node}/{plugin}/config/reload

Reloads the plugin configuration information of the specified plugin name under the specified node.

**Path Parameters:**

| Name | Type | Required | Description |
| ---- | --------- | ------------|-------------|
| node | Integer    | True       | Node ID, Such as: 1    |
| plugin | String    | True       | Plugin name        |

**Success Response Body:**

Returns JSON `true` on success.

**Examples:**

```bash
$ curl -i -X PUT "http://localhost:6060/api/v1/plugins/1/ferromq-http-api/config/reload"

true
```

### PUT /api/v1/plugins/{node}/{plugin}/load

Load the specified plugin under the specified node.

**Path Parameters:**

| Name | Type | Required | Description |
| ---- | --------- | ------------|-------------|
| node | Integer    | True       | Node ID, Such as: 1    |
| plugin | String    | True       | Plugin name        |

**Success Response Body:**

Returns JSON `true` on success.

**Examples:**

```bash
$ curl -i -X PUT "http://localhost:6060/api/v1/plugins/1/ferromq-web-hook/load"

true
```

### PUT /api/v1/plugins/{node}/{plugin}/unload

Unload the specified plugin under the specified node.

**Path Parameters:**

| Name | Type | Required | Description |
| ---- | --------- | ------------|-------------|
| node | Integer    | True       | Node ID, Such as: 1    |
| plugin | String    | True       | Plugin name        |

**Success Response Body (JSON):**

| Name | Type | Description |
|------|------|-------------|
| body | Bool | true/false  |

**Examples:**

```bash
$ curl -i -X PUT "http://localhost:6060/api/v1/plugins/1/ferromq-web-hook/unload"

true
```

## Stats

### GET /api/v1/stats

<span id = "get-stats" />

Return all status data in the cluster.

**Path Parameters:** None

**Success Response Body (JSON):**

| Name          | Type             | Description   |
|---------------|------------------| ------------- |
| []            | Array of Objects | List of status data on each node |
| [0].node  | Json Object      | Node information |
| [0].stats | Json Object      | Status data, see  *stats* below |

**node:**

| Name          | Type    | Description |
|---------------|---------|-------------|
| id            | Integer | Node ID     |
| name          | String  | Node name        |
| running        | Bool | Whether the node is running        |

**stats:**

| Name                       | Type    | Description                      |
|----------------------------|---------|----------------------------------|
| connections.count          | Integer | Number of current connections     |
| connections.max            | Integer | Historical maximum number of connections |
| handshakings.count         | Integer | Current number of active handshakes |
| handshakings.max           | Integer | Historical maximum of current active handshake connections |
| handshakings_active.count  | Integer | Current number of connections undergoing handshake operations |
| handshakings_rate.count    | Integer | Connection handshake rate  |
| handshakings_rate.max      | Integer | Historical maximum of connection handshake rate |
| sessions.count             | Integer | Number of current sessions |
| sessions.max               | Integer | Historical maximum number of sessions |
| topics.count               | Integer | Number of current topics |
| topics.max                 | Integer | Historical maximum number of topics |
| subscriptions.count        | Integer | Number of current subscriptions, including shared subscriptions |
| subscriptions.max          | Integer | Historical maximum number of subscriptions |
| subscriptions_shared.count | Integer | Number of current shared subscriptions |
| subscriptions_shared.max   | Integer | Historical maximum number of shared subscriptions |
| routes.count               | Integer | Number of current routes |
| routes.max                 | Integer | Historical maximum number of routes |
| retained.count             | Integer | Number of currently retained messages |
| retained.max               | Integer | Historical maximum number of retained messages |
| delayed_publishs.count     | Integer | Number of current delayed publish messages |
| delayed_publishs.max       | Integer | Historical maximum number of delayed publish messages |
| forwards.count             | Integer | Number of current forwarded messages |
| forwards.max               | Integer | Historical maximum number of forwarded messages |
| in_inflights.count         | Integer | Current number of incoming inflight messages (awaiting ACK) |
| in_inflights.max           | Integer | Historical maximum number of incoming inflight messages |
| out_inflights.count        | Integer | Current number of outgoing inflight messages (awaiting ACK) |
| out_inflights.max          | Integer | Historical maximum number of outgoing inflight messages |
| message_queues.count       | Integer | Current number of message queues |
| message_queues.max         | Integer | Historical maximum number of message queues |
| message_storages.count     | Integer | Current number of message storages (-1 means storage module not enabled) |
| message_storages.max       | Integer | Historical maximum number of message storages |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/stats"

[{"node":{"id":1,"name":"1@127.0.0.1","running":true},"stats":{"connections.count":1,"connections.max":2,"retained.count":2,"retained.max":2,"routes.count":3,"routes.max":4,"sessions.count":1,"sessions.max":2,"subscriptions.count":7,"subscriptions.max":8,"subscriptions_shared.count":1,"subscriptions_shared.max":2,"topics.count":3,"topics.max":4}}]
```

### GET /api/v1/stats/{node}

Returns status data on the specified node.

**Path Parameters:**

| Name | Type | Required | Description |
| ---- | --------- | ------------|-------------|
| node | Integer    | True       | Node ID, Such as: 1    |

**Success Response Body (JSON):**

| Name          | Type                 | Description                      |
|---------------|----------------------|----------------------------------|
| {}            | Object               | List of status data on each node |
| {}.node  | Json Object          | Node information                 |
| {}.stats | Json Object          | Status data, see  *stats* below    |

**node:**

| Name          | Type    | Description |
|---------------|---------|-------------|
| id            | Integer | Node ID       |
| name          | String  | Node name      |
| running        | Bool | Whether the node is running       |

**stats:**

| Name | Type | Description   |
|------| --------- |---------------|
| {}   | Json Object | Status data, see [GET /api/v1/stats](#get-stats) for details |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/stats/1"

{"node":{"id":1,"name":"1@127.0.0.1","running":true},"stats":{"connections.count":1,"connections.max":2,"retained.count":2,"retained.max":2,"routes.count":3,"routes.max":4,"sessions.count":1,"sessions.max":2,"subscriptions.count":7,"subscriptions.max":8,"subscriptions_shared.count":1,"subscriptions_shared.max":2,"topics.count":3,"topics.max":4}}
```

### GET /api/v1/stats/sum

Summarize the status data of all nodes in the cluster.

**Path Parameters:** None

**Success Response Body (JSON):**

| Name      | Type                 | Description                            |
|-----------|----------------------|----------------------------------------|
| {}        | Object               | Status summary on each node            |
| {}.nodes  | Json Objects         | Node information                       |
| {}.stats | Json Object          | Status summary data, see *stats* below |

**nodes:**

| Name         | Type     | Description       |
|--------------|----------|-------------------|
| {id}         | Object   | Node, key is the Node ID |
| {id}.name    | String   | Node name         |
| {id}.running | Bool     | Whether the node is running |

**stats:**

| Name | Type | Description                                                           |
|------| --------- |-----------------------------------------------------------------------|
| {}   | Json Object | Status summary data, see [GET /api/v1/stats](#get-stats) for details  |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/stats/sum"

{"nodes":{"1":{"name":"1@127.0.0.1","running":true}},"stats":{"connections.count":1,"connections.max":2,"retained.count":2,"retained.max":2,"routes.count":3,"routes.max":4,"sessions.count":1,"sessions.max":2,"subscriptions.count":7,"subscriptions.max":8,"subscriptions_shared.count":1,"subscriptions_shared.max":2,"topics.count":3,"topics.max":4}}
```

### GET /api/v1/stats/sys

Returns system status data for all nodes in the cluster. Response format is the same as [GET /api/v1/stats](#get-stats), but stats fields use system-level JSON representation.

**Path Parameters:** None

**Success Response Body (JSON):** Same as [GET /api/v1/stats](#get-stats)

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/stats/sys"
```

### GET /api/v1/stats/sys/{node}

Returns system status data for the specified node.

**Path Parameters:**

| Name | Type    | Required | Description        |
|------|---------|----------|--------------------|
| node | Integer | True     | Node ID, e.g., 1 |

**Success Response Body (JSON):** Same as [GET /api/v1/stats](#get-stats)

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/stats/sys/1"
```

### GET /api/v1/stats/sys/sum

Summarize system status data across all nodes.

**Path Parameters:** None

**Success Response Body (JSON):** Same as [GET /api/v1/stats/sum](#get-statssum)

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/stats/sys/sum"
```

### GET /api/v1/stats/history

Queries historical stats data for all nodes in the cluster. Requires history storage configuration to be enabled.

**Query String Parameters:**

| Name         | Type    | Required | Default | Description                         |
|--------------|---------|----------|---------|-------------------------------------|
| minutes      | Integer | Optional | 5       | Query data for the last N minutes   |
| hours        | Integer | Optional |         | Query data for the last N hours (mutually exclusive with minutes/days) |
| days         | Integer | Optional |         | Query data for the last N days (mutually exclusive with minutes/hours) |
| limit        | Integer | Optional | 1000    | Maximum number of data points       |
| merge_window | Integer | Optional |         | Merge window (seconds), merges data at this granularity |

**Success Response Body (JSON):**

| Name       | Type              | Description                          |
|------------|-------------------|--------------------------------------|
| from       | Integer           | Query start timestamp (milliseconds) |
| to         | Integer           | Query end timestamp (milliseconds)   |
| nodes      | Object            | Per-node history data, key is Node ID |
| nodes.{id} | Object            | Node history data                    |
| .from      | Integer           | Start timestamp for this node        |
| .to        | Integer           | End timestamp for this node          |
| .node      | Integer           | Node ID                              |
| .count     | Integer           | Number of data points                |
| .data      | Array             | Array of snapshot objects with `ts` (timestamp) and stats fields |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/stats/history?minutes=10&limit=100"

{"from":1700000000000,"to":1700000600000,"nodes":{"1":{"from":1700000000000,"to":1700000600000,"node":1,"count":120,"data":[{"ts":1700000000000,"connections.count":1,"sessions.count":1,...},...]}}}
```

### GET /api/v1/stats/history/{node}

Queries historical stats data for the specified node.

**Path Parameters:**

| Name | Type    | Required | Description        |
|------|---------|----------|--------------------|
| node | Integer | True     | Node ID, e.g., 1 |

**Query String Parameters:** Same as [GET /api/v1/stats/history](#get-statshistory)

**Success Response Body (JSON):**

| Name   | Type    | Description                                    |
|--------|---------|------------------------------------------------|
| from   | Integer | Query start timestamp (milliseconds)           |
| to     | Integer | Query end timestamp (milliseconds)             |
| node   | Integer | Node ID                                        |
| count  | Integer | Number of data points                          |
| data   | Array   | Array of snapshot objects with `ts` (timestamp) and stats fields |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/stats/history/1?hours=1&limit=200"
```

### GET /api/v1/stats/history/sum

Aggregates historical stats data across all nodes (sums numeric fields at each timestamp).

**Query String Parameters:** Same as [GET /api/v1/stats/history](#get-statshistory)

**Success Response Body (JSON):**

| Name       | Type    | Description                              |
|------------|---------|------------------------------------------|
| from       | Integer | Query start timestamp (milliseconds)     |
| to         | Integer | Query end timestamp (milliseconds)       |
| node_count | Integer | Number of nodes participating in aggregation |
| count      | Integer | Number of data points                    |
| data       | Array   | Aggregated data points with summed numeric fields |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/stats/history/sum?minutes=30&limit=500"
```

## Metrics

### GET /api/v1/metrics

<span id = "get-metrics" />

Returns all statistical metrics under the cluster

**Path Parameters:** None

**Success Response Body (JSON):**

| Name          | Type             | Description                              |
|---------------|------------------|------------------------------------------|
| []            | Array of Objects | List of statistical metrics on each node |
| [0].node  | Json Object      | Node information                         |
| [0].metrics | Json Object      | Metrics, see *metrics* below             |

**node:**

| Name          | Type    | Description |
|---------------|---------|-------------|
| id            | Integer | Node ID       |
| name          | String  | Node name      |

**metrics:**

| Name | Type | Description |
| ----------------| --------- |-------------------------------------------------------------------------------------------|
| client.auth.anonymous           | Integer   | Number of clients who log in anonymously                                                   |
| client.auth.anonymous.error     | Integer   | Number of client login failures for anonymous connections.                                 |
| client.authenticate             | Integer   | Number of client authentications                                                           |
| client.connack                  | Integer   | Number of CONNACK packet sent                                                              |
| client.connack.auth.error       | Integer   | Number of CONNACK packets sent with connection authentication failures                     |
| client.connack.error            | Integer   | Number of CONNACK packets sent with connection failures                                    |
| client.connect                  | Integer   | Number of client connections                                                               |
| client.connected                | Integer   | Number of successful client connections                                                    |
| client.disconnected             | Integer   | Number of client disconnects                                                               |
| client.handshaking.timeout      | Integer   | Number of handshake timeouts for connections.                                              |
| client.publish.auth.error       | Integer   | Publish, Number of failed ACL rule checks.                                                 |
| client.publish.check.acl        | Integer   | Publish, Number of ACL rule checks                                                         |
| client.publish.error            | Integer   | Publish, Number of Failures                                                                |
| client.subscribe.auth.error     | Integer   | Subscribe, Number of ACL Rule Check Failures                                               |
| client.subscribe.error          | Integer   | Subscribe, Number of Failures                                                              |
| client.subscribe.check.acl      | Integer   | Subscribe, Number of ACL rule checks                                                       |
| client.subscribe                | Integer   | Number of client subscriptions                                                             |
| client.unsubscribe              | Integer   | Number of client unsubscriptions                                                           |
| messages.publish                | Integer   | Number of received PUBLISH packet                                                          |
| messages.publish.admin          | Integer   | Messages published via the HTTP API                                                        |
| messages.publish.bridge         | Integer   | Messages published via Bridge                                                              |
| messages.publish.custom         | Integer   | Messages published via MQTT clients                                                        |
| messages.publish.lastwill       | Integer   | Last Will Message                                                                          |
| messages.publish.retain         | Integer   | Forwarded Retained Message                                                                 |
| messages.publish.system         | Integer   | System Topic Messages ($SYS/#)                                                             |
| messages.delivered              | Integer   | Number of messages sent to the client                                                      |
| messages.delivered.admin        | Integer   | Messages published via the HTTP API, delivered                                             |
| messages.delivered.bridge       | Integer   | Messages published via Bridge, delivered                                                   |
| messages.delivered.custom       | Integer   | Messages published via MQTT clients, delivered                                             |
| messages.delivered.lastwill     | Integer   | Last Will Message, delivered                                                               |
| messages.delivered.retain       | Integer   | Forwarded Retained Message, delivered                                                      |
| messages.delivered.system       | Integer   | System Topic Messages ($SYS/#), delivered                                                  |
| messages.acked                  | Integer   | Number of received PUBACK and PUBREC packet                                                |
| messages.acked.admin            | Integer   | PUBACK / PUBREC received, for messages published via HTTP API                              |
| messages.acked.bridge           | Integer   | PUBACK / PUBREC received, for messages published via Bridge                                |
| messages.acked.custom           | Integer   | PUBACK / PUBREC received, for messages published via MQTT clients                          |
| messages.acked.lastwill         | Integer   | PUBACK / PUBREC received, for Last Will Message                                            |
| messages.acked.retain           | Integer   | PUBACK / PUBREC received, for Forwarded Retained Message                                   |
| messages.acked.system           | Integer   | PUBACK / PUBREC received, for System Topic Messages ($SYS/#)                               |
| messages.nonsubscribed          | Integer   | Number of PUBLISH Messages Without Subscription Found                                      |
| messages.nonsubscribed.admin    | Integer   | Without Subscription Found, Messages published via HTTP API                                |
| messages.nonsubscribed.bridge   | Integer   | Without Subscription Found, Messages published via Bridge                                  |
| messages.nonsubscribed.custom   | Integer   | Without Subscription Found, Messages published via MQTT clients                            |
| messages.nonsubscribed.lastwill | Integer   | Without Subscription Found, Last Will Message                                              |
| messages.nonsubscribed.system   | Integer   | Without Subscription Found, System Topic Messages ($SYS/#)                                 |
| messages.dropped                | Integer   | Total number of messages dropped                                                           |
| session.created                 | Integer   | Number of sessions created                                                                 |
| session.resumed                 | Integer   | Number of sessions resumed because `Clean Session` or `Clean Start` is false               |
| session.subscribed              | Integer   | Number of successful client subscriptions                                                  |
| session.unsubscribed            | Integer   | Number of successful client unsubscriptions                                                |
| session.terminated              | Integer   | Number of terminated sessions                                                              |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/metrics"

[{"metrics":{"client.auth.anonymous":38,"client.authenticate":47,"client.connack":47,"client.connect":47,"client.connected":47,"client.disconnected":46,"client.publish.check.acl":50,"client.subscribe":37,"client.subscribe.check.acl":15,"client.unsubscribe":8,"messages.acked":35,"messages.delivered":78,"messages.dropped":0,"messages.publish":78,"session.created":45,"session.resumed":2,"session.subscribed":15,"session.terminated":42,"session.unsubscribed":8},"node":{"id":1,"name":"1@127.0.0.1"}}]
```

### GET /api/v1/metrics/{node}

Returns statistical metrics data of the specified node under the cluster.

**Path Parameters:**

| Name | Type | Required | Description |
| ---- | --------- | ------------|-------------|
| node | Integer    | True       | Node ID, Such as: 1    |

**Success Response Body (JSON):**

| Name          | Type                | Description        |
|---------------|---------------------|--------------------|
| {}            | Object         | Statistical metrics information      |
| {}.node  | Json Object         | Node information       |
| {}.metrics | Json Object       | Metrics, see *metrics* below |

**node:**

| Name          | Type    | Description |
|---------------|---------|-------------|
| id            | Integer | Node ID       |
| name          | String  | Node name      |

**metrics:**

| Name | Type | Description                                                                                                              |
|------| --------- |--------------------------------------------------------------------------------------------------------------------------|
| {}   | Json Object | Statistical metrics data, see [GET /api/v1/metrics](#get-metrics) for details |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/metrics/1"

{"metrics":{"client.auth.anonymous":38,"client.authenticate":47,"client.connack":47,"client.connect":47,"client.connected":47,"client.disconnected":46,"client.publish.check.acl":50,"client.subscribe":37,"client.subscribe.check.acl":15,"client.unsubscribe":8,"messages.acked":35,"messages.delivered":78,"messages.dropped":0,"messages.publish":78,"session.created":45,"session.resumed":2,"session.subscribed":15,"session.terminated":42,"session.unsubscribed":8},"node":{"id":1,"name":"1@127.0.0.1"}}
```

### GET /api/v1/metrics/sum

Summarize the statistical metrics data of all nodes under the cluster.

**Path Parameters:** None

**Success Response Body (JSON):**

| Name | Type | Description                                                                    |
|------| --------- |--------------------------------------------------------------------------------|
| {}   | Json Object | Statistical metrics data, see [GET /api/v1/metrics](#get-metrics) for details  |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/metrics/sum"

{"client.auth.anonymous":38,"client.authenticate":47,"client.connack":47,"client.connect":47,"client.connected":47,"client.disconnected":46,"client.publish.check.acl":50,"client.subscribe":37,"client.subscribe.check.acl":15,"client.unsubscribe":8,"messages.acked":35,"messages.delivered":78,"messages.dropped":0,"messages.publish":78,"session.created":45,"session.resumed":2,"session.subscribed":15,"session.terminated":42,"session.unsubscribed":8}
```

### GET /api/v1/metrics/history

Queries historical metrics data for all nodes in the cluster. Requires history storage configuration to be enabled.

**Query String Parameters:**

| Name         | Type    | Required | Default | Description                         |
|--------------|---------|----------|---------|-------------------------------------|
| minutes      | Integer | Optional | 5       | Query data for the last N minutes   |
| hours        | Integer | Optional |         | Query data for the last N hours     |
| days         | Integer | Optional |         | Query data for the last N days      |
| limit        | Integer | Optional | 1000    | Maximum number of data points       |
| merge_window | Integer | Optional |         | Merge window (seconds)              |

**Success Response Body (JSON):**

| Name       | Type              | Description                          |
|------------|-------------------|--------------------------------------|
| from       | Integer           | Query start timestamp (milliseconds) |
| to         | Integer           | Query end timestamp (milliseconds)   |
| nodes      | Object            | Per-node history data, key is Node ID |
| nodes.{id} | Object            | Node history data                    |
| .from      | Integer           | Start timestamp for this node        |
| .to        | Integer           | End timestamp for this node          |
| .node      | Integer           | Node ID                              |
| .count     | Integer           | Number of data points                |
| .data      | Array             | Array of snapshot objects with `ts` (timestamp) and metrics fields |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/metrics/history?minutes=10&limit=100"
```

### GET /api/v1/metrics/history/{node}

Queries historical metrics data for the specified node.

**Path Parameters:**

| Name | Type    | Required | Description        |
|------|---------|----------|--------------------|
| node | Integer | True     | Node ID, e.g., 1 |

**Query String Parameters:** Same as [GET /api/v1/metrics/history](#get-metricshistory)

**Success Response Body (JSON):**

| Name   | Type    | Description                                    |
|--------|---------|------------------------------------------------|
| from   | Integer | Query start timestamp (milliseconds)           |
| to     | Integer | Query end timestamp (milliseconds)             |
| node   | Integer | Node ID                                        |
| count  | Integer | Number of data points                          |
| data   | Array   | Array of snapshot objects with `ts` (timestamp) and metrics fields |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/metrics/history/1?minutes=30"
```

### GET /api/v1/metrics/history/sum

Aggregates historical metrics data across all nodes.

**Query String Parameters:** Same as [GET /api/v1/metrics/history](#get-metricshistory)

**Success Response Body (JSON):**

| Name       | Type    | Description                              |
|------------|---------|------------------------------------------|
| from       | Integer | Query start timestamp (milliseconds)     |
| to         | Integer | Query end timestamp (milliseconds)       |
| node_count | Integer | Number of nodes participating in aggregation |
| count      | Integer | Number of data points                    |
| data       | Array   | Aggregated data points with summed numeric fields |

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/metrics/history/sum?minutes=60"
```

### GET /api/v1/metrics/prometheus

<span id = "get-prometheus" />

Return the status data and statistical metrics of all nodes in the cluster in *prometheus* format.

**Path Parameters:** None

**Success Response Body (TEXT):**

**Examples:**

```bash
$ curl -i -X GET "http://localhost:6060/api/v1/metrics/prometheus"

# HELP ferromq_metrics All metrics data
# TYPE ferromq_metrics gauge
ferromq_metrics{item="client.auth.anonymous",node="1"} 0
ferromq_metrics{item="client.auth.anonymous",node="2"} 2
...
# HELP ferromq_nodes All nodes status
# TYPE ferromq_nodes gauge
ferromq_nodes{item="disk_free",node="1"} 46307106816
...
# HELP ferromq_stats All status data
# TYPE ferromq_stats gauge
ferromq_stats{item="connections.count",node="1"} 1
...
```

### GET /api/v1/metrics/prometheus/{node}

Returns the status data and statistical metrics of the specified node in the cluster in Prometheus format.

**Path Parameters:**

| Name | Type | Required | Description |
| ---- | --------- | ------------|-------------|
| node | Integer    | True       | Node ID, Such as: 1    |

**Success Response Body (TEXT):**

see [GET /api/v1/metrics/prometheus](#get-prometheus) 

### GET /api/v1/metrics/prometheus/sum

Returns the total of the status data and statistical metrics of all nodes in the cluster in Prometheus format.

**Path Parameters:** None

**Success Response Body (TEXT):**

see [GET /api/v1/metrics/prometheus](#get-prometheus) 
