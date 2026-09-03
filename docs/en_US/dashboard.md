English | [简体中文](../zh_CN/dashboard.md)

# Dashboard (Web Management UI)

FerroMQ ships with a built-in web management UI served by the `ferromq-http-api` plugin. Production assets are the Vite build of [`sllt/ferromq-dashboard`](https://github.com/sllt/ferromq-dashboard) (React 19, Hash Router, `base: './'`), committed under `ferromq-plugins/ferromq-http-api/dashboard-dist/` and embedded into the binary with `rust-embed`.

Day-to-day frontend work happens in that separate repo (`pnpm dev`). This crate only vendors the built `dist/` files.

## Access

### 1. Embedded mode (default)

No extra files are required at runtime:

```
http://<host>:6060/dashboard/
```

The SPA also answers at `/`. Routes are hash-based (`#/overview`, `#/clients`, …), so a browser refresh of `/dashboard/` always loads `index.html`.

Sign in with a dashboard username/password (`POST /api/v1/auth/login`) when `dashboard_admin_password` is set in `ferromq-http-api.toml`. A static Bearer token or API key is still accepted as a fallback. See [HTTP API — Authentication](http-api.md#authentication-p3a-session--p3b-api-keys).

Changing the UI requires rebuilding `dashboard-dist/` and recompiling (`cargo build`).

### 2. External directory mode (`dashboard_static_dir`)

To preview a local Vite production build without recompiling FerroMQ, set `dashboard_static_dir` to that `dist/` directory:

```toml
dashboard_static_dir = "/path/to/ferromq-dashboard/dist"
```

- The plugin serves the SPA from both `/` and `/dashboard/`
- **Relative paths are resolved against the process cwd** (not the config file directory)
- The filesystem directory is used only when it **exists**; otherwise the plugin logs a warning and falls back to the embedded assets
- **Changes take effect on browser refresh without recompiling** — `index.html` is served with `Cache-Control: no-cache`; hashed files under `assets/` are `immutable`

For the Vite dev server (HMR), run `pnpm dev` in `ferromq-dashboard` and keep the `/api/v1` proxy pointed at this plugin. Do not point `dashboard_static_dir` at the dashboard source tree.

## How to verify

Embedded (default — `dashboard_static_dir` unset):

```bash
curl -sI http://127.0.0.1:6060/dashboard/
# 200, text/html, cache-control: no-cache
# x-content-type-options: nosniff, x-frame-options: DENY

curl -sI http://127.0.0.1:6060/dashboard/assets/<hashed>.js
# 200, cache-control: public, max-age=31536000, immutable

curl -s http://127.0.0.1:6060/api/v1/openapi.json | head
curl -sI http://127.0.0.1:6060/api/v1/docs
# /api/v1 and OpenAPI/docs stay on the same listener and are not affected
```

Filesystem override:

```bash
# edit ferromq-http-api.toml, then restart the plugin / ferromqd
# dashboard_static_dir = "/path/to/ferromq-dashboard/dist"
curl -s http://127.0.0.1:6060/dashboard/ | head
# body matches that dist/index.html
```

A missing `dashboard_static_dir` path falls back to the embed (warning in the log).

## Rebuild the vendored assets

From the FerroMQ repo root (Node 20+, pnpm 9+):

```bash
./scripts/sync-dashboard-dist.sh
cargo build -p ferromq-http-api
```

See `ferromq-plugins/ferromq-http-api/dashboard-dist/README.md` for the source commit SHA.

## Pages

| Route | Page | Description |
|-------|------|-------------|
| `#/overview` | Overview | Cluster stats / metrics (and history when configured), node and broker cards |
| `#/nodes` | Nodes | Node list, health, feature support |
| `#/clients` | Clients | Search, detail, online/offline kick |
| `#/subscriptions` | Subscriptions | Cluster subscription list |
| `#/routes` | Routes | Topic routing table |
| `#/retains` | Retained Messages | Query, preview, delete by exact topic |
| `#/publish` | Publish | `POST /api/v1/mqtt/publish` |
| `#/plugins` | Plugins | Cluster plugin list, load / unload / config / versions |
| `#/broker-config` | Broker config | Read mqtt / listener / log; admin writes always `restart_required` |
| `#/acl` | ACL | Structured `ferromq-acl` rules |
| `#/auth-providers` | Auth providers | HTTP / JWT MQTT client auth (not dashboard login) |
| `#/auto-subscriptions` | Auto-subscriptions | Index-based CRUD |
| `#/topic-rewrites` | Topic rewrite | Index-based CRUD |
| `#/webhooks` | Webhooks | URL / rule CRUD and TCP test |
| `#/bridges` | Bridges | List / status / config / load / unload |
| `#/blacklist` | Blacklist | Honest `available: false` gap + ACL alternative |
| `#/alarms` | Alarms | Derived in-memory bus; acknowledge (operator+) |
| `#/logs` `#/trace` `#/slow-subs` | Diagnostics gaps | `available: false` / 501 — no invented metrics |
| `#/topic-metrics` | Topic metrics | Route-derived subscriber counts (not per-topic rates) |
| `#/cluster` | Cluster | Read-only topology; join always disabled; leave only on raft |
| `#/users` | Users | List / create / disable (admin) |
| `#/api-keys` | API Keys | Create hashed keys; secret shown once (admin) |
| `#/audit` | Audit Log | Write-operation audit events (admin) |

See [HTTP API](http-api.md) for what is real vs `available: false`.

## Internationalization

The console ships **Simplified Chinese** and **English**, switchable from the header (stored in `localStorage`). Light / dark theme is also stored locally.

## Developer notes

- **Hash Router + `base: './'`**: asset URLs are relative (`./assets/<name>-<hash>.js`), so the same files work at `/` and `/dashboard/`.
- **Cache**: `index.html` is `no-cache`; hashed `assets/*` are long-lived `immutable`. After embedding a new build, a normal refresh picks up the new HTML and therefore the new hashes.
- **Headers**: Dashboard responses add `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: same-origin`, and a pragmatic CSP (self + Google Fonts used by the shipped `index.html`). Gzip is enabled when the client sends `Accept-Encoding: gzip`.
- **`/api/v1` is untouched**: OpenAPI (`/api/v1/openapi.json`) and Swagger UI (`/api/v1/docs`) stay on the API router.

## Troubleshooting

| Issue | Cause & fix |
|-------|-------------|
| Blank page after upgrade | Hard-refresh once so `index.html` is not reused from an old cache; hashed assets then load from the new HTML |
| Changes not applied (embedded) | Rebuild `dashboard-dist/` (`./scripts/sync-dashboard-dist.sh`) and `cargo build` |
| `dashboard_static_dir` ignored | Path must exist (resolved from the process cwd). Check the “not found, falling back to embedded assets” warning |
| API 401 / CORS in `pnpm dev` | Vite proxies `/api/v1` to the broker; start `ferromqd` first and keep `VITE_API_PROXY_TARGET` in sync |
