[English](../en_US/dashboard.md)  | 简体中文

# Dashboard（Web 管理界面）

FerroMQ 内置 Web 管理界面，由 `ferromq-http-api` 插件提供服务。生产静态资源来自 [`sllt/ferromq-dashboard`](https://github.com/sllt/ferromq-dashboard) 的 Vite 构建产物（React 19、Hash Router、`base: './'`），提交在 `ferromq-plugins/ferromq-http-api/dashboard-dist/`，通过 `rust-embed` 打进二进制。

日常前端开发在独立的 dashboard 仓库进行（`pnpm dev`）。本仓库只内嵌构建后的 `dist/`。

## 访问方式

### 1. 嵌入模式（默认）

运行时不需要额外文件：

```
http://<host>:6060/dashboard/
```

`/` 同样会返回 SPA。路由是 hash（`#/overview`、`#/clients` 等），刷新 `/dashboard/` 始终加载 `index.html`。

在 `ferromq-http-api.toml` 中配置了 `dashboard_admin_password` 时，使用用户名/密码登录（`POST /api/v1/auth/login`）。仍可用静态 Bearer Token 或 API Key 作为回退。详见 [HTTP API — 认证](http-api.md#认证p3a-会话--p3b-api-key)。

修改界面需要重新生成 `dashboard-dist/` 并编译（`cargo build`）。

### 2. 外部目录模式（`dashboard_static_dir`）

若要在不重编 FerroMQ 的情况下预览本地 Vite 生产构建，将 `dashboard_static_dir` 指到该 `dist/`：

```toml
dashboard_static_dir = "/path/to/ferromq-dashboard/dist"
```

- 插件将 SPA 同时挂载到 `/` 与 `/dashboard/`
- **相对路径相对于进程 cwd**（不是配置文件所在目录）
- 仅当目录**存在**时才走文件系统；否则打警告并回退到嵌入资源
- **改文件刷新浏览器即生效，无需重新编译** — `index.html` 使用 `Cache-Control: no-cache`；`assets/` 下带 hash 的文件为 `immutable`

Vite 开发服务器（HMR）请在 `ferromq-dashboard` 里运行 `pnpm dev`，并把 `/api/v1` 代理到本插件。不要把 `dashboard_static_dir` 指到 dashboard 源码树。

## 如何验证

嵌入模式（默认，未设置 `dashboard_static_dir`）：

```bash
curl -sI http://127.0.0.1:6060/dashboard/
# 200, text/html, cache-control: no-cache
# x-content-type-options: nosniff, x-frame-options: DENY

curl -sI http://127.0.0.1:6060/dashboard/assets/<hashed>.js
# 200, cache-control: public, max-age=31536000, immutable

curl -s http://127.0.0.1:6060/api/v1/openapi.json | head
curl -sI http://127.0.0.1:6060/api/v1/docs
# 同一监听器上的 /api/v1 与 OpenAPI/docs 不受影响
```

文件系统覆盖：

```bash
# 修改 ferromq-http-api.toml 后重启插件 / ferromqd
# dashboard_static_dir = "/path/to/ferromq-dashboard/dist"
curl -s http://127.0.0.1:6060/dashboard/ | head
# 响应体应与该 dist/index.html 一致
```

`dashboard_static_dir` 指向的路径不存在时回退到嵌入资源（日志有警告）。

## 重新生成内嵌资源

在 FerroMQ 仓库根目录（需要 Node 20+、pnpm 9+）：

```bash
./scripts/sync-dashboard-dist.sh
cargo build -p ferromq-http-api
```

源仓库提交 SHA 见 `ferromq-plugins/ferromq-http-api/dashboard-dist/README.md`。

## 页面功能

| 路由 | 页面 | 说明 |
|------|------|------|
| `#/overview` | 集群概览 | 统计 / 指标（配置了历史存储时含趋势），节点与 Broker 卡片 |
| `#/nodes` | 节点 | 节点列表、健康检查、功能支持 |
| `#/clients` | 客户端 | 搜索、详情、在线/离线踢出 |
| `#/subscriptions` | 订阅 | 集群订阅列表 |
| `#/routes` | 路由 | 主题路由表 |
| `#/retains` | 保留消息 | 查询、预览、按精确主题删除 |
| `#/publish` | 发布 | `POST /api/v1/mqtt/publish` |
| `#/plugins` | 插件 | 集群插件列表、加载 / 卸载 / 配置 / 版本 |
| `#/broker-config` | Broker 配置 | 读取 mqtt / listener / log；admin 写入一律 `restart_required` |
| `#/acl` | ACL | 结构化 `ferromq-acl` 规则 |
| `#/auth-providers` | 认证插件 | HTTP / JWT MQTT 客户端认证（不是 Dashboard 登录） |
| `#/auto-subscriptions` | 自动订阅 | 按索引增删改 |
| `#/topic-rewrites` | 主题改写 | 按索引增删改 |
| `#/webhooks` | Webhook | URL / 规则 CRUD 与 TCP 探测 |
| `#/bridges` | 桥接 | 列表 / 状态 / 配置 / 加载卸载 |
| `#/blacklist` | 黑名单 | 诚实的 `available: false` 缺口与 ACL 替代说明 |
| `#/alarms` | 告警 | 派生内存总线；确认（operator+） |
| `#/logs` `#/trace` `#/slow-subs` | 诊断缺口 | `available: false` / 501，不编造指标 |
| `#/topic-metrics` | 主题指标 | 由路由派生的订阅者计数（不是按主题速率） |
| `#/cluster` | 集群 | 只读拓扑；join 始终禁用；leave 仅 raft |
| `#/users` | 用户 | 列出 / 创建 / 禁用（admin） |
| `#/api-keys` | API 密钥 | 创建哈希 API Key，密钥只显示一次（admin） |
| `#/audit` | 审计日志 | 写操作审计（admin） |

真实性见 [HTTP API](http-api.md)。

## 国际化

控制台内置**简体中文**与 **English**，可在页头切换（写入 `localStorage`）。浅色 / 深色主题同样本地保存。

## 开发须知

- **Hash Router + `base: './'`**：资源 URL 为相对路径（`./assets/<name>-<hash>.js`），因此同一套文件可挂在 `/` 与 `/dashboard/`。
- **缓存**：`index.html` 为 `no-cache`；带 hash 的 `assets/*` 为长期 `immutable`。重新嵌入后普通刷新即可拿到新 HTML 和新 hash。
- **响应头**：Dashboard 响应带 `X-Content-Type-Options: nosniff`、`X-Frame-Options: DENY`、`Referrer-Policy: same-origin`，以及务实的 CSP（同源 + 构建产物里的 Google Fonts）。客户端声明 `Accept-Encoding: gzip` 时启用 Gzip。
- **`/api/v1` 不受影响**：OpenAPI（`/api/v1/openapi.json`）与 Swagger UI（`/api/v1/docs`）仍走 API 路由。

## 常见问题

| 问题 | 原因与处理 |
|------|-----------|
| 升级后白屏 | 强刷一次，避免沿用旧的 `index.html`；随后会按新 HTML 加载新 hash 资源 |
| 嵌入模式改了界面不生效 | 重新生成 `dashboard-dist/`（`./scripts/sync-dashboard-dist.sh`）并 `cargo build` |
| `dashboard_static_dir` 未生效 | 路径必须存在（相对进程 cwd）。检查日志中的 “not found, falling back to embedded assets” |
| `pnpm dev` 下 API 401 / CORS | Vite 把 `/api/v1` 代理到 Broker；先启动 `ferromqd`，并保持 `VITE_API_PROXY_TARGET` 一致 |
