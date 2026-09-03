[English](README.md) | [**简体中文**](README-CN.md)

# ferromq-http-api

[![crates.io](https://img.shields.io/crates/v/ferromq-http-api.svg)](https://crates.io/crates/ferromq-http-api)

RESTful HTTP API 插件。提供 Broker 管理、健康检查、节点信息、客户端/订阅/路由查询、MQTT 操作、插件管理、统计信息和 Prometheus 指标等端点。

## 概述

使用 `salvo` HTTP 框架提供 API 服务端。HTTP 服务器在插件构造（`new()`）时启动。支持热重载：当配置变化需要重启时（监听地址变更），先启动新服务再关闭旧服务。通过 gRPC 转发集群范围内的查询。

## 使用方法

### 构建

在 `ferromqd/Cargo.toml` 中添加依赖：

```toml
ferromq-http-api = "0.21"
```

需要 `ferromq` 的 features：`plugin`、`metrics`、`stats`、`grpc`、`shared-subscription`。

### 注册

```rust
ferromq_http_api::register(&scx, true, false).await?;
// 或指定名称：
ferromq_http_api::register_named(&scx, "ferromq-http-api", true, false).await?;
```

参数说明：`(scx, default_startup, immutable)`。

## 配置

配置文件：`ferromq-http-api.toml`（位于插件配置目录）。通过 `scx.plugins.read_config_default::<PluginConfig>("ferromq-http-api")` 加载。

| 选项 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `max_row_limit` | `usize` | `10000` | 列表端点返回的最大行数 |
| `http_laddr` | `string` | `"0.0.0.0:6060"` | HTTP 服务器监听地址 |
| `http_request_log` | `bool` | `false` | 是否打印 HTTP 请求日志 |
| `http_reuseaddr` | `bool` | `true` | 启用 `SO_REUSEADDR` socket 选项（仅 Unix） |
| `http_reuseport` | `bool` | `false` | 启用 `SO_REUSEPORT` socket 选项（仅 Unix） |
| `http_bearer_token` | `string` | — | HTTP API Bearer 令牌认证（可选；视为 admin/operator） |
| `dashboard_admin_username` | `string` | `"admin"` | 尚无 Dashboard 用户时的引导管理员用户名 |
| `dashboard_admin_password` | `string` | — | 引导管理员密码（首次登录 / `/auth/init` 时 bcrypt 哈希，从不存明文） |
| `dashboard_viewer_username` | `string` | — | 可选引导只读用户名 |
| `dashboard_viewer_password` | `string` | — | 可选引导只读密码 |
| `dashboard_cookie_secure` | `bool` | `false` | 会话 Cookie 是否设置 `Secure`（HTTPS 时启用） |
| `dashboard_session_idle_timeout` | `string` | `"30m"` | 会话空闲过期 |
| `dashboard_session_max_age` | `string` | `"12h"` | 会话绝对寿命 |
| `dashboard_login_rate_limit` | `u32` | `10` | 每个 IP 在窗口内的最大登录次数 |
| `dashboard_login_rate_window` | `string` | `"1m"` | 登录限流窗口 |
| `audit_max_events` | `usize` | `10000` | 内存审计环形缓冲区大小 |
| `audit_file` | `string` | — | 可选的 JSONL 审计文件 |
| `config_history_keep` | `usize` | `10` | 写入插件 / `ferromq.toml` 时保留的备份份数 |
| `broker_config_file` | `string` | — | 本节点 `ferromq.toml` 路径，供 `/api/v1/broker/config` 使用 |
| `message_type` | `u8` | `99` | 插件间 gRPC 通信的消息类型标识符 |
| `message_expiry_interval` | `string` | `"5m"` | 发布操作消息的默认过期时间 |
| `metrics_sample_interval` | `string` | `"5s"` | 指标采样间隔 |
| `prometheus_metrics_cache_interval` | `string` | `"5s"` | Prometheus 指标数据缓存间隔 |
| `dashboard_static_dir` | `string` | — | 可选的 React 控制台 `dist/` 目录。路径存在时在 `/dashboard/` 覆盖 rust-embed 的 `dashboard-dist/` |
| `storage` | `object` | — | 历史数据存储配置（可选，不配置则不启用历史功能） |
| `flush_interval` | `string` | `"5s"` | 历史数据刷盘间隔 |
| `history_retention` | `string` | `"7d"` | 历史数据保留时长。Warmup 从存储加载时，会检查每条数据的时间戳是否在 `history_retention` 范围内，已过期的数据不会被加载到缓存并从存储中删除。 |

### 配置来源

支持标准 FerroMQ 插件配置链：

1. `{plugins.dir}/ferromq-http-api.toml`（文件，可选——文件缺失时使用默认值）
2. `ferromq_plugin_ferromq_http_api_*` 环境变量
3. 通过 `ServerContext::plugins_config_map_add()` 内联配置

### 示例

```toml
# ferromq-http-api.toml
max_row_limit = 10_000
http_laddr = "0.0.0.0:6060"
http_request_log = false
message_expiry_interval = "5m"
prometheus_metrics_cache_interval = "5s"
```

## API 端点

所有端点都以 `/api/v1` 为前缀。

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 列出所有可用 API 端点 |
| POST | `/auth/login` | Dashboard 登录，设置 `ferromq_session` Cookie |
| POST | `/auth/logout` | 清除会话 Cookie |
| GET | `/auth/me` | 当前用户（`session` / `bearer` / `anonymous`） |
| POST | `/auth/change-password` | 修改当前会话用户密码 |
| POST | `/auth/init` | 按配置一次性引导管理员 |
| GET | `/openapi.json` | `/api/v1` 的 OpenAPI 3 文档 |
| GET | `/docs` | OpenAPI 的 Swagger UI |
| **Broker** | | |
| GET | `/brokers` | 返回集群中所有节点的基本信息 |
| GET | `/brokers/{id}` | 返回指定节点的基本信息 |
| **节点** | | |
| GET | `/nodes` | 返回集群中所有节点的状态 |
| GET | `/nodes/{id}` | 返回指定节点的状态 |
| **功能支持** | | |
| GET | `/features` | 返回集群各节点的功能支持状态 |
| GET | `/features/{id}` | 返回指定节点的功能支持状态 |
| **健康检查** | | |
| GET | `/health/check` | 集群健康检查 |
| GET | `/health/check/{id}` | 指定节点健康检查 |
| **客户端** | | |
| GET | `/clients` | 从集群中搜索客户端信息 |
| GET | `/clients/{clientid}` | 获取指定客户端信息 |
| DELETE | `/clients/{clientid}` | 从集群中踢出客户端 |
| GET | `/clients/{clientid}/online` | 检查客户端是否在线 |
| GET | `/clients/offlines` | 搜索离线客户端信息 |
| DELETE | `/clients/offlines` | 从集群中踢出离线客户端 |
| **订阅** | | |
| GET | `/subscriptions` | 从集群中查询订阅信息 |
| GET | `/subscriptions/{clientid}` | 获取指定客户端的订阅 |
| **路由** | | |
| GET | `/routes` | 返回集群中所有路由信息 |
| GET | `/routes/{topic}` | 获取指定主题的路由信息 |
| **保留消息** | | |
| GET | `/retains` | 查询保留消息，支持 `topic_filter` / `offset` / `limit` 参数 |
| DELETE | `/retains?topic=` | 按精确主题删除一条保留消息 |
| **MQTT 操作** | | |
| POST | `/mqtt/publish` | 发布 MQTT 消息 |
| POST | `/mqtt/subscribe` | 为会话订阅 MQTT 主题 |
| POST | `/mqtt/unsubscribe` | 取消订阅 MQTT 主题 |
| **插件** | | |
| GET | `/plugins` | 返回集群中所有插件的信息 |
| GET | `/plugins/{node}` | 返回指定节点的插件信息 |
| GET | `/plugins/{node}/{plugin}` | 获取指定插件的详细信息 |
| GET | `/plugins/{node}/{plugin}/config` | 获取插件配置（默认脱敏；`?reveal=1` 仅 admin） |
| PUT | `/plugins/{node}/{plugin}/config` | 写入插件配置（`?apply=reload\|none`），返回 diff 与 `effective` |
| POST | `/plugins/{node}/{plugin}/config/validate` | 校验插件配置（不落盘） |
| GET | `/plugins/{node}/{plugin}/config/versions` | 列出最近 N 份配置备份 |
| POST | `/plugins/{node}/{plugin}/config/rollback/{version}` | 回滚插件配置 |
| PUT | `/plugins/{node}/{plugin}/config/reload` | 重新加载插件配置 |
| GET | `/broker/config` | 只读 `ferromq.toml` 总览（mqtt/listener/log） |
| PUT | `/broker/config/{mqtt\|listener\|log}` | 写入 Broker 段（admin；始终 `restart_required`） |
| GET/POST | `/acl/rules` | 结构化 `ferromq-acl` 规则（经 `load_config` 热生效） |
| GET | `/auth-providers` | MQTT 客户端认证插件（`http` / `jwt`） |
| GET | `/blacklist` | 无独立黑名单插件（诚实缺口） |
| GET/POST | `/auto-subscriptions` | 自动订阅 |
| GET/POST | `/topic-rewrites` | 主题重写 |
| GET/POST | `/webhooks` | Webhook；`POST /webhooks/test` 为 TCP stub |
| GET/PUT | `/bridges/{plugin}` | Bridge 状态 / 配置 / 加载卸载 |
| GET | `/alarms` | 由健康检查 / 功能一致性 / 不可达节点派生的当前告警（内存） |
| GET | `/alarms/history` | 已清除告警（进程内存） |
| POST | `/alarms/{id}/acknowledge` | 确认当前告警（operator+） |
| GET | `/logs` | 无日志查询插件（`available: false`） |
| GET | `/trace` | 无报文追踪插件 |
| GET | `/slow-subs` | 无慢订阅统计 |
| GET | `/topic-metrics` | 由路由派生的订阅数；无按主题速率 |
| GET | `/cluster` | 只读拓扑（`standalone` / `raft` / `broadcast`） |
| POST | `/cluster/join` | 始终 501（仅启动时加入）；按节点结果 |
| POST | `/cluster/leave` | `ferromq-cluster-raft` 的 `Plugin::send` leave，否则 501 |
| PUT | `/plugins/{node}/{plugin}/load` | 在节点上加载/启动插件 |
| PUT | `/plugins/{node}/{plugin}/unload` | 在节点上卸载/停止插件 |
| **统计** | | |
| GET | `/stats` | 返回集群中所有统计信息 |
| GET | `/stats/sum` | 汇总集群中所有统计信息 |
| GET | `/stats/{id}` | 返回指定节点的统计信息 |
| GET | `/stats/sys` | 返回集群中所有系统统计信息 |
| GET | `/stats/sys/sum` | 汇总集群中所有系统统计信息 |
| GET | `/stats/sys/{id}` | 返回指定节点的系统统计信息 |
| **指标** | | |
| GET | `/metrics` | 返回集群中所有指标 |
| GET | `/metrics/sum` | 汇总集群中所有指标 |
| GET | `/metrics/{id}` | 返回指定节点的指标 |
| GET | `/metrics/prometheus` | 获取集群的 Prometheus 指标 |
| GET | `/metrics/prometheus/sum` | 汇总 Prometheus 指标 |
| GET | `/metrics/prometheus/{id}` | 获取指定节点的 Prometheus 指标 |

### 保留消息查询

`GET /retains` 查询保留消息：

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `topic_filter` | string | `#` | 主题过滤器，支持 `#` / `+` 通配；为空或 `#` 时走全量分页 |
| `offset` | usize | `0` | 分页偏移量 |
| `limit` | usize | `max_row_limit` | 每页条数，超出 `max_row_limit` 时收敛 |

响应示例：

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
        "payload": "<base64 编码>",
        "create_time": 1780000000000,
        "properties": null
      },
      "remaining_ttl": 3599
    }
  ],
  "has_more": false
}
```

> 说明：`topic_filter=#`（全量）路径由存储层分页并附带 `remaining_ttl`（剩余秒数）；指定 `topic_filter` 的过滤路径在内存分页，`remaining_ttl` 为 `null`。保留消息在集群中通过广播保持各节点同步，单节点查询即覆盖全集群。

### 功能支持查询

`GET /features` 返回集群各节点功能支持状态及一致性汇总：

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

- `consistent`：所有可达节点功能状态是否完全一致；`false` 时说明存在节点配置漂移或插件加载失败
- `failed_count` / `partial`：不可达节点（HTTP 200，节点项为 `{ ok, error }`）
- `enabled`：各标志在可达节点上的 OR 聚合，供仪表盘菜单门控
- `conflicts`：取值不一致的字段（按值分组列出节点），`consistent` 为 `true` 时为空
- `nodes`：逐节点 `{ ok, node_id, features? / error? }`（不再使用裸错误字符串）
- 检测到不一致时后端会输出 `features inconsistent across cluster` 警告日志

### 诊断与集群（P6）

| 能力 | 真实 / 缺口 |
|------|-------------|
| 告警 | **派生**总线（健康检查 / 功能一致性 / 不可达节点）。无独立告警插件，不落盘。 |
| 日志 / 追踪 / 慢订阅 | `available: false`（无采集插件）。 |
| 主题指标 | **由路由派生**的订阅数。无按主题速率。仅当加载 `ferromq-sys-topic` 时列出 `$SYS`。 |
| 集群 GET | 真实拓扑。`/brokers` 与 `/nodes` 增加附加 `cluster` 字段。 |
| 集群 join | **501** — `Raft::join` 仅启动时（`raft_peer_addrs`）。 |
| 集群 leave | `ferromq-cluster-raft` 激活时真实（`Plugin::send` → `Mailbox::leave`）；否则 501。写入始终返回按节点结果。 |

OpenAPI 3：`GET /api/v1/openapi.json`。可选列表信封：`?format=page` → `{ items, offset, limit, truncated, total? }`（默认仍为裸数组）。错误体：`{ code, message, details?, request_id }`。

### 认证

HTTP API 接受两种凭证（**不影响 MQTT 客户端认证插件**）：

- **会话 Cookie** — `POST /api/v1/auth/login` 提交 `{ username, password }`。Cookie `ferromq_session` 为 `HttpOnly`、`SameSite=Lax`，`dashboard_cookie_secure` 为 true 时带 `Secure`。角色：`admin`（用户 / Key / 审计 + 写操作）、`operator`（踢人 / 发布 / 插件）、`viewer`（只读）。
- **Bearer 令牌** — `Authorization: Bearer <http_bearer_token>` 仍是自动化用的超级用户 admin 凭证（用户名 `operator`）。
- **API Key** — `POST /api/v1/api-keys`（admin）。密钥以 SHA-256 哈希保存，明文只返回一次；用 `Authorization: Bearer <secret>` 认证，角色随 Key 绑定。
- **开放访问** — 未配置 bearer 与 `dashboard_admin_password`（且尚无用户/Key）时保持开放（匿名 admin）。

引导：尚无用户时，首次匹配的登录或 `POST /api/v1/auth/init` 会创建配置中的 admin（及可选 viewer）。密码以 bcrypt 哈希保存在**进程内存**（重启丢失；集群节点不共享会话，需粘性会话）。健康检查、OpenAPI、login/logout/init 保持公开。仅 admin：`GET/POST /users`、`GET/POST/DELETE /api-keys`、`GET /audit`。

## Dashboard

默认界面是 [`sllt/ferromq-dashboard`](https://github.com/sllt/ferromq-dashboard) 的 React 控制台，以 `dashboard-dist/` 入库并用 rust-embed 嵌入（Hash Router，`base: './'`）。打开 `http://<host>:6060/dashboard/`。

- **嵌入（默认）：** 无需额外文件。重新生成：`./scripts/sync-dashboard-dist.sh`，然后 `cargo build -p ferromq-http-api`。
- **覆盖：** 将 `dashboard_static_dir` 设为磁盘上存在的 Vite `dist/`（相对进程 cwd）。路径不存在时打警告并回退到嵌入资源。
- **开发热更新：** 在 dashboard 仓库运行 `pnpm dev`；不要把 `dashboard_static_dir` 指到源码树。
- **验证：** `curl -sI http://127.0.0.1:6060/dashboard/`（`no-cache`、`nosniff`、`DENY`）；带 hash 的 `/dashboard/assets/*.js` 为 `immutable`。`/api/v1/openapi.json` 与 `/api/v1/docs` 仍走 API 路由。

详见 [docs/zh_CN/dashboard.md](../../docs/zh_CN/dashboard.md)。

## 依赖

`ferromq`（features: `plugin`、`metrics`、`stats`、`grpc`、`shared-subscription`）、`salvo`、`tokio`、`serde`、`serde_json`、`anyhow`、`base64`、`futures`

## 许可证

MIT OR Apache-2.0
