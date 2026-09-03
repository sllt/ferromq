[English](../../en_US/reference/http-api.md) | [**简体中文**](http-api.md)

# HTTP API 参考

FerroMQ HTTP API 提供 Broker 管理的 RESTful 端点，涵盖监控、客户端管理、订阅、消息和插件控制。

## 配置

由 `ferromq-http-api` 插件提供。

```toml
# ferromq-http-api.toml
http_laddr = "0.0.0.0:6060"
max_row_limit = 10_000
# 请求日志（/auth/* 省略 body；密钥与 URL userinfo 脱敏；不记录 Authorization/Cookie）
http_request_log = false
message_expiry_interval = "5m"
prometheus_metrics_cache_interval = "5s"
# http_bearer_token = "your-secret-token"
# dashboard_admin_username = "admin"
# dashboard_admin_password = "change-me"
```

## 端点速查

| 方法 | 路径 | 说明 |
|--------|------|------|
| `GET` | `/api/v1` | API 列表 |
| `POST` | `/api/v1/auth/login` | Dashboard 登录（会话 Cookie） |
| `POST` | `/api/v1/auth/logout` | 退出登录 |
| `GET` | `/api/v1/auth/me` | 当前用户 |
| `POST` | `/api/v1/auth/change-password` | 修改密码 |
| `POST` | `/api/v1/auth/init` | 按配置一次性引导管理员 |
| `GET` | `/api/v1/users` | 列出 Dashboard 用户（admin） |
| `POST` | `/api/v1/users` | 创建用户（admin） |
| `POST` | `/api/v1/users/{username}/disable` | 禁用用户（admin） |
| `GET` | `/api/v1/api-keys` | 列出 API Key（admin，不含密钥） |
| `POST` | `/api/v1/api-keys` | 创建 API Key，密钥只返回一次（admin） |
| `DELETE` | `/api/v1/api-keys/{id}` | 吊销 API Key（admin） |
| `GET` | `/api/v1/audit` | 审计日志（admin，可分页） |
| `GET` | `/api/v1/openapi.json` | OpenAPI 3 契约 |
| `GET` | `/api/v1/docs` | Swagger UI |
| `GET` | `/api/v1/brokers` | 集群节点信息 |
| `GET` | `/api/v1/features` | 功能支持状态（含集群一致性汇总） |
| `GET` | `/api/v1/features/{id}` | 指定节点功能支持状态 |
| `GET` | `/api/v1/health/check` | 健康检查 |
| `GET` | `/api/v1/clients` | 搜索客户端 |
| `DELETE` | `/api/v1/clients/{clientid}` | 踢出客户端 |
| `GET` | `/api/v1/subscriptions` | 订阅列表 |
| `GET` | `/api/v1/routes` | 路由表 |
| `GET` | `/api/v1/retains` | 查询保留消息 |
| `DELETE` | `/api/v1/retains?topic=` | 按精确主题删除保留消息 |
| `POST` | `/api/v1/mqtt/publish` | 发布消息 |
| `POST` | `/api/v1/mqtt/subscribe` | 订阅主题 |
| `POST` | `/api/v1/mqtt/unsubscribe` | 取消订阅 |
| `GET` | `/api/v1/plugins` | 插件列表 |
| `GET` | `/api/v1/plugins/{node}/{plugin}/config` | 读插件配置（默认脱敏） |
| `PUT` | `/api/v1/plugins/{node}/{plugin}/config` | 写插件配置（operator+） |
| `POST` | `/api/v1/plugins/{node}/{plugin}/config/validate` | 校验插件配置（不落盘） |
| `GET` | `/api/v1/plugins/{node}/{plugin}/config/versions` | 插件配置备份列表 |
| `POST` | `/api/v1/plugins/{node}/{plugin}/config/rollback/{version}` | 回滚插件配置 |
| `PUT` | `/api/v1/plugins/{node}/{plugin}/config/reload` | 重载插件配置 |
| `GET` | `/api/v1/broker/config` | 只读 ferromq.toml 总览 |
| `PUT` | `/api/v1/broker/config/{mqtt\|listener\|log}` | 写 Broker 段（admin，仅落盘，`restart_required`） |
| `GET` / `POST` | `/api/v1/acl/rules` | ACL 规则列表 / 添加（P5，热加载） |
| `GET` | `/api/v1/auth-providers` | MQTT 客户端认证插件（非 Dashboard 登录） |
| `GET` | `/api/v1/blacklist` | 无独立黑名单插件（诚实缺口） |
| `GET` / `POST` | `/api/v1/auto-subscriptions` | 自动订阅 |
| `GET` / `POST` | `/api/v1/topic-rewrites` | 主题重写 |
| `GET` / `POST` | `/api/v1/webhooks` / `.../urls` / `.../test` | Webhook（密钥脱敏；test 为 TCP stub） |
| `GET` / `PUT` | `/api/v1/bridges` / `{plugin}` | Bridge 状态 + P4 配置写入 |
| `GET` | `/api/v1/alarms` | 派生告警（健康检查 / 功能 / 不可达节点） |
| `GET` | `/api/v1/logs` / `/trace` / `/slow-subs` | 无采集插件（`available: false`） |
| `GET` | `/api/v1/topic-metrics` | 由路由派生的订阅数 |
| `GET` | `/api/v1/cluster` | 只读拓扑 |
| `POST` | `/api/v1/cluster/join` / `leave` | join 始终 501；leave 仅 raft |
| `GET` | `/api/v1/stats` | 统计信息 |
| `GET` | `/api/v1/metrics` | 指标（JSON） |
| `GET` | `/api/v1/metrics/prometheus` | 指标（Prometheus 格式） |

完整端点列表含 `stats/history`、`metrics/history`、会话认证、API Key 与审计日志。`operator` 可踢人 / 发布 / 管理插件，但不能管理用户与 Key；`viewer` 只读。Cookie CSRF 用 Origin/Referer 对照 Host，反向代理须保留原始公网 Host（不信任 `X-Forwarded-Host`）。详情见英文版文档。

## 许可证

MIT OR Apache-2.0
