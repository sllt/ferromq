[English](README.md) | [**简体中文**](README-CN.md)

# FerroMQ Server

核心 MQTT Broker 库 — 会话管理、路由、Hook、插件和集群协调。

## 模块结构

```
ferromq/src/
├── lib.rs          — 重新导出：ferromq_codec as codec、ferromq_net as net、ferromq_utils as utils
│
├── acl.rs          — ACL 类型（ACLConfig、AclCheckFn、AuthInfo）
├── args.rs         — CommandArgs 结构体（node_id、plugins_default_startups 等）
├── context.rs      — ServerContext Builder（流畅 API：.node()、.task_exec_workers()、.plugins_config_dir() 等）
├── executor.rs     — 异步任务执行器（封装 rust-box task-exec-queue）
├── extend.rs       — 扩展点（10 个 RwLock 保护的插槽）
├── fitter.rs       — 主题过滤器匹配引擎
├── hook.rs         — Hook 系统（Hook trait，10+ 个 Hook 点：message_publish、client_keepalive、session_created 等）
├── inflight.rs     — 进行中消息追踪（InInflight、OutInflight、OutInflightMessage）
├── node.rs         — 集群节点（Node::new()、Node::version()、gRPC 服务启动）
├── queue.rs        — 消息队列（Limiter、Policy、速率限制队列）
├── router.rs       — 基于主题的消息路由（publish、subscribe、unsubscribe、离线投递）
├── server.rs       — MqttServer（Builder：.listener()、.build()、.start()；连接接受循环）
├── session.rs      — 会话处理（~2400 行：连接、断开、订阅、发布、QoS 流程）
├── shared.rs       — 共享订阅（$share/{group}/{topic}）
├── topic.rs        — 主题解析/验证（TopicFilter、parse_topic_filter、topic_size）
├── trie.rs         — 订阅匹配的 Trie 树
├── types.rs        — 核心类型（~3000 行：ConnectInfo、Publish、Packet、Reason、Id、SessionTx 等）
├── v3.rs           — MQTT v3.1.1 协议处理器
├── v5.rs           — MQTT v5.0 协议处理器
│
├── delayed.rs      — [feature: delayed] 延迟消息发布
├── grpc.rs         — [feature: grpc] gRPC 节点间通信
├── message.rs      — [feature: msgstore] 消息存储子系统
├── metrics.rs      — [feature: metrics] 指标收集
├── plugin.rs       — [feature: plugin] Plugin trait 和注册
├── retain.rs       — [feature: retain] 保留消息存储
├── stats.rs        — [feature: stats] 运行时统计
├── subscribe.rs    — [feature: auto-subscription|shared-subscription] 订阅服务
```

## Feature 标志

| Feature | 启用的依赖 | 说明 |
|---------|-------------|------|
| `metrics` | ferromq-macros/metrics | 指标收集 |
| `stats` | — | 运行时统计追踪 |
| `plugin` | ferromq-macros/plugin | 插件系统 |
| `grpc` | rust-box/handy-grpc | gRPC 节点间通信 |
| `tls` | ferromq-net/tls | TLS 传输 |
| `ws` | ferromq-net/ws | WebSocket 传输 |
| `quic` | ferromq-net/quic | QUIC 传输 |
| `delayed` | — | 延迟消息发布 |
| `retain` | — | 保留消息存储 |
| `msgstore` | — | 消息持久化 |
| `shared-subscription` | — | 共享订阅（$share/） |
| `auto-subscription` | — | 连接时自动订阅 |
| `limit-subscription` | — | 订阅限制 |
| `macros` | dep:ferromq-macros | 同时启用 metrics + plugin |
| `full` | 以上全部 | 所有功能 |
| `debug` | — | 调试模式 |
| `default` | （无） | 最小构建 |

## 重新导出

```rust
pub use ferromq_codec as codec;   // MQTT 协议编解码
pub use ferromq_net as net;       // 网络层（Builder、MqttStream 等）
pub use ferromq_utils as utils;   // 工具（Bytesize、NodeAddr 等）
pub use ferromq_macros as macros; // [features: metrics|plugin] 派生宏
pub use net::{Error, Result};   // 重新导出的错误类型
```

## 使用方式

```rust,no_run
use ferromq::context::ServerContext;
use ferromq::net::Builder;
use ferromq::server::MqttServer;
use ferromq::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let scx = ServerContext::new().build().await;

    MqttServer::new(scx)
        .listener(Builder::new().name("tcp").laddr("0.0.0.0:1883".parse()?).bind()?.tcp()?)
        .listener(Builder::new().name("ws").laddr("0.0.0.0:8080".parse()?).bind()?.ws()?)
        .build()
        .run()
        .await?;
    Ok(())
}
```

## 示例

参见 `ferromq/examples/`：`simple`、`simple_tls`、`simple_ws`、`simple_wss`、`simple_quic`、`multi`、`plugin`、`plugins`、`simple_quic_client`。

## 许可证

MIT OR Apache-2.0
