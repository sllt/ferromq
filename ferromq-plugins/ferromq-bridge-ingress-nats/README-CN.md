[English](README.md) | [**简体中文**](README-CN.md)

# ferromq-bridge-ingress-nats

[![crates.io](https://img.shields.io/crates/v/ferromq-bridge-ingress-nats.svg)](https://crates.io/crates/ferromq-bridge-ingress-nats)

NATS 入站桥接插件。从 NATS 主题消费消息并发布到本地 FerroMQ Broker。

## 使用方式

```toml
ferromq-bridge-ingress-nats = "0.21"
```

```rust
ferromq_bridge_ingress_nats::register(&scx, true, false).await?;
```

## 配置

文件：`ferromq-bridge-ingress-nats.toml`

```toml
[[bridges]]
enable = true
name = "bridge_nats_1"
servers = "nats://127.0.0.1:4222"
consumer_name_prefix = "consumer_1"

[[bridges.entries]]
remote.topic = "test1"
local.topic = "local/topic1/ingress/${remote.topic}"
```

| 选项 | 类型 | 默认值 | 说明 |
|--------|------|---------|------|
| `bridges[].enable` | `bool` | `true` | 启用桥接 |
| `bridges[].servers` | `string` | — | NATS 服务器地址 |
| `bridges[].entries[].remote.topic` | `string` | — | NATS 消费主题 |
| `bridges[].entries[].local.topic` | `string` | — | 本地 MQTT 主题 |

## 依赖

`ferromq`（feature `plugin`）、`async-nats`、`tokio`

## 许可证

MIT OR Apache-2.0
