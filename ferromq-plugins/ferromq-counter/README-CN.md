[English](README.md) | [**简体中文**](README-CN.md)

# ferromq-counter

[![crates.io](https://img.shields.io/crates/v/ferromq-counter.svg)](https://crates.io/crates/ferromq-counter)

指标计数器插件。通过 15 个 Hook 回调以 `Priority::MAX` 优先级追踪 MQTT 事件。

## 追踪的事件

**客户端**：`ClientConnect`、`ClientAuthenticate`、`ClientConnack`、`ClientConnected`、`ClientDisconnected`
**会话**：`SessionCreated`、`SessionTerminated`、`SessionSubscribed`、`SessionUnsubscribed`
**订阅**：`ClientSubscribe`、`ClientUnsubscribe`、`ClientSubscribeCheckAcl`、`MessagePublishCheckAcl`
**消息**：`MessagePublish`、`MessageDelivered`、`MessageAcked`、`MessageDropped`、`MessageNonsubscribed`

消息按 QoS 级别（0、1、2）和来源类型（Custom、Admin、System、LastWill、Bridge）进一步细分。

## 使用方法

### 构建

需要 `ferromq` 的 features：`plugin`、`metrics`。

```toml
ferromq-counter = "0.21"
```

### 注册

```rust
ferromq_counter::register(&scx, true, false).await?;
// 或指定名称：
ferromq_counter::register_named(&scx, "ferromq-counter", true, false).await?;
```

参数说明：`(scx, default_startup, immutable)`。

此插件没有配置文件（无 `cfg` 字段）。所有计数器通过 `scx.metrics.*` 访问。

## 依赖

`ferromq`（features: `plugin`、`metrics`）

## 许可证

MIT OR Apache-2.0
