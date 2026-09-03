[English](README.md) | [**简体中文**](README-CN.md)

# ferromqd

![Rust](https://img.shields.io/badge/rust-1.94%2B-blue)

FerroMQ MQTT Broker 的官方二进制入口。

## 实际功能

- **启动流程**：解析 CLI 参数 → 初始化 `ferromq_conf::Settings` 单例 → 安装 rustls 加密后端 → 初始化 tracing 日志 → 创建 `ferromq::context::ServerContext` → 启动 gRPC 服务 → 从 `Cargo.toml` 元数据注册插件 → 绑定配置的监听器 → 启动 MQTT 服务
- **构建时插件注册**：`build.rs` 读取 `Cargo.toml` 中 `[package.metadata.plugins]` 配置，自动生成 `plugin.rs`，包含 `registers()` 函数。每个插件根据 `default_startup` 和 `immutable` 标志注册
- **监听器类型**：TCP、TLS、WebSocket (WS)、TLS-WebSocket (WSS)、QUIC
- **CLI 参数**：参见下方命令行参数表格（通过 `ferromq_conf::Options` 定义，`clap::Parser` 解析）
- **信号处理**：Windows 上监听 `Ctrl+C`；非 Windows 上监听 `SIGTERM` + `SIGINT`，收到信号后 100ms 延时退出
- **日志**：通过 `ferromq_conf::logging::Log` 配置，支持 `off/console/file/both` 模式，UTC+8 时间戳，非阻塞文件写入
- **Linux 分配器**：使用 `tikv-jemallocator` 作为默认内存分配器

## 构建

```bash
cargo build -p ferromqd --release
# 产物: target/release/ferromqd（以及兼容别名 target/release/ferromqd）
```

## 运行

```bash
./target/release/ferromqd
./target/release/ferromqd -f /path/to/ferromq.toml
./target/release/ferromqd --config /path/to/ferromq.toml
./target/release/ferromqd --id 1
# 兼容别名（会打印弃用提示）：
./target/release/ferromqd
```

## 命令行参数

通过 `ferromq_conf::Options` 结构体（`clap::Parser`）定义：

| 参数 | 类型 | 说明 |
|------|------|------|
| `-f`, `--config` | `Option<String>` | 配置文件路径 |
| `-V`, `--version` | `bool` | 打印版本信息 |
| `--id` | `Option<u64>` | 节点 ID |
| `--plugins-default-startups` | `Option<Vec<String>>` | 覆盖默认启动插件列表（可多次使用） |
| `--node-grpc-addrs` | `Option<Vec<NodeAddr>>` | 节点 gRPC 地址列表，格式 `"1@127.0.0.1:5363"`（可多次使用） |
| `--raft-peer-addrs` | `Option<Vec<NodeAddr>>` | Raft 对端地址列表，格式 `"1@127.0.0.1:6003"`（可多次使用） |
| `--raft-leader-id` | `Option<u64>` | Raft Leader ID；默认为 0（第一个节点被选为 Leader） |

## 配置

通过 `ferromq_conf::Settings` 加载配置。后加载的来源覆盖先加载的：

1. `/etc/ferromq/ferromq.{toml,json,...}` 与 `/etc/ferromq.{toml,json,...}`（可选）
2. `./ferromq.{toml,json,...}`（可选）
3. `FERROMQ_*` 环境变量
4. `-f` / `--config` 指定的文件
5. CLI 参数（`--id`、插件启动列表、集群地址）

## Docker

支持三种架构的 Dockerfile：
- `Dockerfile` — 默认
- `Dockerfile.amd64` — x86_64
- `Dockerfile.aarch64` — ARM64

```bash
docker build -t ferromqd .
```

## 相关 crate

- [ferromq] — 核心 MQTT Broker 库
- [ferromq-conf] — 配置管理
- [ferromq-plugins] — 插件集合

[ferromq]: https://github.com/sllt/ferromq/tree/main/ferromq
[ferromq-conf]: https://github.com/sllt/ferromq/tree/main/ferromq-conf
[ferromq-plugins]: https://github.com/sllt/ferromq/tree/main/ferromq-plugins

## 许可证

MIT OR Apache-2.0
