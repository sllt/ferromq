[English](README.md) | [**简体中文**](README-CN.md)

# ferromq-macros

[![crates.io page](https://img.shields.io/crates/v/ferromq-macros.svg)](https://crates.io/crates/ferromq-macros)
[![docs.rs page](https://docs.rs/ferromq-macros/badge.svg)](https://docs.rs/ferromq-macros/latest/ferromq_macros)

FerroMQ 生态的过程宏。通过 feature 门控。

## 派生宏

### `#[derive(Metrics)]` — feature `metrics`

为每个字段生成基于 `AtomicUsize` 的计数器：

```rust,ignore
impl MyStruct {
    pub fn new() -> Self;                              // 全零初始化
    pub fn {field}_inc(&self);                         // fetch_add(1, SeqCst)
    pub fn {field}(&self) -> usize;                    // load(SeqCst)
    pub fn to_json(&self) -> serde_json::Value;        // {"field.name": value, ...}
    pub fn add(&mut self, other: &Self);               // 原子合并
    pub fn build_prometheus_metrics(&self, label: &str, gauge_vec: &IntGaugeVec);
}
impl Clone for MyStruct { ... }                        // 通过 AtomicUsize::new(load(...)) 克隆
```

### `#[derive(Plugin)]` — feature `plugin`

生成 `impl ferromq::plugin::PackageInfo`，编译时从 Cargo 环境变量读取包元数据：

```rust,ignore
impl ferromq::plugin::PackageInfo for MyStruct {
    fn name(&self) -> &str;              // env!("CARGO_PKG_NAME")
    fn version(&self) -> &str;           // env!("CARGO_PKG_VERSION")
    fn descr(&self) -> Option<&str>;     // env!("CARGO_PKG_DESCRIPTION")
    fn authors(&self) -> Option<Vec<&str>>;  // env!("CARGO_PKG_AUTHORS")
    fn homepage(&self) -> Option<&str>;  // env!("CARGO_PKG_HOMEPAGE")
    fn license(&self) -> Option<&str>;   // env!("CARGO_PKG_LICENSE")
    fn repository(&self) -> Option<&str>;// env!("CARGO_PKG_REPOSITORY")
}
```

## Cargo.toml

```toml
[dependencies]
ferromq-macros = { version = "0.1", features = ["metrics", "plugin"] }
```

## 许可证

MIT OR Apache-2.0
