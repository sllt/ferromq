[**English**](README.md) | [简体中文](../zh_CN/README.md)

# FerroMQ Documentation

Welcome to the FerroMQ documentation. This index provides a structured overview of all available documentation resources.

## Quick Links

| Resource | Description |
|----------|-------------|
| [GitHub Repository](https://github.com/rmqtt/rmqtt) | Source code, issues, discussions |
| [crates.io](https://crates.io/crates/rmqtt) | Published crate versions |
| [docs.rs](https://docs.rs/rmqtt/latest/rmqtt/) | API reference (library mode) |

---

## Architecture

| Document | Description |
|----------|-------------|
| [Architecture Overview](architecture/overview.md) | System architecture, crate layers, core modules, session lifecycle |
| [Plugin System](../architecture/overview.md#plugin-system) | Plugin trait, lifecycle, registration pattern |
| [Hook System](../architecture/overview.md#hook-system) | All 23 hook types, handler registration, priority |
| [Message Flow](../architecture/overview.md#message-flow) | End-to-end publish/subscribe flow with diagrams |

---

## Getting Started

| Document | Description |
|----------|-------------|
| [Installation Guide](install.md) | Install via Docker, binary package, or source build |
| [MQTT Protocol Support](mqtt-protocol.md) | Supported MQTT versions, features, and configuration |

---

## Configuration

| Document | Description |
|----------|-------------|
| [Configuration Reference](https://github.com/rmqtt/rmqtt/blob/master/ferromq.toml) | Full configuration file example |
| [Permission List](perm-list.md) | Available permissions and their meanings |

---

## Features

### Authentication & Access Control

| Document | Description |
|----------|-------------|
| [ACL (Access Control List)](acl.md) | File-based ACL rule engine |
| [HTTP Authentication](auth-http.md) | External HTTP API authentication |
| [JWT Authentication](auth-jwt.md) | JSON Web Token validation |

### Message Storage & Delivery

| Document | Description |
|----------|-------------|
| [Retained Messages](retainer.md) | Persistent retained message storage |
| [Offline Messages](offline-message.md) | Message storage for disconnected clients |
| [Session Storage](store-session.md) | Session state persistence |
| [Message Storage](store-message.md) | Unexpired message persistence |

### Clustering

| Document | Description |
|----------|-------------|
| [Raft Cluster](cluster-raft.md) | Strongly consistent clustering via Raft consensus |
| [Benchmark Testing](benchmark-testing.md) | Performance benchmarks (1M clients, 150K msg/s) |

### Bridges

| Document | Direction |
|----------|-----------|
| [MQTT Bridge - Ingress](bridge-ingress-mqtt.md) | Remote MQTT → Local |
| [MQTT Bridge - Egress](bridge-egress-mqtt.md) | Local → Remote MQTT |
| [Kafka Bridge - Ingress](bridge-ingress-kafka.md) | Kafka → Local |
| [Kafka Bridge - Egress](bridge-egress-kafka.md) | Local → Kafka |
| [Pulsar Bridge - Ingress](bridge-ingress-pulsar.md) | Pulsar → Local |
| [Pulsar Bridge - Egress](bridge-egress-pulsar.md) | Local → Pulsar |
| [NATS Bridge - Ingress](bridge-ingress-nats.md) | NATS → Local |
| [NATS Bridge - Egress](bridge-egress-nats.md) | Local → NATS |
| [ReductStore Bridge - Egress](bridge-egress-reductstore.md) | Local → ReductStore |
| [Bridge Origin](bridge-origin.md) | Bridge client identification |

### Management & Monitoring

| Document | Description |
|----------|-------------|
| [HTTP API](http-api.md) | RESTful management API reference |
| [Dashboard](dashboard.md) | Built-in web management UI |
| [WebHook](web-hook.md) | HTTP event notifications |
| [System Topics](sys-topic.md) | `$SYS/` monitoring metrics |

### Topic Features

| Document | Description |
|----------|-------------|
| [Topic Rewrite](topic-rewrite.md) | Topic filter and name rewriting |
| [Auto Subscription](auto-subscription.md) | Automatic subscription on connect |
| [Shared Subscription](shared-subscription.md) | Load-balanced consumer groups |
| [P2P Messaging](p2p-messaging.md) | Direct client-to-client messaging |

---

## Crate Documentation

Each crate has its own bilingual README:

| Crate | Description | README |
|-------|-------------|--------|
| `ferromq` | Core broker library | [README](../ferromq/README.md) |
| `ferromqd` | Binary entry point | [README](../ferromq-bin/README.md) |
| `ferromq-codec` | MQTT protocol codec | [README](../ferromq-codec/README.md) |
| `ferromq-net` | Network layer (TCP/TLS/WS/QUIC) | [README](../ferromq-net/README.md) |
| `ferromq-conf` | Configuration management | [README](../ferromq-conf/README.md) |
| `ferromq-utils` | Shared utilities | [README](../ferromq-utils/README.md) |
| `ferromq-macros` | Procedural macros | [README](../ferromq-macros/README.md) |
| `ferromq-test` | Test harness | [README](../ferromq-test/README.md) |
| `ferromq-plugins` | Plugin collection meta-crate | [README](../ferromq-plugins/README.md) |

### Plugin Crate READMEs

| Category | Plugin | Description |
|----------|--------|-------------|
| **Auth** | [ferromq-acl](../ferromq-plugins/ferromq-acl/README.md) | File-based ACL |
| | [ferromq-auth-http](../ferromq-plugins/ferromq-auth-http/README.md) | HTTP authentication |
| | [ferromq-auth-jwt](../ferromq-plugins/ferromq-auth-jwt/README.md) | JWT authentication |
| **Storage** | [ferromq-retainer](../ferromq-plugins/ferromq-retainer/README.md) | Retained message store |
| | [ferromq-message-storage](../ferromq-plugins/ferromq-message-storage/README.md) | Message persistence |
| | [ferromq-session-storage](../ferromq-plugins/ferromq-session-storage/README.md) | Session persistence |
| **Cluster** | [ferromq-cluster-raft](../ferromq-plugins/ferromq-cluster-raft/README.md) | Raft consensus |
| | [ferromq-cluster-broadcast](../ferromq-plugins/ferromq-cluster-broadcast/README.md) | Broadcast cluster |
| **Bridge** | [ferromq-bridge-*-mqtt](../ferromq-plugins/ferromq-bridge-egress-mqtt/README.md) | MQTT bridge |
| | [ferromq-bridge-*-kafka](../ferromq-plugins/ferromq-bridge-egress-kafka/README.md) | Kafka bridge |
| | [ferromq-bridge-*-pulsar](../ferromq-plugins/ferromq-bridge-egress-pulsar/README.md) | Pulsar bridge |
| | [ferromq-bridge-*-nats](../ferromq-plugins/ferromq-bridge-egress-nats/README.md) | NATS bridge |
| | [ferromq-bridge-egress-reductstore](../ferromq-plugins/ferromq-bridge-egress-reductstore/README.md) | ReductStore bridge |
| | [ferromq-bridge-origin](../ferromq-plugins/ferromq-bridge-origin/README.md) | Bridge origin identification |
| **API** | [ferromq-http-api](../ferromq-plugins/ferromq-http-api/README.md) | HTTP REST API |
| | [ferromq-web-hook](../ferromq-plugins/ferromq-web-hook/README.md) | Webhook notifications |
| | [ferromq-sys-topic](../ferromq-plugins/ferromq-sys-topic/README.md) | System topics |
| **Utility** | [ferromq-counter](../ferromq-plugins/ferromq-counter/README.md) | Metrics counters |
| **Subscription** | [ferromq-shared-subscription](../ferromq-plugins/ferromq-shared-subscription/README.md) | Shared subscription strategies |
| | [ferromq-auto-subscription](../ferromq-plugins/ferromq-auto-subscription/README.md) | Auto-subscription on connect |
| | [ferromq-topic-rewrite](../ferromq-plugins/ferromq-topic-rewrite/README.md) | Topic rewrite |
| | [ferromq-p2p-messaging](../ferromq-plugins/ferromq-p2p-messaging/README.md) | P2P messaging |

---

## Development

| Resource | Description |
|----------|-------------|
| [Contributing Guide](../CONTRIBUTING.md) | Contribution guidelines |
| [Changelog](../CHANGELOG.md) | Release history |
| [Developer Getting Started](development/getting-started.md) | Dev environment setup, build, workflow |
| [Testing Guide](development/testing.md) | Test layers, running tests, writing tests |
| [Test Report](testing-report.md) | Interoperability results and benchmark data |
| [Plugin Development Guide](development/plugin-development.md) | Creating plugins, hook system, lifecycle |
| [FQA](https://github.com/rmqtt/rmqtt/issues) | Issues and discussions |

---

## Reference

| Resource | Description |
|----------|-------------|
| [HTTP API Reference](reference/http-api.md) | Complete REST API endpoint reference (36 endpoints) |

---

## License

FerroMQ is licensed under [MIT](https://opensource.org/licenses/MIT) or [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) at your option.
