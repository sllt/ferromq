//! ferromq-plugins
//!
//! A collection of plugins for the FerroMQ MQTT broker, providing functionality
//! for various features such as authentication, message retention, HTTP APIs,
//! and more. Each plugin can be conditionally included based on feature flags,
//! allowing for modularity and customization. The plugins are categorized into
//! core, bridge, storage, utility, and cluster plugins, making it easy to extend
//! FerroMQ with different backend systems or protocols.
//!
//! The following categories of plugins are available:
//! - **Core Plugins**: Fundamental plugins for core functionality such as ACL,
//!   retention, and HTTP API support.
//! - **Bridge Plugins**: Plugins to integrate with external message brokers like
//!   Kafka, Pulsar, NATS, and more.
//! - **Storage Plugins**: Plugins for message and session storage with support
//!   for different backends like Redis and SLED.
//! - **Utility Plugins**: Additional utility features such as system topics,
//!   topic rewrites, and web hooks.
//! - **Cluster Plugins**: Support for clustering features such as Raft and
//!   broadcast modes for distributed setups.

#![deny(missing_docs)]

// ---- Core Plugins ----
#[cfg(feature = "acl")]
pub use ferromq_acl as acl;

#[cfg(any(
    feature = "retainer",
    feature = "retainer-ram",
    feature = "retainer-sled",
    feature = "retainer-redis"
))]
pub use ferromq_retainer as retainer;

#[cfg(feature = "http-api")]
pub use ferromq_http_api as http_api;

#[cfg(feature = "counter")]
pub use ferromq_counter as counter;

#[cfg(feature = "auth-http")]
pub use ferromq_auth_http as auth_http;

#[cfg(feature = "auth-jwt")]
pub use ferromq_auth_jwt as auth_jwt;

#[cfg(feature = "auto-subscription")]
pub use ferromq_auto_subscription as auto_subscription;

// ---- Bridge Plugins ----
#[cfg(feature = "bridge-egress-kafka")]
pub use ferromq_bridge_egress_kafka as bridge_egress_kafka;

#[cfg(feature = "bridge-ingress-kafka")]
pub use ferromq_bridge_ingress_kafka as bridge_ingress_kafka;

#[cfg(feature = "bridge-egress-mqtt")]
pub use ferromq_bridge_egress_mqtt as bridge_egress_mqtt;

#[cfg(feature = "bridge-ingress-mqtt")]
pub use ferromq_bridge_ingress_mqtt as bridge_ingress_mqtt;

#[cfg(feature = "bridge-egress-pulsar")]
pub use ferromq_bridge_egress_pulsar as bridge_egress_pulsar;

#[cfg(feature = "bridge-ingress-pulsar")]
pub use ferromq_bridge_ingress_pulsar as bridge_ingress_pulsar;

#[cfg(feature = "bridge-egress-nats")]
pub use ferromq_bridge_egress_nats as bridge_egress_nats;

#[cfg(feature = "bridge-egress-reductstore")]
pub use ferromq_bridge_egress_reductstore as bridge_egress_reductstore;

#[cfg(feature = "bridge-origin")]
pub use ferromq_bridge_origin as bridge_origin;

// ---- Storage Plugins ----
#[cfg(any(
    feature = "message-storage",
    feature = "message-storage-ram",
    feature = "message-storage-redis",
    feature = "message-storage-redis-cluster"
))]
pub use ferromq_message_storage as message_storage;

#[cfg(any(
    feature = "session-storage",
    feature = "session-storage-sled",
    feature = "session-storage-redis",
    feature = "session-storage-redis-cluster"
))]
pub use ferromq_session_storage as session_storage;

// ---- Utility Plugins ----
#[cfg(feature = "sys-topic")]
pub use ferromq_sys_topic as sys_topic;

#[cfg(feature = "topic-rewrite")]
pub use ferromq_topic_rewrite as topic_rewrite;

#[cfg(feature = "web-hook")]
pub use ferromq_web_hook as web_hook;

#[cfg(feature = "p2p-messaging")]
pub use ferromq_p2p_messaging as p2p_messaging;

#[cfg(feature = "shared-subscription")]
pub use ferromq_shared_subscription as shared_subscription;

// ---- Cluster Plugins ----
#[cfg(feature = "cluster-raft")]
pub use ferromq_cluster_raft as cluster_raft;

#[cfg(feature = "cluster-broadcast")]
pub use ferromq_cluster_broadcast as cluster_broadcast;
