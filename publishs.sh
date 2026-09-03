#!/bin/bash

cargo publish --registry crates-io --all-features --manifest-path ferromq/Cargo.toml

sleep 15

cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-acl/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-retainer/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-http-api/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-counter/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-auth-http/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-auth-jwt/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-auto-subscription/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-shared-subscription/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-bridge-egress-kafka/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-bridge-ingress-kafka/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-bridge-egress-mqtt/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-bridge-ingress-mqtt/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-bridge-ingress-pulsar/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-bridge-egress-pulsar/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-bridge-ingress-nats/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-bridge-egress-nats/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-bridge-egress-reductstore/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-bridge-origin/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-message-storage/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-session-storage/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-sys-topic/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-topic-rewrite/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-web-hook/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-cluster-raft/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-cluster-broadcast/Cargo.toml
cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/ferromq-p2p-messaging/Cargo.toml

sleep 15

cargo publish --registry crates-io --all-features --manifest-path ferromq-plugins/Cargo.toml

sleep 5

cargo publish --registry crates-io --all-features --manifest-path ferromq-bin/Cargo.toml

# cargo publish --registry crates-io --all-features --manifest-path ferromq-macros/Cargo.toml
# cargo publish --registry crates-io --all-features --manifest-path ferromq-utils/Cargo.toml
# cargo publish --registry crates-io --all-features --manifest-path ferromq-codec/Cargo.toml
# cargo publish --registry crates-io --all-features --manifest-path ferromq-net/Cargo.toml
# cargo publish --registry crates-io --all-features --manifest-path ferromq-conf/Cargo.toml

