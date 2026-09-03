//! Example: MQTT server with plugin support using the `ferromq_plugins` meta-crate.
//! Demonstrates how to register plugins (ACL, HTTP API, Retainer, P2P Messaging)
//! via the unified `ferromq_plugins` module path.

use ferromq::{context::ServerContext, net::Builder, server::MqttServer, Result};
use simple_logger::SimpleLogger;

#[tokio::main]
async fn main() -> Result<()> {
    SimpleLogger::new().with_level(log::LevelFilter::Info).init()?;

    let scx = ServerContext::new().plugins_config_dir("ferromq-plugins/").build().await;

    ferromq_plugins::acl::register(&scx, true, false).await?;
    ferromq_plugins::http_api::register(&scx, true, false).await?;
    ferromq_plugins::retainer::register(&scx, true, false).await?;
    ferromq_plugins::p2p_messaging::register(&scx, true, false).await?;
    // ferromq_plugins::sys_topic::register(&scx, true, false).await?;
    // ferromq_plugins::message_storage::register(&scx, true, false).await?;

    // ferromq_plugins::session_storage::register(&scx, true, false).await?;
    // ferromq_plugins::auth_jwt::register(&scx, true, false).await?;
    // ferromq_plugins::auth_http::register(&scx, true, false).await?;
    // ferromq_plugins::web_hook::register(&scx, true, false).await?;
    // ferromq_plugins::counter::register(&scx, true, false).await?;
    // ferromq_plugins::bridge_egress_kafka::register(&scx, true, false).await?;
    // ferromq_plugins::bridge_ingress_kafka::register(&scx, true, false).await?;
    // ferromq_plugins::auto_subscription::register(&scx, true, false).await?;
    // ferromq_plugins::topic_rewrite::register(&scx, true, false).await?;
    // ferromq_plugins::bridge_egress_mqtt::register(&scx, true, false).await?;
    // ferromq_plugins::bridge_ingress_mqtt::register(&scx, true, false).await?;
    // ferromq_plugins::bridge_egress_pulsar::register(&scx, true, false).await?;
    // ferromq_plugins::bridge_ingress_pulsar::register(&scx, true, false).await?;
    // ferromq_plugins::bridge_egress_nats::register(&scx, true, false).await?;
    // ferromq_plugins::bridge_egress_reductstore::register(&scx, true, false).await?;
    //
    // ferromq_plugins::cluster_raft::register(&scx, true, true).await?;
    // ferromq_plugins::cluster_broadcast::register(&scx, true, true).await?;

    MqttServer::new(scx)
        .listener(
            Builder::new()
                .name("external/tcp")
                .laddr(([0, 0, 0, 0], 1883).into())
                .allow_anonymous(false)
                .bind()?
                .tcp()?,
        )
        .build()
        .run()
        .await?;
    Ok(())
}
