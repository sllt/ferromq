//! Example: MQTT server with secure WebSocket (WSS) transport on port 8443.
//! Demonstrates how to configure TLS for WebSocket connections.

use ferromq::{context::ServerContext, net::Builder, server::MqttServer, Result};
use simple_logger::SimpleLogger;

#[tokio::main]
async fn main() -> Result<()> {
    SimpleLogger::new().with_level(log::LevelFilter::Info).init()?;

    let scx = ServerContext::new().build().await;

    MqttServer::new(scx)
        .listener(
            Builder::new()
                .name("external/wss")
                .laddr(([0, 0, 0, 0], 8443).into())
                .tls_key(Some("./ferromq-bin/ferromq.key"))
                .tls_cert(Some("./ferromq-bin/ferromq.pem"))
                .bind()?
                .wss()?,
        )
        .build()
        .run()
        .await?;
    Ok(())
}
