//! HTTP API route handlers for the FerroMQ management API.
//!
//! Defines the HTTP server, route tree, session + Bearer authentication, and
//! handler functions for brokers, nodes, clients, subscriptions, routes,
//! MQTT actions, plugins, stats, metrics, and history.

use std::convert::From as _;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use salvo::conn::tcp::TcpAcceptor;
use salvo::http::header::{HeaderValue, CONTENT_TYPE};
use salvo::http::mime;
use salvo::prelude::*;

use anyhow::anyhow;
use base64::prelude::{Engine, BASE64_STANDARD};
use serde_json::{self, json};
use tokio::sync::oneshot;

use ferromq::{
    codec::v5::PublishProperties,
    context::ServerContext,
    grpc::{
        GrpcClient, Message as GrpcMessage, MessageBroadcaster, MessageReply as GrpcMessageReply,
        MessageSender, MessageType,
    },
    metrics::Metrics,
    net::MqttError,
    node::{NodeInfo, NodeStatus},
    session::SessionState,
    stats::Stats,
    types::NodeId,
    types::{
        ClientId, CodecPublish, From, HashMap, Id, NodeHealthStatus, Publish, QoS, Retain, SubsSearchParams,
        TopicFilter, TopicName, UserName,
    },
    utils::timestamp_millis,
    Result,
};

use salvo::serve_static::{static_embed, StaticDir};

use super::auth::{
    auth_change_password, auth_init, auth_login, auth_logout, auth_me, require_write, AuthGuard, AuthState,
    AUTH_STATE,
};
use super::embed::DashboardAssets;
use super::flusher::{HistoryCache, HistoryCaches};
use super::openapi::{get_docs, get_openapi};
use super::prome::{Monitor, PROME_MONITOR};
use super::response::{
    apply_list_headers, cluster_node_failure, cluster_node_success, new_request_id, render_api_error,
    render_list, render_not_found, status_for_plugin_error, subscription_to_json, wants_page_format,
    ListPaging, DEPOT_REQUEST_ID, HEADER_REQUEST_ID,
};
use super::types::{
    ClientSearchParams, ClientSearchResult, FeatureConflict, FeatureValueGroup, Features, FeaturesInfo,
    FeaturesNodeResult, FeaturesSummary, HistoryData, HistoryQuery, Message, MessageReply,
    PrometheusDataType, PublishParams, RetainInfo, RetainQueryParams, SubscribeParams, UnsubscribeParams,
};
use super::{clients, plugin, prome, subs, PluginConfigType};

/// Depot key for the history caches + storage handle.
const HISTORY_CACHES: &str = "HISTORY_CACHES";

pub(crate) fn route(
    scx: ServerContext,
    cfg: PluginConfigType,
    token: Option<String>,
    monitor: prome::Monitor,
    history_caches: Option<HistoryCaches>,
) -> Router {
    route_with_auth(scx, cfg, Arc::new(AuthState::new(token)), monitor, history_caches)
}

fn route_with_auth(
    scx: ServerContext,
    cfg: PluginConfigType,
    auth_state: Arc<AuthState>,
    monitor: prome::Monitor,
    history_caches: Option<HistoryCaches>,
) -> Router {
    let mut router = Router::with_path("api/v1")
        .hoop(affix_state::inject((scx, cfg)))
        .hoop(affix_state::insert(PROME_MONITOR, monitor))
        .hoop(affix_state::insert(AUTH_STATE, auth_state.clone()))
        .hoop(request_id_hoop)
        .hoop(api_logger);
    // Inject history caches so query handlers can access LRU + storage.
    if let Some(hc) = history_caches {
        router = router.hoop(affix_state::insert(HISTORY_CACHES, hc));
    }

    // Public: login/init/logout, health probes, and the OpenAPI contract.
    let public = Router::new()
        .push(Router::with_path("auth/login").post(auth_login))
        .push(Router::with_path("auth/logout").post(auth_logout))
        .push(Router::with_path("auth/init").post(auth_init))
        .push(Router::with_path("openapi.json").get(get_openapi))
        .push(Router::with_path("docs").get(get_docs))
        .push(
            Router::with_path("health/check")
                .get(check_health)
                .push(Router::with_path("{id}").get(check_health)),
        );

    // Session cookie or Bearer token (or anonymous admin when auth is unset).
    let protected = Router::new()
        .hoop(AuthGuard::new(auth_state))
        .get(list_apis)
        .push(Router::with_path("auth/me").get(auth_me))
        .push(Router::with_path("auth/change-password").post(auth_change_password))
        .push(Router::with_path("brokers").get(get_brokers).push(Router::with_path("{id}").get(get_brokers)))
        .push(Router::with_path("nodes").get(get_nodes).push(Router::with_path("{id}").get(get_nodes)))
        .push(
            Router::with_path("features").get(get_features).push(Router::with_path("{id}").get(get_features)),
        )
        .push(
            Router::with_path("clients")
                .push(
                    Router::with_path("offlines")
                        .get(search_offlines)
                        .push(Router::new().hoop(require_write).delete(kick_offlines)),
                )
                .get(search_clients)
                .push(
                    Router::with_path("{clientid}")
                        .get(get_client)
                        .push(Router::new().hoop(require_write).delete(kick_client))
                        .push(Router::with_path("online").get(check_online)),
                ),
        )
        .push(
            Router::with_path("subscriptions")
                .get(query_subscriptions)
                .push(Router::with_path("{clientid}").get(get_client_subscriptions)),
        )
        .push(Router::with_path("routes").get(get_routes).push(Router::with_path("{topic}").get(get_route)))
        .push(
            Router::with_path("retains")
                .get(get_retains)
                .push(Router::new().hoop(require_write).delete(delete_retain)),
        )
        .push(
            Router::with_path("mqtt")
                .hoop(require_write)
                .push(Router::with_path("publish").post(publish))
                .push(Router::with_path("subscribe").post(subscribe))
                .push(Router::with_path("unsubscribe").post(unsubscribe)),
        )
        .push(
            Router::with_path("plugins")
                .get(all_plugins)
                .push(Router::with_path("{node}").get(node_plugins))
                .push(Router::with_path("{node}/{plugin}").get(node_plugin_info))
                .push(Router::with_path("{node}/{plugin}/config").get(node_plugin_config))
                .push(
                    Router::with_path("{node}/{plugin}/config/reload")
                        .hoop(require_write)
                        .put(node_plugin_config_reload),
                )
                .push(Router::with_path("{node}/{plugin}/load").hoop(require_write).put(node_plugin_load))
                .push(
                    Router::with_path("{node}/{plugin}/unload").hoop(require_write).put(node_plugin_unload),
                ),
        )
        .push(
            Router::with_path("stats")
                .get(get_stats)
                .push(
                    Router::with_path("sys")
                        .get(get_sys_stats)
                        .push(Router::with_path("sum").get(get_sys_stats_sum))
                        .push(Router::with_path("{id}").get(get_sys_stats)),
                )
                .push(Router::with_path("sum").get(get_stats_sum))
                .push(
                    Router::with_path("history")
                        .get(get_stats_history)
                        .push(Router::with_path("sum").get(get_stats_history_sum))
                        .push(Router::with_path("{id}").get(get_stats_history)),
                )
                .push(Router::with_path("{id}").get(get_stats)),
        )
        .push(
            Router::with_path("metrics")
                .get(get_metrics)
                .push(
                    Router::with_path("prometheus")
                        .get(get_prometheus_metrics)
                        .push(Router::with_path("sum").get(get_prometheus_metrics_sum))
                        .push(Router::with_path("{id}").get(get_prometheus_metrics)),
                )
                .push(Router::with_path("sum").get(get_metrics_sum))
                .push(
                    Router::with_path("history")
                        .get(get_metrics_history)
                        .push(Router::with_path("sum").get(get_metrics_history_sum))
                        .push(Router::with_path("{id}").get(get_metrics_history)),
                )
                .push(Router::with_path("{id}").get(get_metrics)),
        );

    router.push(public).push(protected)
}

pub(crate) async fn listen_and_serve(
    scx: ServerContext,
    laddr: SocketAddr,
    cfg: PluginConfigType,
    history_caches: Option<HistoryCaches>,
    rx: oneshot::Receiver<()>,
    started_tx: oneshot::Sender<()>,
) -> Result<()> {
    let (reuseaddr, reuseport, http_bearer_token, dashboard_static_dir) = {
        let cfg = cfg.read().await;
        (
            cfg.http_reuseaddr,
            cfg.http_reuseport,
            cfg.http_bearer_token.clone(),
            cfg.dashboard_static_dir.clone(),
        )
    };
    log::info!("HTTP API Listening on {laddr}, reuseaddr: {reuseaddr}, reuseport: {reuseport}");

    let listen = tokio::net::TcpListener::from_std(bind(laddr, 128, reuseaddr, reuseport)?)?;

    let acceptor = TcpAcceptor::try_from(listen)?;
    let server = Server::new(acceptor);
    let handler = server.handle();
    tokio::task::spawn(async move {
        rx.await.ok();
        handler.stop_graceful(None);
    });
    let _ = started_tx.send(());
    let monitor = prome::Monitor::new();
    let api_router = route(scx, cfg, http_bearer_token, monitor, history_caches);

    let mut root_router = Router::new().push(api_router);

    // Mount Dashboard SPA — prefer filesystem directory (dev hot-reload) over embedded assets.
    // If dashboard_static_dir is configured AND the directory exists, use StaticDir
    // (supports live editing of dashboard files during development).
    // Otherwise, fall back to assets embedded via rust-embed (production mode, no config needed).
    let dashboard_mounted = if let Some(dir) = &dashboard_static_dir {
        let path = std::path::Path::new(dir);
        if path.exists() {
            root_router = root_router.push(
                Router::with_path("dashboard/{**path}").get(StaticDir::new([dir]).defaults("index.html")),
            );
            root_router = root_router
                .push(Router::with_path("{**path}").get(StaticDir::new([dir]).defaults("index.html")));
            log::info!("Dashboard SPA mounted from filesystem: {dir}, canonical: {:?}", path.canonicalize());
            true
        } else {
            log::warn!(
                "Dashboard static dir configured but not found: {dir}, falling back to embedded assets"
            );
            false
        }
    } else {
        false
    };

    if !dashboard_mounted {
        root_router = root_router.push(
            Router::with_path("dashboard/{*path}")
                .get(static_embed::<DashboardAssets>().fallback("index.html")),
        );
        root_router = root_router
            .push(Router::with_path("{*path}").get(static_embed::<DashboardAssets>().fallback("index.html")));
        log::info!("Dashboard SPA mounted from embedded assets (rust-embed)");
    }

    server.try_serve(root_router).await?;
    Ok(())
}

#[inline]
fn bind(
    laddr: std::net::SocketAddr,
    backlog: i32,
    _reuseaddr: bool,
    _reuseport: bool,
) -> Result<std::net::TcpListener> {
    use socket2::{Domain, SockAddr, Socket, Type};
    let builder = Socket::new(Domain::for_address(laddr), Type::STREAM, None)?;
    builder.set_nonblocking(true)?;
    #[cfg(unix)]
    builder.set_reuse_address(_reuseaddr)?;
    #[cfg(unix)]
    builder.set_reuse_port(_reuseport)?;
    builder.bind(&SockAddr::from(laddr))?;
    builder.listen(backlog)?;
    Ok(std::net::TcpListener::from(builder))
}

#[handler]
async fn request_id_hoop(req: &mut Request, depot: &mut Depot, res: &mut Response, ctrl: &mut FlowCtrl) {
    let id = req.header::<String>("x-request-id").filter(|s| !s.is_empty()).unwrap_or_else(new_request_id);
    depot.insert(DEPOT_REQUEST_ID, id.clone());
    if let Ok(v) = HeaderValue::from_str(&id) {
        res.add_header(HEADER_REQUEST_ID, v, true).ok();
    }
    ctrl.call_next(req, depot, res).await;
}

#[handler]
async fn list_apis(res: &mut Response) {
    let data = serde_json::json!([
        {
            "name": "auth_login",
            "method": "POST",
            "path": "/api/v1/auth/login",
            "descr": "Dashboard login; sets an HttpOnly session cookie"
        },
        {
            "name": "auth_logout",
            "method": "POST",
            "path": "/api/v1/auth/logout",
            "descr": "Clear the dashboard session cookie"
        },
        {
            "name": "auth_me",
            "method": "GET",
            "path": "/api/v1/auth/me",
            "descr": "Current dashboard user (session, bearer, or anonymous)"
        },
        {
            "name": "auth_change_password",
            "method": "POST",
            "path": "/api/v1/auth/change-password",
            "descr": "Change the current dashboard user's password"
        },
        {
            "name": "auth_init",
            "method": "POST",
            "path": "/api/v1/auth/init",
            "descr": "One-time bootstrap of the configured dashboard admin"
        },
        {
            "name": "get_openapi",
            "method": "GET",
            "path": "/api/v1/openapi.json",
            "descr": "OpenAPI 3 document for the /api/v1 management surface"
        },
        {
            "name": "get_docs",
            "method": "GET",
            "path": "/api/v1/docs",
            "descr": "Swagger UI for the OpenAPI document"
        },
        {
            "name": "get_brokers",
            "method": "GET",
            "path": "/api/v1/brokers/{node}",
            "descr": "Return the basic information of all nodes in the cluster"
        },
        {
            "name": "get_nodes",
            "method": "GET",
            "path": "/api/v1/nodes/{node}",
            "descr": "Returns the status of the node"
        },
        {
            "name": "get_features",
            "method": "GET",
            "path": "/api/v1/features[/{node}]",
            "descr": "Returns the supported feature state (retain/message_storage/session_storage/delayed/shared_subscription/auto_subscription) of cluster nodes"
        },
        {
            "name": "check_health",
            "method": "GET",
            "path": "/api/v1/health/check/{node}",
            "descr": "Node health check"
        },
        {
            "name": "search_clients",
            "method": "GET",
            "path": "/api/v1/clients/",
            "descr": "Search clients information from the cluster"
        },
        {
            "name": "get_client",
            "method": "GET",
            "path": "/api/v1/clients/{clientid}",
            "descr": "Get client information from the cluster"
        },
        {
            "name": "kick_client",
            "method": "DELETE",
            "path": "/api/v1/clients/{clientid}",
            "descr": "Kick client from the cluster"
        },
        {
            "name": "check_online",
            "method": "GET",
            "path": "/api/v1/clients/{clientid}/online",
            "descr": "Check a client whether online from the cluster"
        },
        {
            "name": "search_offlines",
            "method": "GET",
            "path": "/api/v1/clients/offlines",
            "descr": "Search offlines clients information from the cluster"
        },
        {
            "name": "kick_offlines",
            "method": "DELETE",
            "path": "/api/v1/clients/offlines",
            "descr": "Kick offlines clients from the cluster"
        },
        {
            "name": "query_subscriptions",
            "method": "GET",
            "path": "/api/v1/subscriptions",
            "descr": "Query subscriptions information from the cluster"
        },
        {
            "name": "get_client_subscriptions",
            "method": "GET",
            "path": "/api/v1/subscriptions/{clientid}",
            "descr": "Get subscriptions information for the client from the cluster"
        },

        {
            "name": "get_routes",
            "method": "GET",
            "path": "/api/v1/routes",
            "descr": "Return all routing information from the cluster"
        },
        {
            "name": "get_route",
            "method": "GET",
            "path": "/api/v1/routes/{topic}",
            "descr": "Get routing information from the cluster"
        },
        {
            "name": "get_retains",
            "method": "GET",
            "path": "/api/v1/retains",
            "descr": "Query retained messages with optional topic_filter/offset/limit"
        },
        {
            "name": "delete_retain",
            "method": "DELETE",
            "path": "/api/v1/retains?topic={topic}",
            "descr": "Delete a retained message by exact topic (cluster-wide)"
        },

        {
            "name": "publish",
            "method": "POST",
            "path": "/api/v1/mqtt/publish",
            "descr": "Publish MQTT message"
        },
        {
            "name": "subscribe",
            "method": "POST",
            "path": "/api/v1/mqtt/subscribe",
            "descr": "Subscribe to MQTT topic"
        },
        {
            "name": "unsubscribe",
            "method": "POST",
            "path": "/api/v1/mqtt/unsubscribe",
            "descr": "Unsubscribe"
        },

        {
            "name": "all_plugins",
            "method": "GET",
            "path": "/api/v1/plugins/",
            "descr": "Returns information of all plugins in the cluster"
        },
        {
            "name": "node_plugins",
            "method": "GET",
            "path": "/api/v1/plugins/{node}",
            "descr": "Similar with GET /api/v1/plugins, return the plugin information under the specified node"
        },
        {
            "name": "node_plugin_info",
            "method": "GET",
            "path": "/api/v1/plugins/{node}/{plugin}",
            "descr": "Get a plugin info"
        },
        {
            "name": "node_plugin_config",
            "method": "GET",
            "path": "/api/v1/plugins/{node}/{plugin}/config",
            "descr": "Get a plugin config"
        },
        {
            "name": "node_plugin_config_reload",
            "method": "PUT",
            "path": "/api/v1/plugins/{node}/{plugin}/config/reload",
            "descr": "Reload a plugin config"
        },
        {
            "name": "node_plugin_load",
            "method": "PUT",
            "path": "/api/v1/plugins/{node}/{plugin}/load",
            "descr": "Load the specified plugin under the specified node."
        },
        {
            "name": "node_plugin_unload",
            "method": "PUT",
            "path": "/api/v1/plugins/{node}/{plugin}/unload",
            "descr": "Unload the specified plugin under the specified node."
        },

        {
            "name": "get_stats",
            "method": "GET",
            "path": "/api/v1/stats/{node}",
            "descr": "Returns all statistics information from the cluster"
        },
        {
            "name": "get_stats_sum",
            "method": "GET",
            "path": "/api/v1/stats/sum",
            "descr": "Summarize all statistics information from the cluster"
        },
        {
            "name": "get_sys_stats",
            "method": "GET",
            "path": "/api/v1/stats/sys/{node}",
            "descr": "Returns all system statistics information from the cluster"
        },
        {
            "name": "get_sys_stats_sum",
            "method": "GET",
            "path": "/api/v1/stats/sys/sum",
            "descr": "Summarize all system statistics information from the cluster"
        },
        {
            "name": "get_metrics",
            "method": "GET",
            "path": "/api/v1/metrics/{node}",
            "descr": "Returns all metrics information from the cluster"
        },
        {
            "name": "get_metrics_sum",
            "method": "GET",
            "path": "/api/v1/metrics/sum",
            "descr": "Summarize all metrics information from the cluster"
        },

        {
          "name": "get_prometheus_metrics",
          "method": "GET",
          "path": "/api/v1/metrics/prometheus",
          "descr": "Get prometheus metrics from the cluster"
        },
        {
          "name": "get_stats_history",
          "method": "GET",
          "path": "/api/v1/stats/history[/{id}]",
          "descr": "Get historical stats (all nodes or a specific node)"
        },
        {
          "name": "get_stats_history_sum",
          "method": "GET",
          "path": "/api/v1/stats/history/sum",
          "descr": "Get aggregated historical stats across all nodes"
        },
        {
          "name": "get_metrics_history",
          "method": "GET",
          "path": "/api/v1/metrics/history[/{id}]",
          "descr": "Get historical metrics (all nodes or a specific node)"
        },
        {
          "name": "get_metrics_history_sum",
          "method": "GET",
          "path": "/api/v1/metrics/history/sum",
          "descr": "Get aggregated historical metrics across all nodes"
        },


    ]);
    res.render(Json(data));
}

fn get_scx_cfg(depot: &mut Depot) -> std::result::Result<&(ServerContext, PluginConfigType), salvo::Error> {
    let scx_cfg = depot.obtain::<(ServerContext, PluginConfigType)>().map_err(|e| match e {
        None => salvo::Error::Io(std::io::Error::new(ErrorKind::NotFound, anyhow!("None"))),
        Some(e) => salvo::Error::Io(std::io::Error::new(ErrorKind::NotFound, format!("{e:?}"))),
    })?;
    Ok(scx_cfg)
}

fn get_monitor(depot: &Depot) -> std::result::Result<Monitor, salvo::Error> {
    let m = depot.get::<Monitor>(PROME_MONITOR).cloned().map_err(|e| match e {
        None => salvo::Error::Io(std::io::Error::new(ErrorKind::NotFound, anyhow!("None"))),
        Some(e) => salvo::Error::Io(std::io::Error::new(ErrorKind::NotFound, format!("{e:?}"))),
    })?;
    Ok(m)
}

/// Returns the history caches (LRU + storage) from the depot, or `None`
/// if history is not configured.
fn get_history_caches(depot: &Depot) -> Option<HistoryCaches> {
    match depot.get::<HistoryCaches>(HISTORY_CACHES) {
        Ok(hc) => Some(hc.clone()),
        _ => None,
    }
}

#[handler]
async fn api_logger(req: &mut Request, depot: &mut Depot) -> std::result::Result<(), salvo::Error> {
    let (_, cfg) = get_scx_cfg(depot)?;
    if !cfg.read().await.http_request_log {
        return Ok(());
    }
    let log_data =
        format!("Request {}, {:?}, {}, {}", req.remote_addr(), req.version(), req.method(), req.uri());
    let txt_body = if let Some(m) = req.content_type() {
        if let mime::PLAIN | mime::JSON | mime::TEXT = m.subtype() {
            if let Ok(body) = req.payload().await {
                Some(String::from_utf8_lossy(body))
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };
    if let Some(txt_body) = txt_body {
        log::info!("{log_data}, body: {txt_body}");
    } else {
        log::info!("{log_data}");
    }
    Ok(())
}

#[handler]
async fn get_brokers(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;

    let id = req.param::<NodeId>("id");
    if let Some(id) = id {
        match _get_broker(scx, message_type, id).await {
            Ok(Some(broker_info)) => res.render(Json(broker_info)),
            Ok(None) => {
                //| Err(MqttError::None)
                render_not_found(res, "not found");
            }
            Err(e) => {
                render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string());
            }
        }
    } else {
        match _get_brokers(scx, message_type).await {
            Ok(brokers) => res.render(Json(brokers)),
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    }
    Ok(())
}

#[inline]
async fn _get_broker(
    scx: &ServerContext,
    message_type: MessageType,
    id: NodeId,
) -> Result<Option<serde_json::Value>> {
    if id == scx.node.id() {
        Ok(Some(scx.node.broker_info(scx).await.to_json()))
    } else {
        let grpc_clients = scx.extends.shared().await.get_grpc_clients();
        if let Some((_, c)) = grpc_clients.get(&id) {
            let msg = Message::BrokerInfo.encode()?;
            let reply = MessageSender::new_quick(
                c.clone(),
                message_type,
                GrpcMessage::Data(msg),
                Some(Duration::from_secs(10)),
            )
            .send()
            .await;
            match reply {
                Ok(GrpcMessageReply::Data(msg)) => match MessageReply::decode(&msg)? {
                    MessageReply::BrokerInfo(broker_info) => {
                        Ok(Some(cluster_node_success(broker_info.to_json())))
                    }
                    _ => {
                        log::error!("unreachable!(), msg: {msg:?}");
                        Err(anyhow!("unreachable!()"))
                    }
                },
                Ok(reply) => {
                    log::info!("Get GrpcMessage::BrokerInfo from other node({id}), reply: {reply:?}");
                    Err(anyhow!("Invalid Result"))
                }
                Err(e) => {
                    log::warn!("Get GrpcMessage::BrokerInfo from other node, error: {e}");
                    Err(e)
                }
            }
        } else {
            Ok(None)
        }
    }
}

#[inline]
async fn _get_brokers(scx: &ServerContext, message_type: MessageType) -> Result<Vec<serde_json::Value>> {
    let mut brokers = vec![cluster_node_success(scx.node.broker_info(scx).await.to_json())];
    let grpc_clients = scx.extends.shared().await.get_grpc_clients();
    if !grpc_clients.is_empty() {
        let msg = Message::BrokerInfo.encode()?;
        let replys = MessageBroadcaster::new_quick(
            grpc_clients,
            message_type,
            GrpcMessage::Data(msg),
            Some(Duration::from_secs(10)),
        )
        .join_all()
        .await
        .drain(..)
        .map(|reply| match reply {
            (_, Ok(GrpcMessageReply::Data(msg))) => match MessageReply::decode(&msg) {
                Ok(MessageReply::BrokerInfo(broker_info)) => Ok(cluster_node_success(broker_info.to_json())),
                Err(e) => Err(e),
                _ => {
                    log::error!("unreachable!(), msg: {msg:?}");
                    Err(anyhow!("unreachable!()"))
                }
            },
            (id, Ok(reply)) => {
                log::info!("Get GrpcMessage::BrokerInfo from other node({id}), reply: {reply:?}");
                Ok(cluster_node_failure(id, "Invalid Result"))
            }
            (id, Err(e)) => {
                log::warn!("Get GrpcMessage::BrokerInfo from other node({id}), error: {e}");
                Ok(cluster_node_failure(id, e))
            }
        })
        .collect::<Result<Vec<_>>>()?;
        brokers.extend(replys);
    }
    Ok(brokers)
}

#[handler]
async fn get_nodes(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;

    let id = req.param::<NodeId>("id");
    if let Some(id) = id {
        match get_node(scx, message_type, id).await {
            Ok(Some(node_info)) => res.render(Json(node_info.to_json())),
            Ok(None) => {
                render_not_found(res, "not found");
            }
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    } else {
        match get_nodes_all(scx, message_type).await {
            Ok(node_infos) => {
                let mut nodes = Vec::new();
                for item in node_infos {
                    match item {
                        Ok(node_info) => {
                            nodes.push(cluster_node_success(node_info.to_json()));
                        }
                        Err((id, e)) => {
                            nodes.push(cluster_node_failure(id, e));
                        }
                    }
                }
                res.render(Json(nodes))
            }
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    }
    Ok(())
}

#[inline]
async fn _get_nodes(scx: &ServerContext, message_type: MessageType) -> Result<Vec<serde_json::Value>> {
    let mut nodes = vec![scx.node.node_info(scx).await.to_json()];
    let grpc_clients = scx.extends.shared().await.get_grpc_clients();
    if !grpc_clients.is_empty() {
        let msg = Message::NodeInfo.encode()?;
        let replys = MessageBroadcaster::new_quick(
            grpc_clients,
            message_type,
            GrpcMessage::Data(msg),
            Some(Duration::from_secs(10)),
        )
        .join_all()
        .await
        .drain(..)
        .map(|reply| match reply {
            (_, Ok(GrpcMessageReply::Data(msg))) => match MessageReply::decode(&msg) {
                Ok(MessageReply::NodeInfo(node_info)) => Ok(node_info.to_json()),
                Err(e) => Err(e),
                _ => {
                    log::error!("unreachable!(), msg: {msg:?}");
                    Err(anyhow!("unreachable!()"))
                }
            },
            (id, Ok(reply)) => {
                log::info!("Get GrpcMessage::NodeInfo from other node({id}), reply: {reply:?}");
                Err(anyhow!("Invalid Result"))
            }
            (id, Err(e)) => {
                log::warn!("Get GrpcMessage::NodeInfo from other node({id}), error: {e}");
                Ok(cluster_node_failure(id, e))
            }
        })
        .collect::<Result<Vec<_>>>()?;
        nodes.extend(replys);
    }
    Ok(nodes)
}

#[inline]
pub(crate) async fn get_node(
    scx: &ServerContext,
    message_type: MessageType,
    id: NodeId,
) -> Result<Option<NodeInfo>> {
    if id == scx.node.id() {
        Ok(Some(scx.node.node_info(scx).await))
    } else {
        let grpc_clients = scx.extends.shared().await.get_grpc_clients();
        if let Some((_, c)) = grpc_clients.get(&id) {
            let msg = Message::NodeInfo.encode()?;
            let reply = MessageSender::new_quick(
                c.clone(),
                message_type,
                GrpcMessage::Data(msg),
                Some(Duration::from_secs(10)),
            )
            .send()
            .await;
            match reply {
                Ok(GrpcMessageReply::Data(msg)) => match MessageReply::decode(&msg)? {
                    MessageReply::NodeInfo(node_info) => Ok(Some(node_info)),
                    _ => {
                        log::error!("unreachable!(), msg: {msg:?}");
                        Err(anyhow!("unreachable!()"))
                    }
                },
                Ok(reply) => {
                    log::info!("Get GrpcMessage::NodeInfo from other node({id}), reply: {reply:?}");
                    Err(anyhow!("Invalid Result"))
                }
                Err(e) => {
                    log::warn!("Get GrpcMessage::NodeInfo from other node, error: {e}");
                    Err(e)
                }
            }
        } else {
            Ok(None)
        }
    }
}

#[inline]
pub(crate) async fn get_nodes_all(
    scx: &ServerContext,
    message_type: MessageType,
) -> Result<Vec<std::result::Result<NodeInfo, (NodeId, anyhow::Error)>>> {
    let mut nodes = vec![Ok(scx.node.node_info(scx).await)];
    let grpc_clients = scx.extends.shared().await.get_grpc_clients();
    if !grpc_clients.is_empty() {
        let msg = Message::NodeInfo.encode()?;
        let replys = MessageBroadcaster::new_quick(
            grpc_clients,
            message_type,
            GrpcMessage::Data(msg),
            Some(Duration::from_secs(10)),
        )
        .join_all()
        .await
        .drain(..)
        .map(|reply| match reply {
            (_, Ok(GrpcMessageReply::Data(msg))) => match MessageReply::decode(&msg) {
                Ok(MessageReply::NodeInfo(node_info)) => Ok(Ok(node_info)),
                Err(e) => Err(e),
                _ => {
                    log::error!("unreachable!(), msg: {msg:?}");
                    Err(anyhow!("unreachable!()"))
                }
            },
            (id, Ok(reply)) => {
                log::info!("Get GrpcMessage::NodeInfo from other node({id}), reply: {reply:?}");
                Ok(Err((id, anyhow!("Invalid Result"))))
            }
            (id, Err(e)) => {
                log::warn!("Get GrpcMessage::NodeInfo from other node({id}), error: {e}");
                Ok(Err((id, e)))
            }
        })
        .collect::<Result<Vec<_>>>()?;
        nodes.extend(replys);
    }
    Ok(nodes)
}

/// Build the feature support state of the current node.
#[inline]
pub(crate) async fn build_features(scx: &ServerContext) -> FeaturesInfo {
    let extends = &scx.extends;
    FeaturesInfo {
        node_id: scx.node.id(),
        node_name: scx.node.name(scx, scx.node.id()).await,
        features: Features {
            retain: extends.retain().await.enable(),
            message_storage: extends.message_mgr().await.enable(),
            session_storage: extends.session_mgr().await.enable(),
            delayed: extends.delayed_sender().await.enable(),
            shared_subscription: extends.shared_subscription().await.is_supported(),
            auto_subscription: extends.auto_subscription().await.enable(),
        },
    }
}

/// Query the feature support state of a single node (local or remote).
#[inline]
async fn get_feature(
    scx: &ServerContext,
    message_type: MessageType,
    id: NodeId,
) -> Result<Option<FeaturesInfo>> {
    if id == scx.node.id() {
        Ok(Some(build_features(scx).await))
    } else {
        let grpc_clients = scx.extends.shared().await.get_grpc_clients();
        if let Some((_, c)) = grpc_clients.get(&id) {
            let msg = Message::Features.encode()?;
            let reply = MessageSender::new_quick(
                c.clone(),
                message_type,
                GrpcMessage::Data(msg),
                Some(Duration::from_secs(10)),
            )
            .send()
            .await;
            match reply {
                Ok(GrpcMessageReply::Data(msg)) => match MessageReply::decode(&msg)? {
                    MessageReply::Features(features_info) => Ok(Some(features_info)),
                    _ => {
                        log::error!("unreachable!(), msg: {msg:?}");
                        Err(anyhow!("unreachable!()"))
                    }
                },
                Ok(reply) => {
                    log::info!("Get GrpcMessage::Features from other node({id}), reply: {reply:?}");
                    Err(anyhow!("Invalid Result"))
                }
                Err(e) => {
                    log::warn!("Get GrpcMessage::Features from other node, error: {e}");
                    Err(e)
                }
            }
        } else {
            Ok(None)
        }
    }
}

/// Query the feature support state of all cluster nodes.
#[inline]
async fn get_features_all(scx: &ServerContext, message_type: MessageType) -> Result<Vec<FeaturesNodeResult>> {
    let mut features = vec![FeaturesNodeResult::ok(build_features(scx).await)];
    let grpc_clients = scx.extends.shared().await.get_grpc_clients();
    if !grpc_clients.is_empty() {
        let msg = Message::Features.encode()?;
        let replys = MessageBroadcaster::new_quick(
            grpc_clients,
            message_type,
            GrpcMessage::Data(msg),
            Some(Duration::from_secs(10)),
        )
        .join_all()
        .await
        .drain(..)
        .map(|reply| match reply {
            (_, Ok(GrpcMessageReply::Data(msg))) => match MessageReply::decode(&msg) {
                Ok(MessageReply::Features(features_info)) => Ok(FeaturesNodeResult::ok(features_info)),
                Err(e) => Err(e),
                _ => {
                    log::error!("unreachable!(), msg: {msg:?}");
                    Err(anyhow!("unreachable!()"))
                }
            },
            (id, Ok(reply)) => {
                log::info!("Get GrpcMessage::Features from other node({id}), reply: {reply:?}");
                Ok(FeaturesNodeResult::err(id, "Invalid Result"))
            }
            (id, Err(e)) => {
                log::warn!("Get GrpcMessage::Features from other node({id}), error: {e}");
                Ok(FeaturesNodeResult::err(id, e.to_string()))
            }
        })
        .collect::<Result<Vec<_>>>()?;
        features.extend(replys);
    }
    Ok(features)
}

/// A feature field getter: `(JSON key, function extracting the bool flag)`.
type FeatureGetter = (&'static str, fn(&Features) -> bool);

/// Compare feature flags across the successfully-reached nodes and report
/// which fields differ. Fields are compared per node; a field is conflicting
/// when some nodes report `true` while others report `false`.
#[inline]
fn summarize_features(successes: &[FeaturesInfo]) -> (bool, Vec<FeatureConflict>) {
    let feature_getters: [FeatureGetter; 6] = [
        ("retain", |f| f.retain),
        ("message_storage", |f| f.message_storage),
        ("session_storage", |f| f.session_storage),
        ("delayed", |f| f.delayed),
        ("shared_subscription", |f| f.shared_subscription),
        ("auto_subscription", |f| f.auto_subscription),
    ];

    let mut conflicts = Vec::new();
    for (name, getter) in feature_getters {
        let mut true_nodes = Vec::new();
        let mut false_nodes = Vec::new();
        for info in successes {
            if getter(&info.features) {
                true_nodes.push(info.node_id);
            } else {
                false_nodes.push(info.node_id);
            }
        }
        if !true_nodes.is_empty() && !false_nodes.is_empty() {
            conflicts.push(FeatureConflict {
                feature: name.to_string(),
                values: vec![
                    FeatureValueGroup { value: true, node_ids: true_nodes },
                    FeatureValueGroup { value: false, node_ids: false_nodes },
                ],
            });
        }
    }
    (conflicts.is_empty(), conflicts)
}

/// OR-aggregate feature flags across reachable nodes (dashboard menu gating).
#[inline]
fn aggregate_enabled(successes: &[FeaturesInfo]) -> Features {
    let mut enabled = Features::default();
    for info in successes {
        enabled.retain |= info.features.retain;
        enabled.message_storage |= info.features.message_storage;
        enabled.session_storage |= info.features.session_storage;
        enabled.delayed |= info.features.delayed;
        enabled.shared_subscription |= info.features.shared_subscription;
        enabled.auto_subscription |= info.features.auto_subscription;
    }
    enabled
}

/// Query which broker features are supported.
///
/// `GET /api/v1/features` returns the feature support state of every cluster
/// node plus a cluster-wide consistency summary (`consistent` / `conflicts`);
/// `GET /api/v1/features/{node}` targets a single node.
#[handler]
async fn get_features(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;

    let id = req.param::<NodeId>("id");
    if let Some(id) = id {
        match get_feature(scx, message_type, id).await {
            Ok(Some(features_info)) => res.render(Json(features_info)),
            Ok(None) => {
                render_not_found(res, "not found");
            }
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    } else {
        match get_features_all(scx, message_type).await {
            Ok(nodes) => {
                let mut successes: Vec<FeaturesInfo> = Vec::new();
                let mut failed_count = 0usize;
                for item in &nodes {
                    if item.ok {
                        if let Some(features) = item.features.clone() {
                            successes.push(FeaturesInfo {
                                node_id: item.node_id,
                                node_name: item.node_name.clone().unwrap_or_default(),
                                features,
                            });
                        }
                    } else {
                        failed_count += 1;
                    }
                }
                let (consistent, conflicts) = summarize_features(&successes);
                if !consistent {
                    log::warn!(
                        "features inconsistent across cluster (node_count: {}): {:?}",
                        successes.len(),
                        conflicts
                    );
                }
                res.render(Json(FeaturesSummary {
                    consistent,
                    node_count: successes.len(),
                    failed_count,
                    partial: failed_count > 0,
                    enabled: aggregate_enabled(&successes),
                    conflicts,
                    nodes,
                }))
            }
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    }
    Ok(())
}

#[handler]
async fn check_health(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;
    let id = req.param::<NodeId>("id");
    if let Some(id) = id {
        match check_health_one(scx, message_type, id).await {
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
            Ok(None) => render_not_found(res, "not found"),
            Ok(Some(health_status)) => {
                if health_status.is_running() {
                    res.render(Json(health_status.to_json()))
                } else {
                    log::info!("{health_status:?}");
                    res.status_code(StatusCode::SERVICE_UNAVAILABLE);
                    res.render(Json(health_status.to_json()))
                }
            }
        }
    } else {
        match scx.extends.shared().await.check_health().await {
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
            Ok(health_info) => res.render(Json(health_info.to_json())),
        }
    }
    Ok(())
}

async fn check_health_one(
    scx: &ServerContext,
    message_type: MessageType,
    id: NodeId,
) -> Result<Option<NodeHealthStatus>> {
    if id == scx.node.id() {
        Ok(Some(scx.extends.shared().await.health_status().await?))
    } else {
        let grpc_clients = scx.extends.shared().await.get_grpc_clients();
        if let Some((_, c)) = grpc_clients.get(&id) {
            let msg = Message::NodeHealthStatus.encode()?;
            let reply = MessageSender::new_quick(
                c.clone(),
                message_type,
                GrpcMessage::Data(msg),
                Some(Duration::from_secs(10)),
            )
            .send()
            .await;
            match reply {
                Ok(GrpcMessageReply::Data(msg)) => match MessageReply::decode(&msg)? {
                    MessageReply::NodeHealthStatus(health_status) => Ok(Some(health_status)),
                    _ => {
                        log::error!("unreachable!(), msg: {msg:?}");
                        Err(anyhow!("unreachable!()"))
                    }
                },
                Ok(reply) => {
                    log::info!("Get GrpcMessage::NodeHealthStatus from other node({id}), reply: {reply:?}");
                    Err(anyhow!("Invalid Result"))
                }
                Err(e) => {
                    log::warn!("Get GrpcMessage::NodeHealthStatus from other node, error: {e}");
                    Err(e)
                }
            }
        } else {
            Ok(None)
        }
    }
}

#[handler]
async fn get_client(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;
    let clientid = req.param::<String>("clientid");
    if let Some(clientid) = clientid {
        match _get_client(scx, message_type, &clientid).await {
            Ok(Some(reply)) => res.render(Json(reply)),
            Ok(None) => {
                //| Err(MqttError::None)
                render_not_found(res, "not found");
            }
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    } else {
        render_api_error(res, StatusCode::BAD_REQUEST, "bad request")
    }
    Ok(())
}

async fn _get_client(
    scx: &ServerContext,
    message_type: MessageType,
    clientid: &str,
) -> Result<Option<serde_json::Value>> {
    let reply = clients::get(scx, clientid).await;
    if let Some(reply) = reply {
        return Ok(Some(reply.to_json()));
    }

    let check_result = |reply: GrpcMessageReply| match reply {
        GrpcMessageReply::Data(res) => match MessageReply::decode(&res) {
            Ok(MessageReply::ClientGet(ress)) => match ress {
                Some(res) => Ok(res),
                None => Err(anyhow!(MqttError::None)),
            },
            Err(e) => Err(e),
            _ => {
                log::error!("unreachable!(), res: {res:?}");
                Err(anyhow!("unreachable!()"))
            }
        },
        reply => {
            log::info!("Subscribe GrpcMessage::ClientGet from other node, reply: {reply:?}");
            Err(anyhow!("Invalid Result"))
        }
    };

    let grpc_clients = scx.extends.shared().await.get_grpc_clients();
    if !grpc_clients.is_empty() {
        let q = Message::ClientGet { clientid }.encode()?;
        let reply = MessageBroadcaster::new_quick(
            grpc_clients,
            message_type,
            GrpcMessage::Data(q),
            Some(Duration::from_secs(10)),
        )
        .select_ok(check_result)
        .await?;
        return Ok(Some(reply.to_json()));
    }

    Ok(None)
}

#[handler]
async fn search_clients(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;
    let max_row_limit = cfg.read().await.max_row_limit;
    let mut q = match req.parse_queries::<ClientSearchParams>() {
        Ok(q) => q,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };

    let paging = ListPaging::from_request(req, q._limit, max_row_limit);
    q._limit = paging.fetch_limit;
    match _search_clients(scx, message_type, q).await {
        Ok(replys) => {
            let (page, truncated) = paging.apply(replys);
            let replys = page.iter().map(|res| res.to_json()).collect::<Vec<_>>();
            render_list(req, res, replys, paging, truncated, None);
        }
        Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
    }
    Ok(())
}

#[handler]
async fn search_offlines(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;
    let max_row_limit = cfg.read().await.max_row_limit;
    let mut q = match req.parse_queries::<ClientSearchParams>() {
        Ok(q) => q,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    q.connected = Some(false);

    let paging = ListPaging::from_request(req, q._limit, max_row_limit);
    q._limit = paging.fetch_limit;
    match _search_clients(scx, message_type, q).await {
        Ok(replys) => {
            let (page, truncated) = paging.apply(replys);
            let replys = page.iter().map(|res| res.to_json()).collect::<Vec<_>>();
            render_list(req, res, replys, paging, truncated, None);
        }
        Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
    }
    Ok(())
}

async fn _search_clients(
    scx: &ServerContext,
    message_type: MessageType,
    mut q: ClientSearchParams,
) -> Result<Vec<ClientSearchResult>> {
    let mut replys = clients::search(scx, &q).await;
    let grpc_clients = scx.extends.shared().await.get_grpc_clients();
    for (id, (_addr, c)) in grpc_clients.iter() {
        if replys.len() < q._limit {
            q._limit -= replys.len();

            let q = Message::ClientSearch(Box::new(q.clone())).encode()?;
            let reply = MessageSender::new_quick(
                c.clone(),
                message_type,
                GrpcMessage::Data(q),
                Some(Duration::from_secs(10)),
            )
            .send()
            .await;
            match reply {
                Ok(GrpcMessageReply::Data(res)) => match MessageReply::decode(&res)? {
                    MessageReply::ClientSearch(ress) => {
                        replys.extend(ress);
                    }
                    _ => {
                        log::error!("unreachable!(), res: {res:?}");
                    }
                },
                Err(e) => {
                    log::warn!("Get GrpcMessage::ClientSearch, error: {e}");
                }
                Ok(reply) => {
                    log::warn!("Get GrpcMessage::ClientSearch from other node({id}), reply: {reply:?}");
                }
            };
        } else {
            break;
        }
    }

    Ok(replys)
}

#[handler]
async fn kick_client(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, _) = get_scx_cfg(depot)?;
    let clientid = req.param::<String>("clientid");
    if let Some(clientid) = clientid {
        let mut entry = scx.extends.shared().await.entry(Id::from(scx.node.id(), ClientId::from(clientid)));
        let s = entry.session();
        if let Some(s) = s {
            match entry.kick(true, true, true).await {
                Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
                Ok(_) => res.render(Json(s.id.to_json())),
            }
        } else {
            render_not_found(res, "not found");
        }
    } else {
        render_api_error(res, StatusCode::BAD_REQUEST, "bad request")
    }
    Ok(())
}

#[handler]
async fn kick_offlines(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;
    let max_row_limit = cfg.read().await.max_row_limit;
    let mut q = match req.parse_queries::<ClientSearchParams>() {
        Ok(q) => q,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    q.connected = Some(false);

    if q._limit == 0 || q._limit > max_row_limit {
        q._limit = max_row_limit;
    }

    let mut count = 0;
    match _search_clients(scx, message_type, q).await {
        Ok(replys) => {
            for reply in replys.iter() {
                log::debug!("node_id: {}, clientid: {}", reply.node_id, reply.clientid);
                let mut entry = scx
                    .extends
                    .shared()
                    .await
                    .entry(Id::from(reply.node_id, ClientId::from(reply.clientid.clone())));
                let s = entry.session();
                if s.is_some() {
                    match entry.kick(true, true, true).await {
                        Err(e) => {
                            log::warn!("{e}");
                        }
                        Ok(_) => {
                            count += 1;
                        }
                    }
                } else {
                    log::warn!(
                        "session is not found, node_id: {}, clientid: {}",
                        reply.node_id,
                        reply.clientid
                    );
                }
            }
        }
        Err(e) => {
            log::warn!("{e}");
        }
    }
    res.render(Json(json!({"count": count})));
    Ok(())
}

#[handler]
async fn check_online(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, _) = get_scx_cfg(depot)?;
    let clientid = req.param::<String>("clientid");
    if let Some(clientid) = clientid {
        let entry = scx.extends.shared().await.entry(Id::from(scx.node.id(), ClientId::from(clientid)));

        let online = entry.online().await;
        res.render(Json(online));
    } else {
        render_api_error(res, StatusCode::BAD_REQUEST, "bad request")
    }
    Ok(())
}

#[handler]
async fn query_subscriptions(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let max_row_limit = cfg.read().await.max_row_limit;
    let mut q = match req.parse_queries::<SubsSearchParams>() {
        Ok(q) => q,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let paging = ListPaging::from_request(req, q._limit, max_row_limit);
    q._limit = paging.fetch_limit;
    let replys = scx
        .extends
        .shared()
        .await
        .query_subscriptions(&q)
        .await
        .into_iter()
        .map(|res| subscription_to_json(res.to_json()))
        .collect::<Vec<serde_json::Value>>();
    let (page, truncated) = paging.apply(replys);
    render_list(req, res, page, paging, truncated, None);
    Ok(())
}

#[handler]
async fn get_client_subscriptions(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, _) = get_scx_cfg(depot)?;
    let clientid = req.param::<String>("clientid");
    if let Some(clientid) = clientid {
        let entry = scx.extends.shared().await.entry(Id::from(scx.node.id(), ClientId::from(clientid)));
        if let Some(subs) = entry.subscriptions().await {
            let subs = subs
                .into_iter()
                .map(|res| subscription_to_json(res.to_json()))
                .collect::<Vec<serde_json::Value>>();
            let paging = ListPaging::new(0, subs.len(), subs.len().max(1));
            render_list(req, res, subs, paging, false, None);
        } else {
            render_not_found(res, "client not found");
        }
    } else {
        render_api_error(res, StatusCode::BAD_REQUEST, "bad request");
    }
    Ok(())
}

#[handler]
async fn get_routes(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let max_row_limit = cfg.read().await.max_row_limit;
    let requested = req.query::<usize>("_limit").or_else(|| req.query::<usize>("limit")).unwrap_or(0);
    let paging = ListPaging::from_request(req, requested, max_row_limit);
    let replys = scx.extends.router().await.gets(paging.fetch_limit).await;
    let (page, truncated) = paging.apply(replys);
    render_list(req, res, page, paging, truncated, None);
    Ok(())
}

#[handler]
async fn get_route(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, _) = get_scx_cfg(depot)?;
    let topic = req.param::<String>("topic");
    if let Some(topic) = topic {
        match scx.extends.router().await.get(&topic).await {
            Ok(replys) => {
                let paging = ListPaging::new(0, replys.len(), replys.len().max(1));
                render_list(req, res, replys, paging, false, None);
            }
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    } else {
        render_api_error(res, StatusCode::BAD_REQUEST, "bad request")
    }
    Ok(())
}

/// Query retained messages with an optional topic filter and pagination.
///
/// Query parameters:
/// - `topic_filter`: topic filter supporting `#` / `+` wildcards (default `#`).
/// - `offset`: pagination offset (default `0`).
/// - `limit`: page size (default and cap: `max_row_limit`).
///
/// Response: `{ "items": [RetainInfo...], "has_more": bool }`.
///
/// Cluster semantics: retained messages are broadcast-synced to every node,
/// so a single-node query already covers the whole cluster. Storage backends
/// whose `merge_on_read()` returns `true` (future shared-backend case) will
/// return merged data automatically through `RetainStorage::get`.
#[handler]
async fn get_retains(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let max_row_limit = cfg.read().await.max_row_limit;
    let mut q = match req.parse_queries::<RetainQueryParams>() {
        Ok(q) => q,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    if q.limit == 0 || q.limit > max_row_limit {
        q.limit = max_row_limit;
    }

    let retain_mgr = scx.extends.retain().await;
    let topic_filter_all = q.topic_filter.is_empty() || q.topic_filter == "#";
    let (items, has_more) = if topic_filter_all {
        // Full-range path: storage-level pagination with remaining TTL.
        match retain_mgr.get_all_paginated(q.offset, q.limit).await {
            Ok((list, has_more)) => (
                list.into_iter().map(|(t, r, ttl)| RetainInfo::from_paginated(t, r, ttl)).collect::<Vec<_>>(),
                has_more,
            ),
            Err(e) => {
                render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string());
                return Ok(());
            }
        }
    } else {
        // Topic-filtered path: fetch all matches, paginate in memory.
        match retain_mgr.get(&q.topic_filter).await {
            Ok(all) => {
                let total = all.len();
                let has_more = q.offset + q.limit < total;
                let items = all
                    .into_iter()
                    .skip(q.offset)
                    .take(q.limit)
                    .map(|(t, r)| RetainInfo::from_get(t, r))
                    .collect::<Vec<_>>();
                (items, has_more)
            }
            Err(e) => {
                render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string());
                return Ok(());
            }
        }
    };

    apply_list_headers(res, items.len(), has_more);
    if wants_page_format(req) {
        res.render(Json(json!({
            "items": items,
            "has_more": has_more,
            "offset": q.offset,
            "limit": q.limit,
            "truncated": has_more,
        })));
    } else {
        res.render(Json(json!({"items": items, "has_more": has_more})));
    }
    Ok(())
}

/// Delete a retained message by exact topic.
///
/// Query parameters:
/// - `topic`: concrete topic name (wildcards `#` / `+` are NOT allowed).
///
/// Deletion follows the MQTT convention: publishing an empty-payload retained
/// message on the topic clears it from storage via `RetainStorage::set`.
/// The deletion is then propagated to all cluster peers through
/// `retain_set_broadcast`, so every node removes its local copy.
///
/// Responses:
/// - `200`: deleted successfully.
/// - `400`: missing or wildcard topic.
/// - `404`: no retained message exists for the topic.
/// - `503`: retain storage unavailable.
#[handler]
async fn delete_retain(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let http_laddr = cfg.read().await.http_laddr;

    let topic = match req.query::<String>("topic") {
        Some(t) if !t.trim().is_empty() => TopicName::from(t.trim()),
        _ => {
            render_api_error(res, StatusCode::BAD_REQUEST, "topic is required");
            return Ok(());
        }
    };

    // Deletion requires a concrete topic; wildcards are not supported.
    let topic_str = topic.to_string();
    if topic_str.contains('#') || topic_str.contains('+') {
        render_api_error(
            res,
            StatusCode::BAD_REQUEST,
            "topic must be a concrete topic, wildcards '#' and '+' are not allowed",
        );
        return Ok(());
    }

    let retain_mgr = scx.extends.retain().await;
    if !retain_mgr.enable() {
        render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, "retain storage is not enabled");
        return Ok(());
    }

    // Return 404 when no retained message exists for the exact topic.
    match retain_mgr.get(&topic).await {
        Ok(list) => {
            if !list.iter().any(|(t, _)| t == &topic) {
                render_not_found(res, format!("retain message not found for topic: {topic}"));
                return Ok(());
            }
        }
        Err(e) => {
            render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string());
            return Ok(());
        }
    }

    // Empty-payload retained publish clears the retain store (MQTT semantics).
    let from = From::from_admin(Id::new(
        scx.node.id(),
        http_laddr.port(),
        Some(http_laddr),
        None,
        ClientId::default(),
        Some(UserName::from("admin")),
    ));
    let p = CodecPublish {
        dup: false,
        retain: true,
        qos: QoS::AtMostOnce,
        topic: topic.clone(),
        packet_id: None,
        payload: bytes::Bytes::new(),
        properties: Some(PublishProperties::default()),
    };
    let retain = Retain { msg_id: None, from, publish: <CodecPublish as Into<Publish>>::into(p) };

    if let Err(e) = retain_mgr.set(&topic, retain.clone(), None).await {
        render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string());
        return Ok(());
    }

    // Propagate the deletion to cluster peers so their local stores stay in sync.
    if let Err(e) = scx.extends.shared().await.retain_set_broadcast(&topic, &retain, None).await {
        log::warn!("retain delete broadcast to cluster peers failed, {e}");
    }

    res.render(Text::Plain("ok"));
    Ok(())
}

#[handler]
async fn publish(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let (http_laddr, expiry_interval) = {
        let cfg_rl = cfg.read().await;
        (cfg_rl.http_laddr, cfg_rl.message_expiry_interval)
    };

    let addr = req.remote_addr();
    let remote_addr = if let Some(ipv4) = addr.as_ipv4() {
        Some(SocketAddr::V4(*ipv4))
    } else {
        addr.as_ipv6().map(|ipv6| SocketAddr::V6(*ipv6))
    };

    let params = match req.parse_json::<PublishParams>().await {
        Ok(p) => p,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    match _publish(scx, params, remote_addr, http_laddr, expiry_interval).await {
        Ok(()) => res.render(Text::Plain("ok")),
        Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
    }
    Ok(())
}

async fn _publish(
    scx: &ServerContext,
    params: PublishParams,
    remote_addr: Option<SocketAddr>,
    http_laddr: SocketAddr,
    expiry_interval: Duration,
) -> Result<()> {
    let mut topics = if let Some(topics) = params.topics {
        topics.split(',').collect::<Vec<_>>().iter().map(|t| TopicName::from(t.trim())).collect()
    } else {
        Vec::new()
    };
    if let Some(topic) = params.topic {
        topics.push(topic);
    }
    if topics.is_empty() {
        return Err(anyhow!("topics or topic is empty"));
    }
    let qos = QoS::try_from(params.qos).map_err(|e| anyhow::Error::msg(e.to_string()))?;
    let encoding = params.encoding.to_ascii_lowercase();
    let payload = if encoding == "plain" {
        bytes::Bytes::from(params.payload)
    } else if encoding == "base64" {
        bytes::Bytes::from(BASE64_STANDARD.decode(params.payload).map_err(anyhow::Error::new)?)
    } else {
        return Err(anyhow!("encoding error, currently only plain and base64 are supported"));
    };

    let from = From::from_admin(Id::new(
        scx.node.id(),
        http_laddr.port(),
        Some(http_laddr),
        remote_addr,
        params.clientid,
        Some(UserName::from("admin")),
    ));
    let p = CodecPublish {
        dup: false,
        retain: params.retain,
        qos,
        topic: "".into(),
        packet_id: None,
        payload,
        properties: Some(PublishProperties::default()),
    };

    let message_expiry_interval = params
        .properties
        .as_ref()
        .and_then(|props| {
            props.message_expiry_interval.map(|interval| Duration::from_secs(interval.get() as u64))
        })
        .unwrap_or(expiry_interval);
    log::debug!("message_expiry_interval: {message_expiry_interval:?}");

    let storage_available = scx.extends.message_mgr().await.enable();

    let create_time = timestamp_millis();

    let mut futs = Vec::new();
    for topic in topics {
        let from = from.clone();
        let mut p1 = p.clone();
        p1.topic = topic;
        let p1 = <CodecPublish as Into<Publish>>::into(p1).create_time(create_time);
        let fut = async move {
            //hook, message_publish
            let p1 = scx.extends.hook_mgr().message_publish(None, from.clone(), &p1).await.unwrap_or(p1);

            if let Err(e) =
                SessionState::forwards(scx, from, p1, storage_available, Some(message_expiry_interval)).await
            {
                log::warn!("{e}");
            }
        };
        futs.push(fut);
    }
    let _ = futures::future::join_all(futs).await;
    Ok(())
}

#[handler]
async fn subscribe(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let params = match req.parse_json::<SubscribeParams>().await {
        Ok(p) => p,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };

    let node_id = if let Some(status) = scx.extends.shared().await.session_status(&params.clientid).await {
        if status.online {
            status.id.node_id
        } else {
            render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, "the session is offline");
            return Ok(());
        }
    } else {
        render_not_found(res, "session does not exist");
        return Ok(());
    };

    if node_id == scx.node.id() {
        #[allow(clippy::mutable_key_type)]
        match subs::subscribe(scx, params).await {
            Ok(replys) => {
                let replys = replys
                    .into_iter()
                    .map(|(t, r)| {
                        let r = match r {
                            Ok(b) => serde_json::Value::Bool(b),
                            Err(e) => serde_json::Value::String(e.to_string()),
                        };
                        (t, r)
                    })
                    .collect::<HashMap<_, _>>();
                res.render(Json(replys))
            }
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    } else {
        // let cfg = get_cfg(depot)?;
        let message_type = cfg.read().await.message_type;
        //The session is on another node
        #[allow(clippy::mutable_key_type)]
        match _subscribe_on_other_node(scx, message_type, node_id, params).await {
            Ok(replys) => {
                let replys = replys
                    .into_iter()
                    .map(|(t, r)| {
                        let r = match r {
                            (b, None) => serde_json::Value::Bool(b),
                            (true, _) => serde_json::Value::Bool(true),
                            (false, Some(reason)) => serde_json::Value::String(reason),
                        };
                        (t, r)
                    })
                    .collect::<HashMap<_, _>>();
                res.render(Json(replys))
            }
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    }
    Ok(())
}

#[inline]
async fn _subscribe_on_other_node(
    scx: &ServerContext,
    message_type: MessageType,
    node_id: NodeId,
    params: SubscribeParams,
) -> Result<HashMap<TopicFilter, (bool, Option<String>)>> {
    let c = get_grpc_client(scx, node_id).await?;
    let q = Message::Subscribe(params).encode()?;
    let reply =
        MessageSender::new_quick(c, message_type, GrpcMessage::Data(q), Some(Duration::from_secs(15)))
            .send()
            .await?;
    match reply {
        GrpcMessageReply::Data(res) => match MessageReply::decode(&res)? {
            MessageReply::Subscribe(ress) => Ok(ress),
            _ => {
                log::error!("unreachable!(), res: {res:?}");
                Err(anyhow!("unreachable!()"))
            }
        },
        reply => {
            log::info!("Subscribe GrpcMessage::Subscribe from other node({node_id}), reply: {reply:?}");
            Err(anyhow!("Invalid Operation"))
        }
    }
}

#[handler]
async fn unsubscribe(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let params = match req.parse_json::<UnsubscribeParams>().await {
        Ok(p) => p,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };

    let node_id = if let Some(status) = scx.extends.shared().await.session_status(&params.clientid).await {
        if status.online {
            status.id.node_id
        } else {
            render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, "the session is offline");
            return Ok(());
        }
    } else {
        render_not_found(res, "session does not exist");
        return Ok(());
    };

    if node_id == scx.node.id() {
        match subs::unsubscribe(scx, params).await {
            Ok(()) => res.render(Json(true)),
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    } else {
        // let cfg = get_cfg(depot)?;
        let message_type = cfg.read().await.message_type;
        //The session is on another node
        match _unsubscribe_on_other_node(scx, message_type, node_id, params).await {
            Ok(()) => res.render(Text::Plain("ok")),
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    }
    Ok(())
}

#[inline]
async fn _unsubscribe_on_other_node(
    scx: &ServerContext,
    message_type: MessageType,
    node_id: NodeId,
    params: UnsubscribeParams,
) -> Result<()> {
    let c = get_grpc_client(scx, node_id).await?;
    let q = Message::Unsubscribe(params).encode()?;
    let reply =
        MessageSender::new_quick(c, message_type, GrpcMessage::Data(q), Some(Duration::from_secs(15)))
            .send()
            .await?;
    match reply {
        GrpcMessageReply::Data(res) => match MessageReply::decode(&res)? {
            MessageReply::Unsubscribe => Ok(()),
            _ => {
                log::error!("unreachable!(), res: {res:?}");
                Err(anyhow!("unreachable!()"))
            }
        },
        reply => {
            log::info!("Unsubscribe GrpcMessage::Unsubscribe from other node({node_id}), reply: {reply:?}");
            Err(anyhow!("Invalid Operation"))
        }
    }
}

#[handler]
async fn all_plugins(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;
    let max_row_limit = cfg.read().await.max_row_limit;
    let requested = req.query::<usize>("_limit").or_else(|| req.query::<usize>("limit")).unwrap_or(0);
    let paging = ListPaging::from_request(req, requested, max_row_limit);

    match _all_plugins(scx, message_type).await {
        Ok(pluginss) => {
            let (page, truncated) = paging.apply(pluginss);
            render_list(req, res, page, paging, truncated, None);
        }
        Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
    }
    Ok(())
}

#[inline]
async fn _all_plugins(scx: &ServerContext, message_type: MessageType) -> Result<Vec<serde_json::Value>> {
    let mut pluginss = Vec::new();
    let node_id = scx.node.id();
    let plugins = plugin::get_plugins(scx).await?;
    let plugins = plugins.into_iter().map(|p| p.to_json()).collect::<Result<Vec<_>>>()?;
    pluginss.push(json!({
        "ok": true,
        "node": node_id,
        "plugins": plugins,
    }));

    let grpc_clients = scx.extends.shared().await.get_grpc_clients();
    if !grpc_clients.is_empty() {
        let msg = Message::GetPlugins.encode()?;
        let replys = MessageBroadcaster::new_quick(
            grpc_clients,
            message_type,
            GrpcMessage::Data(msg),
            Some(Duration::from_secs(10)),
        )
        .join_all()
        .await
        .drain(..)
        .map(|(node_id, reply)| match reply {
            Ok(GrpcMessageReply::Data(reply_msg)) => match MessageReply::decode(&reply_msg) {
                Ok(MessageReply::GetPlugins(plugins)) => {
                    match plugins.into_iter().map(|p| p.to_json()).collect::<Result<Vec<_>>>() {
                        Ok(plugins) => json!({
                            "ok": true,
                            "node": node_id,
                            "plugins": plugins,
                        }),
                        Err(e) => json!({
                            "ok": false,
                            "node": node_id,
                            "plugins": [],
                            "error": e.to_string(),
                        }),
                    }
                }
                Err(e) => json!({
                    "ok": false,
                    "node": node_id,
                    "plugins": [],
                    "error": e.to_string(),
                }),
                _ => {
                    log::error!("unreachable!(), reply_msg: {reply_msg:?}");
                    json!({
                        "ok": false,
                        "node": node_id,
                        "plugins": [],
                        "error": "unreachable!()",
                    })
                }
            },
            Ok(_) => json!({
                "ok": false,
                "node": node_id,
                "plugins": [],
                "error": "Invalid Result",
            }),
            Err(e) => json!({
                "ok": false,
                "node": node_id,
                "plugins": [],
                "error": e.to_string(),
            }),
        })
        .collect::<Vec<_>>();
        pluginss.extend(replys);
    }
    Ok(pluginss)
}

#[handler]
async fn node_plugins(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;
    let node_id = if let Some(node_id) = req.param::<NodeId>("node") {
        node_id
    } else {
        render_not_found(res, "node not found");
        return Ok(());
    };
    let max_row_limit = cfg.read().await.max_row_limit;
    let requested = req.query::<usize>("_limit").or_else(|| req.query::<usize>("limit")).unwrap_or(0);
    let paging = ListPaging::from_request(req, requested, max_row_limit);
    match _node_plugins(scx, node_id, message_type).await {
        Ok(plugins) => {
            let (page, truncated) = paging.apply(plugins);
            render_list(req, res, page, paging, truncated, None);
        }
        Err(e) => render_api_error(res, status_for_plugin_error(&e), e.to_string()),
    }
    Ok(())
}

async fn _node_plugins(
    scx: &ServerContext,
    node_id: NodeId,
    message_type: MessageType,
) -> Result<Vec<serde_json::Value>> {
    let plugins = if node_id == scx.node.id() {
        plugin::get_plugins(scx).await?
    } else {
        let c = get_grpc_client(scx, node_id).await?;
        let msg = Message::GetPlugins.encode()?;
        let reply =
            MessageSender::new_quick(c, message_type, GrpcMessage::Data(msg), Some(Duration::from_secs(10)))
                .send()
                .await?;
        match reply {
            GrpcMessageReply::Data(msg) => match MessageReply::decode(&msg)? {
                MessageReply::GetPlugins(plugins) => plugins,
                _ => {
                    log::error!("unreachable!(), msg: {msg:?}");
                    return Err(anyhow!("unreachable!()"));
                }
            },
            reply => {
                log::info!("Get GrpcMessage::GetPlugins from other node({node_id}), reply: {reply:?}");
                return Err(anyhow!("Invalid Result"));
            }
        }
    };
    plugins.into_iter().map(|p| p.to_json()).collect::<Result<Vec<_>>>()
}

#[handler]
async fn node_plugin_info(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;
    let node_id = if let Some(node_id) = req.param::<NodeId>("node") {
        node_id
    } else {
        render_not_found(res, "node not found");
        return Ok(());
    };
    let name = if let Some(name) = req.param::<String>("plugin") {
        name
    } else {
        render_not_found(res, "plugin not found");
        return Ok(());
    };

    match _node_plugin_info(scx, node_id, &name, message_type).await {
        Ok(Some(plugin)) => res.render(Json(plugin)),
        Ok(None) => render_not_found(res, format!("plugin not found: {name}")),
        Err(e) => render_api_error(res, status_for_plugin_error(&e), e.to_string()),
    }

    Ok(())
}

async fn _node_plugin_info(
    scx: &ServerContext,
    node_id: NodeId,
    name: &str,
    message_type: MessageType,
) -> Result<Option<serde_json::Value>> {
    let plugin = if node_id == scx.node.id() {
        plugin::get_plugin(scx, name).await?
    } else {
        let c = get_grpc_client(scx, node_id).await?;
        let msg = Message::GetPlugin { name }.encode()?;
        let reply =
            MessageSender::new_quick(c, message_type, GrpcMessage::Data(msg), Some(Duration::from_secs(10)))
                .send()
                .await?;
        match reply {
            GrpcMessageReply::Data(msg) => match MessageReply::decode(&msg)? {
                MessageReply::GetPlugin(plugin) => plugin,
                _ => {
                    log::error!("unreachable!(), msg: {msg:?}");
                    return Err(anyhow!("unreachable!()"));
                }
            },
            reply => {
                log::info!("Get GrpcMessage::GetPlugin from other node({node_id}), reply: {reply:?}");
                return Err(anyhow!("Invalid Result"));
            }
        }
    };
    if let Some(plugin) = plugin {
        Ok(Some(plugin.to_json()?))
    } else {
        Ok(None)
    }
}

#[handler]
async fn node_plugin_config(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;
    let node_id = if let Some(node_id) = req.param::<NodeId>("node") {
        node_id
    } else {
        render_not_found(res, "node not found");
        return Ok(());
    };
    let name = if let Some(name) = req.param::<String>("plugin") {
        name
    } else {
        render_not_found(res, "plugin not found");
        return Ok(());
    };

    match _node_plugin_config(scx, node_id, &name, message_type).await {
        Ok(cfg) => {
            res.headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("application/json; charset=utf-8"));
            res.write_body(cfg).ok();
        }
        Err(e) => render_api_error(res, status_for_plugin_error(&e), e.to_string()),
    }
    Ok(())
}

async fn _node_plugin_config(
    scx: &ServerContext,
    node_id: NodeId,
    name: &str,
    message_type: MessageType,
) -> Result<Vec<u8>> {
    let plugin_cfg = if node_id == scx.node.id() {
        plugin::get_plugin_config(scx, name).await?
    } else {
        let c = get_grpc_client(scx, node_id).await?;
        let msg = Message::GetPluginConfig { name }.encode()?;
        let reply =
            MessageSender::new_quick(c, message_type, GrpcMessage::Data(msg), Some(Duration::from_secs(10)))
                .send()
                .await?;
        match reply {
            GrpcMessageReply::Data(msg) => match MessageReply::decode(&msg)? {
                MessageReply::GetPluginConfig(cfg) => cfg,
                _ => {
                    log::error!("unreachable!(), msg: {msg:?}");
                    return Err(anyhow!("unreachable!()"));
                }
            },
            reply => {
                log::info!("Get GrpcMessage::GetPluginConfig from other node({node_id}), reply: {reply:?}");
                return Err(anyhow!("Invalid Result"));
            }
        }
    };
    Ok(plugin_cfg)
}

#[handler]
async fn node_plugin_config_reload(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;
    let node_id = if let Some(node_id) = req.param::<NodeId>("node") {
        node_id
    } else {
        render_not_found(res, "node not found");
        return Ok(());
    };
    let name = if let Some(name) = req.param::<String>("plugin") {
        name
    } else {
        render_not_found(res, "plugin not found");
        return Ok(());
    };

    match _node_plugin_config_reload(scx, node_id, &name, message_type).await {
        Ok(r) => res.render(Json(r)),
        Err(e) => render_api_error(res, status_for_plugin_error(&e), e.to_string()),
    }
    Ok(())
}

async fn _node_plugin_config_reload(
    scx: &ServerContext,
    node_id: NodeId,
    name: &str,
    message_type: MessageType,
) -> Result<bool> {
    if node_id == scx.node.id() {
        scx.plugins.load_config(name).await?;
        Ok(true)
    } else {
        let c = get_grpc_client(scx, node_id).await?;
        let msg = Message::ReloadPluginConfig { name }.encode()?;
        let reply =
            MessageSender::new_quick(c, message_type, GrpcMessage::Data(msg), Some(Duration::from_secs(15)))
                .send()
                .await?;
        match reply {
            GrpcMessageReply::Data(msg) => match MessageReply::decode(&msg)? {
                MessageReply::ReloadPluginConfig => Ok(true),
                _ => {
                    log::error!("unreachable!(), msg: {msg:?}");
                    Err(anyhow!("unreachable!()"))
                }
            },
            reply => {
                log::info!(
                    "ConfigReload GrpcMessage::ReloadPluginConfig from other node({node_id}), reply: {reply:?}"
                );
                Ok(false)
            }
        }
    }
}

#[handler]
async fn node_plugin_load(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;
    let node_id = if let Some(node_id) = req.param::<NodeId>("node") {
        node_id
    } else {
        render_not_found(res, "node not found");
        return Ok(());
    };
    let name = if let Some(name) = req.param::<String>("plugin") {
        name
    } else {
        render_not_found(res, "plugin not found");
        return Ok(());
    };

    match _node_plugin_load(scx, node_id, &name, message_type).await {
        Ok(r) => res.render(Json(r)),
        Err(e) => render_api_error(res, status_for_plugin_error(&e), e.to_string()),
    }
    Ok(())
}

async fn _node_plugin_load(
    scx: &ServerContext,
    node_id: NodeId,
    name: &str,
    message_type: MessageType,
) -> Result<bool> {
    if node_id == scx.node.id() {
        scx.plugins.start(name).await?;
        Ok(true)
    } else {
        let c = get_grpc_client(scx, node_id).await?;
        let msg = Message::LoadPlugin { name }.encode()?;
        let reply =
            MessageSender::new_quick(c, message_type, GrpcMessage::Data(msg), Some(Duration::from_secs(10)))
                .send()
                .await?;
        match reply {
            GrpcMessageReply::Data(msg) => match MessageReply::decode(&msg)? {
                MessageReply::LoadPlugin => Ok(true),
                _ => {
                    log::error!("unreachable!(), msg: {msg:?}");
                    Err(anyhow!("unreachable!()"))
                }
            },
            reply => {
                log::info!("Load GrpcMessage::LoadPlugin from other node({node_id}), reply: {reply:?}");
                Ok(false)
            }
        }
    }
}

#[handler]
async fn node_plugin_unload(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    //let cfg = get_cfg(depot)?;
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;
    let node_id = if let Some(node_id) = req.param::<NodeId>("node") {
        node_id
    } else {
        render_not_found(res, "node not found");
        return Ok(());
    };
    let name = if let Some(name) = req.param::<String>("plugin") {
        name
    } else {
        render_not_found(res, "plugin not found");
        return Ok(());
    };

    match _node_plugin_unload(scx, node_id, &name, message_type).await {
        Ok(r) => res.render(Json(r)),
        Err(e) => render_api_error(res, status_for_plugin_error(&e), e.to_string()),
    }
    Ok(())
}

async fn _node_plugin_unload(
    scx: &ServerContext,
    node_id: NodeId,
    name: &str,
    message_type: MessageType,
) -> Result<bool> {
    if node_id == scx.node.id() {
        scx.plugins.stop(name).await
    } else {
        let c = get_grpc_client(scx, node_id).await?;
        let msg = Message::UnloadPlugin { name }.encode()?;
        let reply =
            MessageSender::new_quick(c, message_type, GrpcMessage::Data(msg), Some(Duration::from_secs(10)))
                .send()
                .await?;
        match reply {
            GrpcMessageReply::Data(msg) => match MessageReply::decode(&msg)? {
                MessageReply::UnloadPlugin(ok) => Ok(ok),
                _ => {
                    log::error!("unreachable!(), msg: {msg:?}");
                    Err(anyhow!("unreachable!()"))
                }
            },
            reply => {
                log::info!("Unload GrpcMessage::UnloadPlugin from other node({node_id}), reply: {reply:?}");
                Ok(false)
            }
        }
    }
}

#[handler]
async fn get_stats_sum(depot: &mut Depot, res: &mut Response) -> std::result::Result<(), salvo::Error> {
    // let cfg = get_cfg(depot)?;
    let (scx, cfg) = get_scx_cfg(depot)?;

    let message_type = cfg.read().await.message_type;

    match _get_stats_sum(scx, message_type, false).await {
        Ok(stats_sum) => res.render(Json(stats_sum)),
        Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
    }
    Ok(())
}

async fn _get_stats_sum(
    scx: &ServerContext,
    message_type: MessageType,
    is_sys: bool,
) -> Result<serde_json::Value> {
    let this_id = scx.node.id();
    let mut nodes = HashMap::default();
    nodes.insert(
        this_id,
        json!({
            "ok": true,
            "name": scx.node.name(scx,this_id).await,
            "running": scx.node.status(scx).await.is_running(),
        }),
    );

    let mut stats_sum = scx.stats.clone(scx).await;
    let grpc_clients = scx.extends.shared().await.get_grpc_clients();
    if !grpc_clients.is_empty() {
        let msg = Message::StatsInfo.encode()?;
        for reply in MessageBroadcaster::new_quick(
            grpc_clients,
            message_type,
            GrpcMessage::Data(msg),
            Some(Duration::from_secs(10)),
        )
        .join_all()
        .await
        {
            match reply {
                (id, Ok(GrpcMessageReply::Data(msg))) => match MessageReply::decode(&msg)? {
                    MessageReply::StatsInfo(node_status, stats) => {
                        nodes.insert(
                            id,
                            json!({
                                "ok": true,
                                "name": scx.node.name(scx, id).await,
                                "running": node_status.is_running(),
                            }),
                        );
                        stats_sum.add(*stats);
                    }
                    _ => {
                        log::error!("unreachable!(), msg: {msg:?}");
                        return Err(anyhow!("unreachable!()"));
                    }
                },
                (id, Ok(reply)) => {
                    log::info!("Get GrpcMessage::StateInfo from other node({id}), reply: {reply:?}");
                    continue;
                }
                (id, Err(e)) => {
                    log::warn!("Get GrpcMessage::StateInfo from other node({id}), error: {e}");
                    nodes.insert(id, cluster_node_failure(id, e));
                }
            };
        }
    }

    let stats_sum = json!({
        "nodes": nodes,
        "stats": if is_sys { stats_sum.to_sys_json(scx).await} else {stats_sum.to_json(scx).await}
    });

    Ok(stats_sum)
}

#[handler]
async fn get_stats(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    //let cfg = get_cfg(depot)?;
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;

    let id = req.param::<NodeId>("id");
    if let Some(id) = id {
        match get_stats_one(scx, message_type, id).await {
            Ok(Some((node_status, stats))) => {
                let stat_info = _build_stats(scx, id, node_status, stats.to_json(scx).await).await;
                res.render(Json(stat_info))
            }
            Ok(None) => {
                //| Err(MqttError::None)
                render_not_found(res, "not found");
            }
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    } else {
        match get_stats_all(scx, message_type).await {
            Ok(stats) => {
                let mut stat_infos = Vec::new();
                for item in stats {
                    match item {
                        Ok((id, node_status, state)) => {
                            stat_infos
                                .push(_build_stats(scx, id, node_status, state.to_json(scx).await).await);
                        }
                        Err((id, e)) => {
                            stat_infos.push(json!({
                                "ok": false,
                                "node": { "id": id },
                                "error": e.to_string(),
                            }));
                        }
                    }
                }
                res.render(Json(stat_infos))
            }
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    }
    Ok(())
}

#[inline]
pub(crate) async fn get_stats_one(
    scx: &ServerContext,
    message_type: MessageType,
    id: NodeId,
) -> Result<Option<(NodeStatus, Box<Stats>)>> {
    if id == scx.node.id() {
        let node_status = scx.node.status(scx).await;
        let stats = scx.stats.clone(scx).await;
        Ok(Some((node_status, Box::new(stats))))
    } else {
        let grpc_clients = scx.extends.shared().await.get_grpc_clients();
        if let Some(c) = grpc_clients.get(&id).map(|(_, c)| c.clone()) {
            let msg = Message::StatsInfo.encode()?;
            let reply = MessageSender::new_quick(
                c,
                message_type,
                GrpcMessage::Data(msg),
                Some(Duration::from_secs(10)),
            )
            .send()
            .await;
            match reply {
                Ok(GrpcMessageReply::Data(msg)) => match MessageReply::decode(&msg)? {
                    MessageReply::StatsInfo(node_status, stats) => Ok(Some((node_status, stats))),
                    _ => {
                        log::error!("unreachable!(), msg: {msg:?}");
                        Err(anyhow!("unreachable!()"))
                    }
                },
                Ok(reply) => {
                    log::info!("Get GrpcMessage::StateInfo from other node, reply: {reply:?}");
                    Err(anyhow!("Invalid Result"))
                }
                Err(e) => {
                    log::warn!("Get GrpcMessage::StateInfo from other node, error: {e}");
                    Err(e)
                }
            }
        } else {
            Ok(None)
        }
    }
}

#[inline]
pub(crate) async fn get_stats_all(
    scx: &ServerContext,
    message_type: MessageType,
) -> Result<Vec<std::result::Result<(NodeId, NodeStatus, Box<Stats>), (NodeId, anyhow::Error)>>> {
    let id = scx.node.id();
    let node_status = scx.node.status(scx).await;
    let state = scx.stats.clone(scx).await;
    let mut stats = vec![Ok((id, node_status, Box::new(state)))];

    let grpc_clients = scx.extends.shared().await.get_grpc_clients();
    if !grpc_clients.is_empty() {
        let msg = Message::StatsInfo.encode()?;
        for reply in MessageBroadcaster::new_quick(
            grpc_clients,
            message_type,
            GrpcMessage::Data(msg),
            Some(Duration::from_secs(10)),
        )
        .join_all()
        .await
        {
            let data = match reply {
                (id, Ok(GrpcMessageReply::Data(msg))) => match MessageReply::decode(&msg)? {
                    MessageReply::StatsInfo(node_status, stats) => Ok((id, node_status, stats)),
                    _ => {
                        log::error!("unreachable!(), msg: {msg:?}");
                        return Err(anyhow!("unreachable!()"));
                    }
                },
                (id, Ok(reply)) => {
                    log::info!("Get GrpcMessage::StateInfo from other node({id}), reply: {reply:?}");
                    Err((id, anyhow!("Invalid Result")))
                }
                (id, Err(e)) => {
                    log::warn!("Get GrpcMessage::StateInfo from other node({id}), error: {e}");
                    Err((id, e))
                }
            };
            stats.push(data);
        }
    }
    Ok(stats)
}

#[inline]
async fn _build_stats(
    scx: &ServerContext,
    id: NodeId,
    node_status: NodeStatus,
    stats: serde_json::Value,
) -> serde_json::Value {
    let node_name = scx.node.name(scx, id).await;
    json!({
        "ok": true,
        "node": {
            "id": id,
            "name": node_name,
            "running": node_status.is_running(),
        },
        "stats": stats
    })
}

#[handler]
async fn get_sys_stats(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;

    let id = req.param::<NodeId>("id");
    if let Some(id) = id {
        match get_stats_one(scx, message_type, id).await {
            Ok(Some((node_status, stats))) => {
                let stat_info = _build_stats(scx, id, node_status, stats.to_sys_json(scx).await).await;
                res.render(Json(stat_info))
            }
            Ok(None) => {
                render_not_found(res, "not found");
            }
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    } else {
        match get_stats_all(scx, message_type).await {
            Ok(stats) => {
                let mut stat_infos = Vec::new();
                for item in stats {
                    match item {
                        Ok((id, node_status, state)) => {
                            stat_infos
                                .push(_build_stats(scx, id, node_status, state.to_sys_json(scx).await).await);
                        }
                        Err((id, e)) => {
                            stat_infos.push(json!({
                                "ok": false,
                                "node": { "id": id },
                                "error": e.to_string(),
                            }));
                        }
                    }
                }
                res.render(Json(stat_infos))
            }
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    }
    Ok(())
}

#[handler]
async fn get_sys_stats_sum(depot: &mut Depot, res: &mut Response) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;

    let message_type = cfg.read().await.message_type;

    match _get_stats_sum(scx, message_type, true).await {
        Ok(stats_sum) => res.render(Json(stats_sum)),
        Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
    }
    Ok(())
}

#[handler]
async fn get_metrics(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;

    let message_type = cfg.read().await.message_type;

    let id = req.param::<NodeId>("id");
    if let Some(id) = id {
        match get_metrics_one(scx, message_type, id).await {
            Ok(Some(metrics)) => {
                let metrics = _build_metrics(scx, id, metrics.to_json()).await;
                res.render(Json(metrics))
            }
            Ok(None) => {
                render_not_found(res, "not found");
            }
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    } else {
        match get_metrics_all(scx, message_type).await {
            Ok(items) => {
                let mut metrics_infos = Vec::new();
                for item in items {
                    match item {
                        Ok((id, metrics)) => {
                            metrics_infos.push(_build_metrics(scx, id, metrics.to_json()).await);
                        }
                        Err((id, e)) => {
                            metrics_infos.push(json!({
                                "ok": false,
                                "node": { "id": id },
                                "error": e.to_string(),
                            }));
                        }
                    }
                }
                res.render(Json(metrics_infos))
            }
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    }
    Ok(())
}

#[inline]
pub(crate) async fn get_metrics_one(
    scx: &ServerContext,
    message_type: MessageType,
    id: NodeId,
) -> Result<Option<Box<Metrics>>> {
    if id == scx.node.id() {
        // let metrics = scx.metrics;
        Ok(Some(Box::new(scx.metrics.clone())))
    } else {
        let grpc_clients = scx.extends.shared().await.get_grpc_clients();
        if let Some(c) = grpc_clients.get(&id).map(|(_, c)| c.clone()) {
            let msg = Message::MetricsInfo.encode()?;
            let reply = MessageSender::new_quick(
                c,
                message_type,
                GrpcMessage::Data(msg),
                Some(Duration::from_secs(10)),
            )
            .send()
            .await;
            match reply {
                Ok(GrpcMessageReply::Data(msg)) => match MessageReply::decode(&msg)? {
                    MessageReply::MetricsInfo(metrics) => Ok(Some(metrics)),
                    _ => {
                        log::error!("unreachable!(), msg: {msg:?}");
                        Err(anyhow!("unreachable!()"))
                    }
                },
                Ok(reply) => {
                    log::info!("Get GrpcMessage::MetricsInfo from other node, reply: {reply:?}");
                    Err(anyhow!("Invalid Result"))
                }
                Err(e) => {
                    log::warn!("Get GrpcMessage::MetricsInfo from other node, error: {e}");
                    Err(e)
                }
            }
        } else {
            Ok(None)
        }
    }
}

#[inline]
pub(crate) async fn get_metrics_all(
    scx: &ServerContext,
    message_type: MessageType,
) -> Result<Vec<std::result::Result<(NodeId, Box<Metrics>), (NodeId, anyhow::Error)>>> {
    let id = scx.node.id();
    let mut metricses = vec![Ok((id, Box::new(scx.metrics.clone())))];

    let grpc_clients = scx.extends.shared().await.get_grpc_clients();
    if !grpc_clients.is_empty() {
        let msg = Message::MetricsInfo.encode()?;
        let replys = MessageBroadcaster::new_quick(
            grpc_clients,
            message_type,
            GrpcMessage::Data(msg),
            Some(Duration::from_secs(10)),
        )
        .join_all()
        .await;
        for reply in replys {
            let data = match reply {
                (id, Ok(GrpcMessageReply::Data(msg))) => match MessageReply::decode(&msg)? {
                    MessageReply::MetricsInfo(metrics) => Ok((id, metrics)),
                    _ => {
                        log::error!("unreachable!(), msg: {msg:?}");
                        return Err(anyhow!("unreachable!()"));
                    }
                },
                (id, Ok(reply)) => {
                    log::info!("Get GrpcMessage::MetricsInfo from other node({id}), reply: {reply:?}");
                    Err((id, anyhow!("Invalid Result")))
                }
                (id, Err(e)) => {
                    log::warn!("Get GrpcMessage::MetricsInfo from other node({id}), error: {e}");
                    Err((id, e))
                }
            };
            metricses.push(data);
        }
    }
    Ok(metricses)
}

#[handler]
async fn get_metrics_sum(depot: &mut Depot, res: &mut Response) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;

    match _get_metrics_sum(scx, message_type).await {
        Ok(metrics_sum) => res.render(Json(metrics_sum)),
        Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
    }
    Ok(())
}

async fn _get_metrics_sum(scx: &ServerContext, message_type: MessageType) -> Result<serde_json::Value> {
    let mut metrics_sum = scx.metrics.clone();
    let grpc_clients = scx.extends.shared().await.get_grpc_clients();
    if !grpc_clients.is_empty() {
        let msg = Message::MetricsInfo.encode()?;
        for reply in MessageBroadcaster::new_quick(
            grpc_clients,
            message_type,
            GrpcMessage::Data(msg),
            Some(Duration::from_secs(10)),
        )
        .join_all()
        .await
        {
            match reply {
                (_, Ok(GrpcMessageReply::Data(msg))) => match MessageReply::decode(&msg)? {
                    MessageReply::MetricsInfo(metrics) => metrics_sum.add(&metrics),
                    _ => {
                        log::error!("unreachable!(), msg: {msg:?}");
                        return Err(anyhow!("unreachable!()"));
                    }
                },
                (id, Ok(reply)) => {
                    log::info!("Get GrpcMessage::MetricsInfo from other node({id}), reply: {reply:?}");
                }
                (id, Err(e)) => {
                    log::warn!("Get GrpcMessage::MetricsInfo from other node({id}), error: {e}");
                }
            };
        }
    }

    Ok(metrics_sum.to_json())
}

#[inline]
async fn _build_metrics(scx: &ServerContext, id: NodeId, metrics: serde_json::Value) -> serde_json::Value {
    let node_name = scx.node.name(scx, id).await;
    json!({
        "ok": true,
        "node": {
            "id": id,
            "name": node_name,
        },
        "metrics": metrics
    })
}

#[handler]
async fn get_prometheus_metrics(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let monitor = get_monitor(depot)?;
    let (scx, cfg) = get_scx_cfg(depot)?;

    let (message_type, cache_interval) = {
        let cfg_rl = cfg.read().await;
        (cfg_rl.message_type, cfg_rl.prometheus_metrics_cache_interval)
    };
    let id = req.param::<NodeId>("id");
    if let Some(id) = id {
        match prome::to_metrics(scx, monitor, message_type, cache_interval, PrometheusDataType::Node(id))
            .await
        {
            Ok(metrics) => {
                res.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"));
                res.write_body(metrics).ok();
            }
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    } else {
        match prome::to_metrics(scx, monitor, message_type, cache_interval, PrometheusDataType::All).await {
            Ok(metrics) => {
                res.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"));
                res.write_body(metrics).ok();
            }
            Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
        }
    }
    Ok(())
}

#[handler]
async fn get_prometheus_metrics_sum(
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let monitor = get_monitor(depot)?;
    let (scx, cfg) = get_scx_cfg(depot)?;
    let (message_type, cache_interval) = {
        let cfg_rl = cfg.read().await;
        (cfg_rl.message_type, cfg_rl.prometheus_metrics_cache_interval)
    };
    match prome::to_metrics(scx, monitor, message_type, cache_interval, PrometheusDataType::Sum).await {
        Ok(metrics) => {
            res.headers_mut().insert(CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"));
            res.write_body(metrics).ok();
        }
        Err(e) => render_api_error(res, StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
    }
    Ok(())
}

#[inline]
async fn get_grpc_client(scx: &ServerContext, node_id: NodeId) -> Result<GrpcClient> {
    scx.extends
        .shared()
        .await
        .get_grpc_clients()
        .get(&node_id)
        .map(|(_, c)| c.clone())
        .ok_or_else(|| anyhow!("node grpc client is not exist!"))
}

// ═════════════════════════════════════════════════════════════════════════
//  History query helpers & HTTP handlers
// ═════════════════════════════════════════════════════════════════════════

/// Queries the local LRU cache for history data points in the given time range.
///
/// Unlike the old version, this does **no Storage IO** — it reads exclusively
/// from the in-memory LRU cache (stats_cache or metrics_cache).
///
/// `interval_ms` is the flush interval in milliseconds (e.g. 5000 for 5s),
/// used to round timestamps and compute the step between consecutive keys.
pub(crate) async fn query_history_local(
    cache: &HistoryCache,
    node_id: NodeId,
    start_ts: u64,
    end_ts: u64,
    limit: usize,
    interval_ms: u64,
    merge_window: Option<u64>,
) -> HistoryData {
    let step_ms = merge_window.map(|s| s * 1000).unwrap_or(interval_ms);
    let from_rounded = (start_ts / step_ms) * step_ms;
    let to_rounded = (end_ts / step_ms) * step_ms;
    let expected_count = ((to_rounded - from_rounded) / step_ms + 1) as usize;
    let mut entries: Vec<(u64, serde_json::Value)> = Vec::with_capacity(expected_count.min(limit));

    let guard = cache.read().await;
    for i in 0..expected_count {
        let ts = from_rounded + i as u64 * step_ms;
        if let Some(entry) = guard.peek(&ts) {
            if let Ok(mut val) = serde_json::from_str::<serde_json::Value>(&entry.json) {
                if let Some(obj) = val.as_object_mut() {
                    obj.insert("ts".into(), json!(ts));
                }
                entries.push((ts, val));
            }
        }
    }
    drop(guard);

    // Sort descending by timestamp (newest first).
    entries.sort_by_key(|b| std::cmp::Reverse(b.0));
    entries.truncate(limit);

    let data: Vec<serde_json::Value> = entries.into_iter().map(|(_, v)| v).collect();
    HistoryData { node: node_id, from: start_ts, to: end_ts, count: data.len(), data }
}

// ── Stats history ──────────────────────────────────────────────────────

#[handler]
async fn get_stats_history(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let hc = get_history_caches(depot);
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;
    let interval_ms = cfg.read().await.flush_interval.as_millis() as u64;

    let id = req.param::<NodeId>("id");
    let (start_ts, end_ts, limit, merge_window) = { parse_time_params(req) };

    if let Some(ref hc) = hc {
        if let Some(node_id) = id {
            let data = if node_id == scx.node.id() {
                query_history_local(&hc.stats, node_id, start_ts, end_ts, limit, interval_ms, merge_window)
                    .await
            } else {
                query_history_remote(
                    scx,
                    message_type,
                    node_id,
                    Message::StatsHistoryQuery(HistoryQuery { start_ts, end_ts, limit, merge_window }),
                )
                .await
            };
            let result = json!({
                "from": data.from,
                "to": data.to,
                "node": data.node,
                "count": data.count,
                "data": data.data,
            });
            res.render(Json(result));
        } else {
            let msg_encoded =
                Message::StatsHistoryQuery(HistoryQuery { start_ts, end_ts, limit, merge_window })
                    .encode()
                    .unwrap_or_default();
            let local_node_id = scx.node.id();
            let params = HistoryQueryParams { start_ts, end_ts, limit, interval_ms, merge_window };
            let results =
                query_history_all_nodes(scx, message_type, &hc.stats, &params, msg_encoded, local_node_id)
                    .await;
            res.render(Json(json!({
                "from": start_ts,
                "to": end_ts,
                "nodes": results,
            })));
        }
    } else {
        res.render(Json(json!({
            "error": "history storage is not configured"
        })));
    }
    Ok(())
}

#[handler]
async fn get_stats_history_sum(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let hc = get_history_caches(depot);
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;
    let interval_ms = cfg.read().await.flush_interval.as_millis() as u64;

    let (start_ts, end_ts, limit, merge_window) = { parse_time_params(req) };

    if let Some(ref hc) = hc {
        let params = HistoryQueryParams { start_ts, end_ts, limit, interval_ms, merge_window };
        let nodes_data = query_history_all_nodes(
            scx,
            message_type,
            &hc.stats,
            &params,
            Message::StatsHistoryQuery(HistoryQuery { start_ts, end_ts, limit, merge_window })
                .encode()
                .unwrap_or_default(),
            scx.node.id(),
        )
        .await;

        let (aggregated, node_count) = aggregate_history_data(&nodes_data);
        res.render(Json(json!({
            "from": start_ts,
            "to": end_ts,
            "node_count": node_count,
            "count": aggregated.len(),
            "data": aggregated,
        })));
    } else {
        res.render(Json(json!({
            "error": "history storage is not configured"
        })));
    }
    Ok(())
}

// ── Metrics history ────────────────────────────────────────────────────

#[handler]
async fn get_metrics_history(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let hc = get_history_caches(depot);
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;
    let interval_ms = cfg.read().await.flush_interval.as_millis() as u64;

    let id = req.param::<NodeId>("id");
    let (start_ts, end_ts, limit, merge_window) = { parse_time_params(req) };

    if let Some(ref hc) = hc {
        if let Some(node_id) = id {
            let data = if node_id == scx.node.id() {
                query_history_local(&hc.metrics, node_id, start_ts, end_ts, limit, interval_ms, merge_window)
                    .await
            } else {
                query_history_remote(
                    scx,
                    message_type,
                    node_id,
                    Message::MetricsHistoryQuery(HistoryQuery { start_ts, end_ts, limit, merge_window }),
                )
                .await
            };
            let result = json!({
                "from": data.from,
                "to": data.to,
                "node": data.node,
                "count": data.count,
                "data": data.data,
            });
            res.render(Json(result));
        } else {
            let params = HistoryQueryParams { start_ts, end_ts, limit, interval_ms, merge_window };
            let results = query_history_all_nodes(
                scx,
                message_type,
                &hc.metrics,
                &params,
                Message::MetricsHistoryQuery(HistoryQuery { start_ts, end_ts, limit, merge_window })
                    .encode()
                    .unwrap_or_default(),
                scx.node.id(),
            )
            .await;
            res.render(Json(json!({
                "from": start_ts,
                "to": end_ts,
                "nodes": results,
            })));
        }
    } else {
        res.render(Json(json!({
            "error": "history storage is not configured"
        })));
    }
    Ok(())
}

#[handler]
async fn get_metrics_history_sum(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let hc = get_history_caches(depot);
    let (scx, cfg) = get_scx_cfg(depot)?;
    let message_type = cfg.read().await.message_type;
    let interval_ms = cfg.read().await.flush_interval.as_millis() as u64;

    let (start_ts, end_ts, limit, merge_window) = { parse_time_params(req) };

    if let Some(ref hc) = hc {
        let params = HistoryQueryParams { start_ts, end_ts, limit, interval_ms, merge_window };
        let nodes_data = query_history_all_nodes(
            scx,
            message_type,
            &hc.metrics,
            &params,
            Message::MetricsHistoryQuery(HistoryQuery { start_ts, end_ts, limit, merge_window })
                .encode()
                .unwrap_or_default(),
            scx.node.id(),
        )
        .await;

        let (aggregated, node_count) = aggregate_history_data(&nodes_data);
        res.render(Json(json!({
            "from": start_ts,
            "to": end_ts,
            "node_count": node_count,
            "count": aggregated.len(),
            "data": aggregated,
        })));
    } else {
        res.render(Json(json!({
            "error": "history storage is not configured"
        })));
    }
    Ok(())
}

// ═════════════════════════════════════════════════════════════════════════
//  Shared helpers
// ═════════════════════════════════════════════════════════════════════════

/// Parses query string time parameters: `minutes`, `hours`, `days`.
/// Returns `(start_ts, end_ts, limit)`.
fn parse_time_params(req: &Request) -> (u64, u64, usize, Option<u64>) {
    let now = timestamp_millis() as u64;
    let default_duration_ms = 5 * 60 * 1000u64; // 5 minutes

    let duration_ms = req
        .query::<u64>("minutes")
        .map(|m| m * 60 * 1000)
        .or_else(|| req.query::<u64>("hours").map(|h| h * 60 * 60 * 1000))
        .or_else(|| req.query::<u64>("days").map(|d| d * 24 * 60 * 60 * 1000))
        .unwrap_or(default_duration_ms);

    let start_ts = now.saturating_sub(duration_ms);
    let limit = req.query::<usize>("limit").unwrap_or(1000);
    let merge_window = req.query::<u64>("merge_window");

    (start_ts, now, limit, merge_window)
}

/// Sends a history query to a single remote node via gRPC and returns the
/// result. Returns empty data on error.
async fn query_history_remote(
    scx: &ServerContext,
    message_type: MessageType,
    node_id: NodeId,
    msg: Message<'_>,
) -> HistoryData {
    let grpc_clients = scx.extends.shared().await.get_grpc_clients();
    if let Some(client) = grpc_clients.get(&node_id).map(|(_, c)| c.clone()) {
        match msg.encode() {
            Ok(encoded) => {
                if let Ok(GrpcMessageReply::Data(reply_data)) = MessageSender::new_quick(
                    client,
                    message_type,
                    GrpcMessage::Data(encoded),
                    Some(Duration::from_secs(10)),
                )
                .send()
                .await
                {
                    // 跨节点传输的是 JSON 字符串化的 HistoryData
                    // （postcard 无法反序列化 serde_json::Value）
                    if let Ok(MessageReply::StatsHistoryReply(s)) | Ok(MessageReply::MetricsHistoryReply(s)) =
                        MessageReply::decode(&reply_data)
                    {
                        if let Ok(d) = serde_json::from_str::<HistoryData>(&s) {
                            return d;
                        }
                    }
                }
            }
            Err(e) => log::error!("encode history query error: {e}"),
        }
    }
    HistoryData { node: node_id, from: 0, to: 0, count: 0, data: vec![] }
}

/// Query parameters shared by stats/metrics history lookups.
#[derive(Copy, Clone)]
struct HistoryQueryParams {
    start_ts: u64,
    end_ts: u64,
    limit: usize,
    interval_ms: u64,
    merge_window: Option<u64>,
}

/// Queries all known nodes (local + remote via gRPC broadcast) and returns
/// a map of `node_id → HistoryData`.
///
/// The caller must provide a `msg_encoded` (a pre-encoded `Message` for the
/// remote side) and a `extract_fn` that picks the correct `HistoryData`
/// variant from a decoded `MessageReply`.
async fn query_history_all_nodes(
    scx: &ServerContext,
    message_type: MessageType,
    cache: &HistoryCache,
    params: &HistoryQueryParams,
    msg_encoded: Vec<u8>,
    local_node_id: NodeId,
) -> HashMap<NodeId, HistoryData> {
    let mut nodes = HashMap::default();

    // 1. Query local storage.
    let local_data = query_history_local(
        cache,
        local_node_id,
        params.start_ts,
        params.end_ts,
        params.limit,
        params.interval_ms,
        params.merge_window,
    )
    .await;
    nodes.insert(local_node_id, local_data);

    // 2. Broadcast to all remote nodes.
    let grpc_clients = scx.extends.shared().await.get_grpc_clients();
    if !grpc_clients.is_empty() {
        for reply in MessageBroadcaster::new_quick(
            grpc_clients,
            message_type,
            GrpcMessage::Data(msg_encoded),
            Some(Duration::from_secs(10)),
        )
        .join_all()
        .await
        {
            match reply {
                (id, Ok(GrpcMessageReply::Data(data))) => {
                    if let Ok(reply_msg) = MessageReply::decode(&data) {
                        match reply_msg {
                            // 跨节点传输的是 JSON 字符串化的 HistoryData
                            MessageReply::StatsHistoryReply(s) | MessageReply::MetricsHistoryReply(s) => {
                                if let Ok(d) = serde_json::from_str::<HistoryData>(&s) {
                                    nodes.insert(id, d);
                                } else {
                                    log::warn!("invalid history data from node({id})");
                                }
                            }
                            _ => {
                                log::info!("unexpected history reply from node({id})");
                            }
                        }
                    }
                }
                (id, Ok(reply)) => {
                    log::info!("unexpected grpc reply from node({id}): {reply:?}");
                }
                (id, Err(e)) => {
                    log::warn!("history query from node({id}) error: {e}");
                }
            }
        }
    }

    nodes
}

/// Aggregates per-node history data into a single time series.
///
/// Numeric fields are summed across nodes at each timestamp, except for
/// cluster-wide fields that all nodes report identically (the shared topic /
/// route tables): those take the maximum instead of a sum.
/// Returns `(data_points, node_count)`.
fn aggregate_history_data(nodes_data: &HashMap<NodeId, HistoryData>) -> (Vec<serde_json::Value>, usize) {
    let node_count = nodes_data.len();
    if node_count == 0 {
        return (vec![], 0);
    }

    // Cluster-shared quantities: every node reports the same value for the
    // shared topic/route tables, so summing would over-count (N nodes → N×).
    fn take_max(key: &str) -> bool {
        matches!(key, "topics.count" | "topics.max" | "routes.count" | "routes.max")
    }

    // Group values by timestamp.
    let mut grouped: HashMap<u64, Vec<&serde_json::Value>> = HashMap::default();
    for data in nodes_data.values() {
        for point in &data.data {
            if let Some(ts) = point.get("ts").and_then(|v| v.as_u64()) {
                grouped.entry(ts).or_default().push(point);
            }
        }
    }

    // For each unique timestamp, merge all numeric fields.
    let mut result: Vec<(u64, serde_json::Value)> = Vec::with_capacity(grouped.len());
    for (ts, points) in grouped {
        let mut merged = serde_json::Map::new();
        merged.insert("ts".into(), json!(ts));

        for point in points {
            if let Some(obj) = point.as_object() {
                for (k, v) in obj {
                    if k == "ts" {
                        continue;
                    }
                    match v {
                        serde_json::Value::Number(n) => {
                            let val = n.as_f64().unwrap_or(0.0);
                            let entry = merged.entry(k.clone()).or_insert_with(|| json!(0.0_f64));
                            if let Some(existing) = entry.as_f64() {
                                *entry = if take_max(k) {
                                    json!(existing.max(val))
                                } else {
                                    json!(existing + val)
                                };
                            }
                        }
                        _ => {
                            // Non-numeric fields (strings, arrays) take the first value.
                            merged.entry(k.clone()).or_insert_with(|| v.clone());
                        }
                    }
                }
            }
        }
        result.push((ts, serde_json::Value::Object(merged)));
    }

    // Sort descending by timestamp.
    result.sort_by_key(|b| std::cmp::Reverse(b.0));

    let data: Vec<serde_json::Value> = result.into_iter().map(|(_, v)| v).collect();
    (data, node_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use salvo::test::{ResponseExt, TestClient};
    use tokio::sync::RwLock;

    use crate::config::PluginConfig;

    fn test_cfg() -> PluginConfig {
        serde_json::from_value(json!({"http_bearer_token": null})).expect("PluginConfig defaults")
    }

    async fn test_service() -> Service {
        let scx = ServerContext::new().node_id(1).busy_check_enable(false).build().await;
        let cfg = Arc::new(RwLock::new(test_cfg()));
        let router = route(scx, cfg, None, prome::Monitor::new(), None);
        Service::new(router)
    }

    #[tokio::test]
    async fn plugins_cluster_list_is_node_grouped_array() {
        let svc = test_service().await;
        let mut resp = TestClient::get("http://127.0.0.1:0/api/v1/plugins").send(&svc).await;
        assert_eq!(resp.status_code, Some(StatusCode::OK));
        assert_eq!(resp.headers().get("X-Row-Count").map(|v| v.as_bytes()), Some(&b"1"[..]));
        assert_eq!(resp.headers().get("X-Truncated").map(|v| v.as_bytes()), Some(&b"false"[..]));
        let body: serde_json::Value = resp.take_json().await.unwrap();
        assert!(body.is_array(), "GET /plugins must remain a bare array");
        assert_eq!(body[0]["node"], 1);
        assert!(body[0]["plugins"].is_array());
        assert_eq!(body[0]["ok"], true);
    }

    #[tokio::test]
    async fn node_plugin_missing_returns_json_404_not_null() {
        let svc = test_service().await;
        let mut resp = TestClient::get("http://127.0.0.1:0/api/v1/plugins/1/no-such-plugin").send(&svc).await;
        assert_eq!(resp.status_code, Some(StatusCode::NOT_FOUND));
        let body: serde_json::Value = resp.take_json().await.unwrap();
        assert_eq!(body["code"], 404);
        assert!(body["message"].as_str().unwrap().contains("plugin"));
        assert!(body["request_id"].is_string());
        assert!(body.get("name").is_none(), "error body is code+message+request_id");
    }

    #[tokio::test]
    async fn plugin_config_missing_returns_json_error() {
        let svc = test_service().await;
        let mut resp =
            TestClient::get("http://127.0.0.1:0/api/v1/plugins/1/no-such-plugin/config").send(&svc).await;
        assert_eq!(resp.status_code, Some(StatusCode::NOT_FOUND));
        let body: serde_json::Value = resp.take_json().await.unwrap();
        assert_eq!(body["code"], 404);
        assert!(body["message"].is_string());
    }

    #[tokio::test]
    async fn clients_list_keeps_bare_array_and_adds_headers() {
        let svc = test_service().await;
        let mut resp = TestClient::get("http://127.0.0.1:0/api/v1/clients?_limit=10").send(&svc).await;
        assert_eq!(resp.status_code, Some(StatusCode::OK));
        assert_eq!(resp.headers().get("X-Row-Count").map(|v| v.as_bytes()), Some(&b"0"[..]));
        assert_eq!(resp.headers().get("X-Truncated").map(|v| v.as_bytes()), Some(&b"false"[..]));
        let body: serde_json::Value = resp.take_json().await.unwrap();
        assert_eq!(body, json!([]));
    }

    #[tokio::test]
    async fn delete_retains_without_topic_is_json_400() {
        let svc = test_service().await;
        let mut resp = TestClient::delete("http://127.0.0.1:0/api/v1/retains").send(&svc).await;
        assert_eq!(resp.status_code, Some(StatusCode::BAD_REQUEST));
        let body: serde_json::Value = resp.take_json().await.unwrap();
        assert_eq!(body["code"], 400);
        assert!(body["message"].as_str().unwrap().contains("topic"));
    }

    #[tokio::test]
    async fn bearer_required_returns_json_401() {
        let scx = ServerContext::new().node_id(1).busy_check_enable(false).build().await;
        let cfg = Arc::new(RwLock::new(test_cfg()));
        let router = route(scx, cfg, Some("secret".into()), prome::Monitor::new(), None);
        let svc = Service::new(router);
        let mut resp = TestClient::get("http://127.0.0.1:0/api/v1/plugins").send(&svc).await;
        assert_eq!(resp.status_code, Some(StatusCode::UNAUTHORIZED));
        let body: serde_json::Value = resp.take_json().await.unwrap();
        assert_eq!(body["code"], 401);
        assert_eq!(body["message"], "unauthorized");
        assert!(body["request_id"].is_string());
    }

    #[tokio::test]
    async fn openapi_json_is_served_and_is_openapi3() {
        let svc = test_service().await;
        let mut resp = TestClient::get("http://127.0.0.1:0/api/v1/openapi.json").send(&svc).await;
        assert_eq!(resp.status_code, Some(StatusCode::OK));
        let ct = resp.headers().get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("");
        assert!(ct.contains("json"), "content-type={ct}");
        let body: serde_json::Value = resp.take_json().await.unwrap();
        assert!(body["openapi"].as_str().unwrap().starts_with("3."));
        assert!(body["paths"]["/api/v1/clients"].is_object());
        assert!(body["components"]["schemas"]["Error"]["properties"]["request_id"].is_object());
        assert!(body["components"]["schemas"]["Page"]["properties"]["items"].is_object());
    }

    #[tokio::test]
    async fn docs_page_is_html() {
        let svc = test_service().await;
        let mut resp = TestClient::get("http://127.0.0.1:0/api/v1/docs").send(&svc).await;
        assert_eq!(resp.status_code, Some(StatusCode::OK));
        let body = resp.take_string().await.unwrap();
        assert!(body.contains("swagger") || body.contains("openapi.json"), "{body}");
    }

    #[tokio::test]
    async fn clients_format_page_wraps_array() {
        let svc = test_service().await;
        let mut resp =
            TestClient::get("http://127.0.0.1:0/api/v1/clients?_limit=10&format=page").send(&svc).await;
        assert_eq!(resp.status_code, Some(StatusCode::OK));
        assert_eq!(resp.headers().get("X-Row-Count").map(|v| v.as_bytes()), Some(&b"0"[..]));
        let body: serde_json::Value = resp.take_json().await.unwrap();
        assert!(body.is_object(), "format=page must be an object");
        assert_eq!(body["items"], json!([]));
        assert_eq!(body["offset"], 0);
        assert_eq!(body["limit"], 10);
        assert_eq!(body["truncated"], false);
        assert!(body.get("total").is_none());
    }

    #[tokio::test]
    async fn clients_default_stays_bare_array() {
        let svc = test_service().await;
        let mut resp = TestClient::get("http://127.0.0.1:0/api/v1/clients?_limit=10").send(&svc).await;
        let body: serde_json::Value = resp.take_json().await.unwrap();
        assert!(body.is_array());
    }

    #[tokio::test]
    async fn error_body_includes_request_id_header() {
        let svc = test_service().await;
        let mut resp = TestClient::get("http://127.0.0.1:0/api/v1/plugins/1/no-such-plugin")
            .add_header("x-request-id", "req-p2-test", true)
            .send(&svc)
            .await;
        assert_eq!(resp.status_code, Some(StatusCode::NOT_FOUND));
        assert_eq!(resp.headers().get("X-Request-Id").map(|v| v.as_bytes()), Some(&b"req-p2-test"[..]));
        let body: serde_json::Value = resp.take_json().await.unwrap();
        assert_eq!(body["code"], 404);
        assert_eq!(body["request_id"], "req-p2-test");
        assert!(body["message"].is_string());
    }

    #[tokio::test]
    async fn features_summary_has_enabled_and_structured_nodes() {
        let svc = test_service().await;
        let mut resp = TestClient::get("http://127.0.0.1:0/api/v1/features").send(&svc).await;
        assert_eq!(resp.status_code, Some(StatusCode::OK));
        let body: serde_json::Value = resp.take_json().await.unwrap();
        assert!(body["consistent"].is_boolean());
        assert_eq!(body["failed_count"], 0);
        assert_eq!(body["partial"], false);
        assert!(body["enabled"].is_object());
        for key in [
            "retain",
            "message_storage",
            "session_storage",
            "delayed",
            "shared_subscription",
            "auto_subscription",
        ] {
            assert!(body["enabled"][key].is_boolean(), "missing enabled.{key}");
        }
        assert!(body["nodes"].is_array());
        assert_eq!(body["nodes"][0]["ok"], true);
        assert!(body["nodes"][0]["features"].is_object());
        assert!(body["nodes"][0]["node_id"].is_number());
    }

    #[tokio::test]
    async fn stats_cluster_items_are_objects_with_ok() {
        let svc = test_service().await;
        let mut resp = TestClient::get("http://127.0.0.1:0/api/v1/stats").send(&svc).await;
        assert_eq!(resp.status_code, Some(StatusCode::OK));
        let body: serde_json::Value = resp.take_json().await.unwrap();
        assert!(body.is_array());
        assert_eq!(body[0]["ok"], true);
        assert!(body[0]["node"]["id"].is_number());
        assert!(body[0]["stats"].is_object());
        assert!(!body[0].is_string(), "partial failure must not be a bare string");
    }

    fn session_cookie(resp: &Response) -> String {
        resp.headers()
            .get_all("set-cookie")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .find_map(|v| {
                v.split(';').next().filter(|p| p.starts_with("ferromq_session=")).map(|s| s.to_string())
            })
            .expect("ferromq_session cookie")
    }

    fn dashboard_cfg() -> PluginConfig {
        let mut cfg = test_cfg();
        cfg.dashboard_admin_username = "admin".into();
        cfg.dashboard_admin_password = Some("admin-secret".into());
        cfg.dashboard_viewer_username = Some("viewer".into());
        cfg.dashboard_viewer_password = Some("viewer-secret".into());
        cfg
    }

    async fn dashboard_service(
        cfg: PluginConfig,
        token: Option<String>,
    ) -> (Service, Arc<crate::auth::AuthState>) {
        let scx = ServerContext::new().node_id(1).busy_check_enable(false).build().await;
        let state = Arc::new(crate::auth::AuthState::new(token.clone()));
        let cfg = Arc::new(RwLock::new(cfg));
        let router = route_with_auth(scx, cfg, state.clone(), prome::Monitor::new(), None);
        (Service::new(router), state)
    }

    async fn login(svc: &Service, username: &str, password: &str) -> (Response, serde_json::Value) {
        let mut resp = TestClient::post("http://127.0.0.1:0/api/v1/auth/login")
            .json(&json!({"username": username, "password": password}))
            .send(svc)
            .await;
        let body: serde_json::Value = resp.take_json().await.unwrap_or(json!({}));
        (resp, body)
    }

    #[tokio::test]
    async fn login_logout_me_and_change_password() {
        let (svc, _) = dashboard_service(dashboard_cfg(), None).await;

        let (resp, body) = login(&svc, "admin", "admin-secret").await;
        assert_eq!(resp.status_code, Some(StatusCode::OK), "{body}");
        assert_eq!(body["username"], "admin");
        assert_eq!(body["role"], "admin");
        assert_eq!(body["auth"], "session");
        assert!(body["expires_in"].as_u64().unwrap_or(0) > 0);
        let cookie = session_cookie(&resp);

        let mut me = TestClient::get("http://127.0.0.1:0/api/v1/auth/me")
            .add_header("cookie", &cookie, true)
            .send(&svc)
            .await;
        assert_eq!(me.status_code, Some(StatusCode::OK));
        let me_body: serde_json::Value = me.take_json().await.unwrap();
        assert_eq!(me_body["username"], "admin");
        assert_eq!(me_body["role"], "admin");
        assert_eq!(me_body["auth"], "session");

        let mut changed = TestClient::post("http://127.0.0.1:0/api/v1/auth/change-password")
            .add_header("cookie", &cookie, true)
            .json(&json!({"old_password": "admin-secret", "new_password": "admin-secret-2"}))
            .send(&svc)
            .await;
        assert_eq!(changed.status_code, Some(StatusCode::OK));
        let changed_body: serde_json::Value = changed.take_json().await.unwrap();
        assert_eq!(changed_body["ok"], true);

        let (bad, bad_body) = login(&svc, "admin", "admin-secret").await;
        assert_eq!(bad.status_code, Some(StatusCode::UNAUTHORIZED), "{bad_body}");

        let (ok2, ok2_body) = login(&svc, "admin", "admin-secret-2").await;
        assert_eq!(ok2.status_code, Some(StatusCode::OK), "{ok2_body}");

        let mut logout = TestClient::post("http://127.0.0.1:0/api/v1/auth/logout")
            .add_header("cookie", &cookie, true)
            .send(&svc)
            .await;
        assert_eq!(logout.status_code, Some(StatusCode::OK));
        let logout_body: serde_json::Value = logout.take_json().await.unwrap();
        assert_eq!(logout_body["ok"], true);

        let me_after = TestClient::get("http://127.0.0.1:0/api/v1/auth/me")
            .add_header("cookie", &cookie, true)
            .send(&svc)
            .await;
        assert_eq!(me_after.status_code, Some(StatusCode::UNAUTHORIZED));
    }

    #[tokio::test]
    async fn login_rejects_bad_password_and_init_is_one_time() {
        let (svc, _) = dashboard_service(dashboard_cfg(), None).await;
        let (resp, body) = login(&svc, "admin", "wrong-password").await;
        assert_eq!(resp.status_code, Some(StatusCode::UNAUTHORIZED), "{body}");
        assert_eq!(body["code"], 401);

        let mut init = TestClient::post("http://127.0.0.1:0/api/v1/auth/init").send(&svc).await;
        assert_eq!(init.status_code, Some(StatusCode::OK));
        let init_body: serde_json::Value = init.take_json().await.unwrap();
        assert_eq!(init_body["username"], "admin");
        assert_eq!(init_body["created"], true);

        let again = TestClient::post("http://127.0.0.1:0/api/v1/auth/init").send(&svc).await;
        assert_eq!(again.status_code, Some(StatusCode::CONFLICT));
    }

    #[tokio::test]
    async fn me_anonymous_when_auth_disabled() {
        let svc = test_service().await;
        let mut resp = TestClient::get("http://127.0.0.1:0/api/v1/auth/me").send(&svc).await;
        assert_eq!(resp.status_code, Some(StatusCode::OK));
        let body: serde_json::Value = resp.take_json().await.unwrap();
        assert_eq!(body["username"], "anonymous");
        assert_eq!(body["role"], "admin");
        assert_eq!(body["auth"], "anonymous");
    }

    #[tokio::test]
    async fn bearer_still_works_as_operator() {
        let (svc, _) = dashboard_service(dashboard_cfg(), Some("secret".into())).await;
        let denied = TestClient::get("http://127.0.0.1:0/api/v1/plugins").send(&svc).await;
        assert_eq!(denied.status_code, Some(StatusCode::UNAUTHORIZED));

        let mut ok = TestClient::get("http://127.0.0.1:0/api/v1/auth/me")
            .add_header("authorization", "Bearer secret", true)
            .send(&svc)
            .await;
        assert_eq!(ok.status_code, Some(StatusCode::OK));
        let body: serde_json::Value = ok.take_json().await.unwrap();
        assert_eq!(body["username"], "operator");
        assert_eq!(body["role"], "admin");
        assert_eq!(body["auth"], "bearer");
    }

    #[tokio::test]
    async fn viewer_is_denied_kick_publish_and_plugin_load() {
        let (svc, state) = dashboard_service(dashboard_cfg(), None).await;
        state.insert_user("viewer", "viewer-secret", crate::auth::Role::Viewer).await.expect("viewer");
        let (resp, body) = login(&svc, "viewer", "viewer-secret").await;
        assert_eq!(resp.status_code, Some(StatusCode::OK), "{body}");
        assert_eq!(body["role"], "viewer");
        let cookie = session_cookie(&resp);

        let mut kick = TestClient::delete("http://127.0.0.1:0/api/v1/clients/no-such-client")
            .add_header("cookie", &cookie, true)
            .send(&svc)
            .await;
        assert_eq!(kick.status_code, Some(StatusCode::FORBIDDEN));
        let kick_body: serde_json::Value = kick.take_json().await.unwrap();
        assert_eq!(kick_body["code"], 403);
        assert_eq!(kick_body["message"], "forbidden");
        assert_eq!(kick_body["details"]["required_role"], "admin");

        let pub_resp = TestClient::post("http://127.0.0.1:0/api/v1/mqtt/publish")
            .add_header("cookie", &cookie, true)
            .json(&json!({"topic": "t", "payload": "x"}))
            .send(&svc)
            .await;
        assert_eq!(pub_resp.status_code, Some(StatusCode::FORBIDDEN));

        let load = TestClient::put("http://127.0.0.1:0/api/v1/plugins/1/ferromq-http-api/load")
            .add_header("cookie", &cookie, true)
            .send(&svc)
            .await;
        assert_eq!(load.status_code, Some(StatusCode::FORBIDDEN));

        let plugins = TestClient::get("http://127.0.0.1:0/api/v1/plugins")
            .add_header("cookie", &cookie, true)
            .send(&svc)
            .await;
        assert_eq!(plugins.status_code, Some(StatusCode::OK));
    }

    #[tokio::test]
    async fn admin_and_bearer_pass_write_rbac() {
        let (svc, _) = dashboard_service(dashboard_cfg(), Some("secret".into())).await;
        let (resp, body) = login(&svc, "admin", "admin-secret").await;
        assert_eq!(resp.status_code, Some(StatusCode::OK), "{body}");
        let cookie = session_cookie(&resp);

        let kick = TestClient::delete("http://127.0.0.1:0/api/v1/clients/no-such-client")
            .add_header("cookie", &cookie, true)
            .send(&svc)
            .await;
        assert_ne!(kick.status_code, Some(StatusCode::FORBIDDEN));
        assert_eq!(kick.status_code, Some(StatusCode::NOT_FOUND));

        let kick_bearer = TestClient::delete("http://127.0.0.1:0/api/v1/clients/no-such-client")
            .add_header("authorization", "Bearer secret", true)
            .send(&svc)
            .await;
        assert_ne!(kick_bearer.status_code, Some(StatusCode::FORBIDDEN));
        assert_eq!(kick_bearer.status_code, Some(StatusCode::NOT_FOUND));
    }

    #[tokio::test]
    async fn login_rate_limit_returns_429() {
        let mut cfg = dashboard_cfg();
        cfg.dashboard_login_rate_limit = 3;
        let (svc, _) = dashboard_service(cfg, None).await;
        for _ in 0..3 {
            let (resp, _) = login(&svc, "admin", "wrong").await;
            assert_eq!(resp.status_code, Some(StatusCode::UNAUTHORIZED));
        }
        let (resp, body) = login(&svc, "admin", "wrong").await;
        assert_eq!(resp.status_code, Some(StatusCode::TOO_MANY_REQUESTS), "{body}");
        assert_eq!(body["code"], 429);
    }

    #[tokio::test]
    async fn health_and_login_remain_public_when_bearer_required() {
        let (svc, _) = dashboard_service(dashboard_cfg(), Some("secret".into())).await;
        let health = TestClient::get("http://127.0.0.1:0/api/v1/health/check").send(&svc).await;
        assert_eq!(health.status_code, Some(StatusCode::OK));
        let openapi = TestClient::get("http://127.0.0.1:0/api/v1/openapi.json").send(&svc).await;
        assert_eq!(openapi.status_code, Some(StatusCode::OK));
        let (resp, body) = login(&svc, "admin", "admin-secret").await;
        assert_eq!(resp.status_code, Some(StatusCode::OK), "{body}");
    }
}
