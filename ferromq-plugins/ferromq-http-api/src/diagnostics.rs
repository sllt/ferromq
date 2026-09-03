//! P6 diagnostics and cluster-ops APIs.
//!
//! Honest surface: expose what FerroMQ can actually derive, and mark gaps
//! with `available: false` instead of inventing collectors.
//!
//! * **Alarms** — thin in-memory bus fed from health / features / unreachable
//!   peers. Acknowledge is supported. There is no native alarm plugin.
//! * **Logs / Trace / Slow Sub** — no collector plugins; structured gap.
//! * **Topic metrics** — subscriber counts from the router (`/routes`) plus
//!   pointers at `/stats`, `/metrics`, and `$SYS` when `ferromq-sys-topic`
//!   is loaded. Per-topic rates are not collected.
//! * **Cluster** — read-only topology from loaded cluster plugins + gRPC
//!   peers. `leave` is forwarded to `ferromq-cluster-raft` via `Plugin::send`
//!   when that plugin is active. Runtime `join` is not exposed (Raft join
//!   happens at plugin init). Broadcast mode and standalone are 501.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use salvo::http::StatusCode;
use salvo::prelude::*;
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::RwLock;

use ferromq::context::ServerContext;
use ferromq::grpc::MessageType;
use ferromq::types::NodeId;

use super::api::{build_features, get_features_all, get_nodes_all, get_scx_cfg};
use super::audit;
use super::auth::identity_from_depot;
use super::response::{
    cluster_node_failure, render_api_error, render_api_error_with, render_list, render_not_found, ListPaging,
};

/// Depot key for [`AlarmBus`].
pub(crate) const ALARM_BUS: &str = "ALARM_BUS";

pub(crate) const PLUGIN_CLUSTER_RAFT: &str = "ferromq-cluster-raft";
pub(crate) const PLUGIN_CLUSTER_BROADCAST: &str = "ferromq-cluster-broadcast";
pub(crate) const PLUGIN_SYS_TOPIC: &str = "ferromq-sys-topic";

const DEFAULT_MAX_HISTORY: usize = 1_000;

// ── Alarm bus ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Alarm {
    pub id: String,
    pub name: String,
    pub level: String,
    pub node_id: Option<NodeId>,
    pub message: String,
    pub source: String,
    pub activated_at: i64,
    pub acknowledged: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acknowledged_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acknowledged_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleared_at: Option<i64>,
}

#[derive(Debug, Clone)]
struct AlarmCondition {
    id: String,
    name: String,
    level: String,
    node_id: Option<NodeId>,
    message: String,
    source: String,
}

/// Process-local current/history alarm store. Refreshed from live health
/// and feature queries; never invents external alarm sources.
pub(crate) struct AlarmBus {
    current: RwLock<HashMap<String, Alarm>>,
    history: RwLock<VecDeque<Alarm>>,
    max_history: usize,
}

impl AlarmBus {
    pub(crate) fn new(max_history: usize) -> Self {
        Self {
            current: RwLock::new(HashMap::new()),
            history: RwLock::new(VecDeque::new()),
            max_history: max_history.max(1),
        }
    }

    pub(crate) fn default_bus() -> Self {
        Self::new(DEFAULT_MAX_HISTORY)
    }

    async fn refresh(&self, conditions: Vec<AlarmCondition>, now: i64) {
        let mut current = self.current.write().await;
        let mut history = self.history.write().await;
        let incoming: HashMap<String, AlarmCondition> =
            conditions.into_iter().map(|c| (c.id.clone(), c)).collect();

        let stale: Vec<String> = current.keys().filter(|k| !incoming.contains_key(*k)).cloned().collect();
        for id in stale {
            if let Some(mut alarm) = current.remove(&id) {
                alarm.cleared_at = Some(now);
                history.push_front(alarm);
            }
        }
        while history.len() > self.max_history {
            history.pop_back();
        }

        for (id, cond) in incoming {
            current.entry(id).or_insert_with(|| Alarm {
                id: cond.id,
                name: cond.name,
                level: cond.level,
                node_id: cond.node_id,
                message: cond.message,
                source: cond.source,
                activated_at: now,
                acknowledged: false,
                acknowledged_at: None,
                acknowledged_by: None,
                cleared_at: None,
            });
        }
    }

    async fn current(&self) -> Vec<Alarm> {
        let mut items: Vec<Alarm> = self.current.read().await.values().cloned().collect();
        items.sort_by(|a, b| b.activated_at.cmp(&a.activated_at).then_with(|| a.id.cmp(&b.id)));
        items
    }

    async fn history(&self) -> Vec<Alarm> {
        self.history.read().await.iter().cloned().collect()
    }

    async fn acknowledge(&self, id: &str, by: &str, now: i64) -> Option<Alarm> {
        let mut current = self.current.write().await;
        let alarm = current.get_mut(id)?;
        alarm.acknowledged = true;
        alarm.acknowledged_at = Some(now);
        alarm.acknowledged_by = Some(by.to_string());
        Some(alarm.clone())
    }
}

fn now_millis() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn alarm_bus(depot: &Depot) -> Option<Arc<AlarmBus>> {
    depot.get::<Arc<AlarmBus>>(ALARM_BUS).ok().cloned()
}

fn feature_conflict_names(successes: &[(NodeId, super::types::Features)]) -> Vec<&'static str> {
    let mut names = Vec::new();
    let retain = (successes.iter().any(|(_, f)| f.retain), successes.iter().any(|(_, f)| !f.retain));
    let msg_store =
        (successes.iter().any(|(_, f)| f.message_storage), successes.iter().any(|(_, f)| !f.message_storage));
    let sess_store =
        (successes.iter().any(|(_, f)| f.session_storage), successes.iter().any(|(_, f)| !f.session_storage));
    let delayed = (successes.iter().any(|(_, f)| f.delayed), successes.iter().any(|(_, f)| !f.delayed));
    let shared = (
        successes.iter().any(|(_, f)| f.shared_subscription),
        successes.iter().any(|(_, f)| !f.shared_subscription),
    );
    let auto = (
        successes.iter().any(|(_, f)| f.auto_subscription),
        successes.iter().any(|(_, f)| !f.auto_subscription),
    );
    for (name, (saw_true, saw_false)) in [
        ("retain", retain),
        ("message_storage", msg_store),
        ("session_storage", sess_store),
        ("delayed", delayed),
        ("shared_subscription", shared),
        ("auto_subscription", auto),
    ] {
        if saw_true && saw_false {
            names.push(name);
        }
    }
    names
}

async fn collect_alarm_conditions(scx: &ServerContext, message_type: MessageType) -> Vec<AlarmCondition> {
    let mut out = Vec::new();

    match scx.extends.shared().await.check_health().await {
        Ok(health) => {
            if !health.running {
                out.push(AlarmCondition {
                    id: "cluster_unhealthy".into(),
                    name: "cluster_unhealthy".into(),
                    level: "critical".into(),
                    node_id: None,
                    message: health.descr.clone().unwrap_or_else(|| "cluster health.running is false".into()),
                    source: "health".into(),
                });
            }
            for node in &health.nodes {
                if !node.is_running() {
                    let nid = node.node_id;
                    let msg = node.descr.clone().filter(|s| !s.is_empty()).unwrap_or_else(|| {
                        format!("node {nid} is not running (leader_id={:?})", node.leader_id)
                    });
                    out.push(AlarmCondition {
                        id: format!("node_unhealthy:{nid}"),
                        name: "node_unhealthy".into(),
                        level: "critical".into(),
                        node_id: Some(nid),
                        message: msg,
                        source: "health".into(),
                    });
                }
            }
        }
        Err(e) => {
            out.push(AlarmCondition {
                id: "health_query_failed".into(),
                name: "health_query_failed".into(),
                level: "warning".into(),
                node_id: Some(scx.node.id()),
                message: e.to_string(),
                source: "health".into(),
            });
        }
    }

    match get_nodes_all(scx, message_type).await {
        Ok(nodes) => {
            for item in nodes {
                if let Err((id, e)) = item {
                    out.push(AlarmCondition {
                        id: format!("node_unreachable:{id}"),
                        name: "node_unreachable".into(),
                        level: "critical".into(),
                        node_id: Some(id),
                        message: e.to_string(),
                        source: "cluster".into(),
                    });
                }
            }
        }
        Err(e) => {
            out.push(AlarmCondition {
                id: "nodes_query_failed".into(),
                name: "nodes_query_failed".into(),
                level: "warning".into(),
                node_id: Some(scx.node.id()),
                message: e.to_string(),
                source: "cluster".into(),
            });
        }
    }

    match get_features_all(scx, message_type).await {
        Ok(nodes) => {
            let mut failed = 0usize;
            let mut successes: Vec<(NodeId, super::types::Features)> = Vec::new();
            for item in nodes {
                if item.ok {
                    if let Some(f) = item.features {
                        successes.push((item.node_id, f));
                    }
                } else {
                    failed += 1;
                    out.push(AlarmCondition {
                        id: format!("features_partial:{}", item.node_id),
                        name: "features_partial".into(),
                        level: "warning".into(),
                        node_id: Some(item.node_id),
                        message: item.error.unwrap_or_else(|| "feature query failed".into()),
                        source: "features".into(),
                    });
                }
            }
            if failed > 0 {
                out.push(AlarmCondition {
                    id: "features_partial_cluster".into(),
                    name: "features_partial".into(),
                    level: "warning".into(),
                    node_id: None,
                    message: format!("{failed} node(s) failed to report features"),
                    source: "features".into(),
                });
            }
            if successes.len() >= 2 {
                let conflict_names = feature_conflict_names(&successes);
                if !conflict_names.is_empty() {
                    out.push(AlarmCondition {
                        id: "features_inconsistent".into(),
                        name: "features_inconsistent".into(),
                        level: "warning".into(),
                        node_id: None,
                        message: format!("feature flags differ across nodes: {}", conflict_names.join(",")),
                        source: "features".into(),
                    });
                }
            }
        }
        Err(e) => {
            out.push(AlarmCondition {
                id: "features_query_failed".into(),
                name: "features_query_failed".into(),
                level: "warning".into(),
                node_id: Some(scx.node.id()),
                message: e.to_string(),
                source: "features".into(),
            });
        }
    }

    out
}

async fn refresh_alarms(
    depot: &Depot,
    scx: &ServerContext,
    message_type: MessageType,
) -> Option<Arc<AlarmBus>> {
    let bus = alarm_bus(depot)?;
    let conditions = collect_alarm_conditions(scx, message_type).await;
    bus.refresh(conditions, now_millis()).await;
    Some(bus)
}

/// `GET /api/v1/alarms` — current alarms derived from health/features/peers.
#[handler]
pub(crate) async fn alarms_current(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, message_type, max_row_limit) = {
        let (scx, cfg) = get_scx_cfg(depot)?;
        let cfg = cfg.read().await;
        (scx.clone(), cfg.message_type, cfg.max_row_limit)
    };
    let Some(bus) = refresh_alarms(depot, &scx, message_type).await else {
        render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "alarm bus not configured");
        return Ok(());
    };
    let items = bus.current().await;
    let total = items.len();
    let requested = req.query::<usize>("_limit").unwrap_or(0);
    let paging = ListPaging::from_request(req, requested, max_row_limit);
    let (page, truncated) = paging.apply(items);
    if super::response::wants_page_format(req) {
        render_list(req, res, page, paging, truncated, Some(total));
    } else {
        res.render(Json(json!({
            "available": true,
            "source": "derived",
            "note": "In-memory bus fed from GET /health/check, GET /features, and node reachability. FerroMQ has no native alarm plugin or persistence.",
            "items": page,
        })));
    }
    Ok(())
}

/// `GET /api/v1/alarms/history` — cleared alarms (process memory only).
#[handler]
pub(crate) async fn alarms_history(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, message_type, max_row_limit) = {
        let (scx, cfg) = get_scx_cfg(depot)?;
        let cfg = cfg.read().await;
        (scx.clone(), cfg.message_type, cfg.max_row_limit)
    };
    let Some(bus) = refresh_alarms(depot, &scx, message_type).await else {
        render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "alarm bus not configured");
        return Ok(());
    };
    let items = bus.history().await;
    let total = items.len();
    let requested = req.query::<usize>("_limit").unwrap_or(0);
    let paging = ListPaging::from_request(req, requested, max_row_limit);
    let (page, truncated) = paging.apply(items);
    if super::response::wants_page_format(req) {
        render_list(req, res, page, paging, truncated, Some(total));
    } else {
        res.render(Json(json!({
            "available": true,
            "source": "derived",
            "note": "Cleared alarms only. Lost on process restart.",
            "items": page,
        })));
    }
    Ok(())
}

/// `POST /api/v1/alarms/{id}/acknowledge` — operator+.
#[handler]
pub(crate) async fn alarms_acknowledge(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let Some(id) = req.param::<String>("id").filter(|s| !s.is_empty()) else {
        render_api_error(res, StatusCode::BAD_REQUEST, "missing alarm id");
        return Ok(());
    };
    let Some(bus) = alarm_bus(depot) else {
        render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "alarm bus not configured");
        return Ok(());
    };
    let who = identity_from_depot(depot).map(|i| i.username).unwrap_or_else(|| "anonymous".into());
    match bus.acknowledge(&id, &who, now_millis()).await {
        Some(alarm) => {
            audit::record(req, depot, "alarm_acknowledge", Some(id), true, Some(json!({"id": alarm.id})))
                .await;
            res.render(Json(json!({ "ok": true, "alarm": alarm })));
        }
        None => {
            audit::record(req, depot, "alarm_acknowledge", Some(id.clone()), false, None).await;
            render_not_found(res, format!("alarm {id} not found"));
        }
    }
    Ok(())
}

// ── Gaps: logs / trace / slow-sub ────────────────────────────────────────

fn gap_body(kind: &str, gap: &str, alternatives: Value) -> Value {
    json!({
        "available": false,
        "plugin": null,
        "kind": kind,
        "items": [],
        "gap": gap,
        "alternatives": alternatives,
    })
}

/// `GET /api/v1/logs` — no log collector.
#[handler]
pub(crate) async fn logs_get(_req: &mut Request, _depot: &mut Depot, res: &mut Response) {
    res.render(Json(gap_body(
        "logs",
        "FerroMQ has no log-query plugin. Process logs go to stderr / RUST_LOG / ferromq.toml [log].",
        json!([
            {"api": "GET /api/v1/broker/config/log", "how": "Read the current log.level / log.to / log.dir"},
            {"api": "PUT /api/v1/broker/config/log", "how": "Write log.level (restart_required; ferromqd is not hot-restarted)"}
        ]),
    )));
}

/// `GET /api/v1/trace` — no packet/trace plugin.
#[handler]
pub(crate) async fn trace_get(_req: &mut Request, _depot: &mut Depot, res: &mut Response) {
    res.render(Json(gap_body(
        "trace",
        "FerroMQ does not ship a client/packet trace plugin (no start/stop/dump API).",
        json!([
            {"how": "Use broker logs (RUST_LOG / log.level=debug|trace) on the node process"},
            {"api": "GET /api/v1/clients/{clientid}", "how": "Inspect a live session"}
        ]),
    )));
}

/// Write ops on gap endpoints stay 501.
#[handler]
pub(crate) async fn trace_write(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    audit::record(req, depot, "trace_write", None, false, Some(json!({"gap": true}))).await;
    render_api_error_with(
        res,
        StatusCode::NOT_IMPLEMENTED,
        "trace plugin is not available",
        Some(gap_body(
            "trace",
            "No packet-trace plugin; start/stop/dump are not implemented.",
            json!([{"how": "Use process logs"}]),
        )),
    );
    Ok(())
}

/// `GET /api/v1/slow-subs` — no slow-subscription plugin.
#[handler]
pub(crate) async fn slow_subs_get(_req: &mut Request, _depot: &mut Depot, res: &mut Response) {
    res.render(Json(gap_body(
        "slow_sub",
        "FerroMQ has no slow-subscription tracker (no per-subscriber latency histogram).",
        json!([
            {"api": "GET /api/v1/subscriptions", "how": "List current subscriptions"},
            {"api": "GET /api/v1/metrics", "how": "Broker-level counters only; not per-subscription latency"}
        ]),
    )));
}

// ── Topic metrics (route-derived, honest about missing rates) ────────────

/// `GET /api/v1/topic-metrics` — subscriber counts from the router.
#[handler]
pub(crate) async fn topic_metrics_get(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, max_row_limit) = {
        let (scx, cfg) = get_scx_cfg(depot)?;
        let max_row_limit = cfg.read().await.max_row_limit;
        (scx.clone(), max_row_limit)
    };
    let requested = req.query::<usize>("_limit").or_else(|| req.query::<usize>("limit")).unwrap_or(0);
    let paging = ListPaging::from_request(req, requested, max_row_limit);
    let routes = scx.extends.router().await.gets(paging.fetch_limit).await;

    let mut by_topic: HashMap<String, (usize, Vec<NodeId>)> = HashMap::new();
    for r in routes {
        let entry = by_topic.entry(r.topic.to_string()).or_insert_with(|| (0, Vec::new()));
        entry.0 += 1;
        if !entry.1.contains(&r.node_id) {
            entry.1.push(r.node_id);
        }
    }
    let mut items: Vec<Value> = by_topic
        .into_iter()
        .map(|(topic, (subscribers, mut node_ids))| {
            node_ids.sort_unstable();
            json!({
                "topic": topic,
                "subscribers": subscribers,
                "node_ids": node_ids,
            })
        })
        .collect();
    items.sort_by(|a, b| {
        let ta = a.get("topic").and_then(|v| v.as_str()).unwrap_or("");
        let tb = b.get("topic").and_then(|v| v.as_str()).unwrap_or("");
        ta.cmp(tb)
    });
    let truncated = paging.fetch_limit > 0 && items.len() >= paging.fetch_limit;
    let page: Vec<Value> = items.into_iter().skip(paging.offset).take(paging.page_size).collect();

    let sys_topic = plugin_status(&scx, PLUGIN_SYS_TOPIC).await;
    let local = build_features(&scx).await;
    res.render(Json(json!({
        "available": true,
        "kind": "route_derived",
        "note": "Subscriber counts come from the topic router (same data as GET /routes). Per-topic publish/deliver rates are not collected. Broker-level counters are on /stats and /metrics. $SYS topics exist only when ferromq-sys-topic is loaded.",
        "sys_topic": {
            "plugin": PLUGIN_SYS_TOPIC,
            "loaded": sys_topic.loaded,
            "active": sys_topic.active,
            "topics": if sys_topic.active {
                json!([
                    format!("$SYS/brokers/{}/stats", local.node_id),
                    format!("$SYS/brokers/{}/metrics", local.node_id)
                ])
            } else {
                json!([])
            },
        },
        "alternatives": [
            {"api": "GET /api/v1/routes", "how": "Raw topic → node routes"},
            {"api": "GET /api/v1/stats", "how": "Broker-level statistics"},
            {"api": "GET /api/v1/metrics", "how": "Broker-level metrics"}
        ],
        "items": page,
        "offset": paging.offset,
        "limit": paging.page_size,
        "truncated": truncated,
    })));
    Ok(())
}

// ── Cluster topology + membership ────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClusterMode {
    Standalone,
    Raft,
    Broadcast,
}

impl ClusterMode {
    fn as_str(self) -> &'static str {
        match self {
            ClusterMode::Standalone => "standalone",
            ClusterMode::Raft => "raft",
            ClusterMode::Broadcast => "broadcast",
        }
    }
}

struct PluginStatus {
    loaded: bool,
    active: bool,
}

async fn plugin_status(scx: &ServerContext, name: &str) -> PluginStatus {
    if let Some(entry) = scx.plugins.get(name) {
        PluginStatus { loaded: true, active: entry.active() }
    } else {
        PluginStatus { loaded: false, active: false }
    }
}

fn classify_cluster_plugin(name: &str) -> Option<ClusterMode> {
    if name.contains("cluster-raft") {
        Some(ClusterMode::Raft)
    } else if name.contains("cluster-broadcast") {
        Some(ClusterMode::Broadcast)
    } else {
        None
    }
}

async fn detect_cluster_plugin(scx: &ServerContext) -> (ClusterMode, Option<String>, bool) {
    for name in [PLUGIN_CLUSTER_RAFT, PLUGIN_CLUSTER_BROADCAST] {
        if let Some(entry) = scx.plugins.get(name) {
            if let Some(mode) = classify_cluster_plugin(name) {
                return (mode, Some(name.to_string()), entry.active());
            }
        }
    }
    for entry in scx.plugins.iter() {
        let name = entry.key().to_string();
        if let Some(mode) = classify_cluster_plugin(&name) {
            return (mode, Some(name), entry.active());
        }
    }
    (ClusterMode::Standalone, None, false)
}

/// Snapshot used by `/cluster` and attached onto `/brokers` + `/nodes`.
pub(crate) async fn cluster_snapshot(scx: &ServerContext) -> Value {
    let (mode, plugin_name, plugin_active) = detect_cluster_plugin(scx).await;
    let local_id = scx.node.id();
    let grpc = scx.extends.shared().await.get_grpc_clients();
    let mut peers: Vec<NodeId> = grpc.keys().copied().collect();
    peers.sort_unstable();

    let health = scx.extends.shared().await.health_status().await.ok();
    let leader_id = health.as_ref().and_then(|h| h.leader_id).filter(|id| *id > 0);

    let mut raft_status = Value::Null;
    if mode == ClusterMode::Raft {
        if let Some(name) = plugin_name.as_deref() {
            if plugin_active {
                match scx.plugins.send(name, json!({"op": "status"})).await {
                    Ok(v) => raft_status = v,
                    Err(e) => {
                        raft_status = json!({"ok": false, "error": e.to_string()});
                    }
                }
            }
        }
    }

    let leader_from_raft =
        raft_status.get("raft_status").and_then(|s| s.get("leader_id")).and_then(|v| v.as_u64());
    let leader_id = leader_id.or(leader_from_raft).filter(|id| *id > 0);

    let local_role = match mode {
        ClusterMode::Standalone => "standalone",
        ClusterMode::Broadcast => "member",
        ClusterMode::Raft => match leader_id {
            Some(id) if id == local_id => "leader",
            Some(_) => "follower",
            None => "unknown",
        },
    };

    let mut nodes = vec![json!({
        "ok": true,
        "node_id": local_id,
        "role": local_role,
        "reachable": true,
        "leader": leader_id == Some(local_id),
    })];
    for pid in &peers {
        nodes.push(json!({
            "ok": true,
            "node_id": pid,
            "role": match leader_id {
                Some(id) if id == *pid => "leader",
                Some(_) if mode == ClusterMode::Raft => "follower",
                _ if mode == ClusterMode::Broadcast => "member",
                _ => "peer",
            },
            "reachable": true,
            "leader": leader_id == Some(*pid),
        }));
    }

    let (join, leave, reason) = match (mode, plugin_active) {
        (ClusterMode::Raft, true) => (
            false,
            true,
            "ferromq-cluster-raft exposes Mailbox::leave via Plugin::send. Runtime Raft::join is not available (a node joins at plugin init from raft_peer_addrs / --raft-peer-addrs).",
        ),
        (ClusterMode::Raft, false) => (
            false,
            false,
            "ferromq-cluster-raft is registered but not active.",
        ),
        (ClusterMode::Broadcast, _) => (
            false,
            false,
            "ferromq-cluster-broadcast has no runtime membership API (peers come from node_grpc_addrs at startup).",
        ),
        (ClusterMode::Standalone, _) => (
            false,
            false,
            "No cluster plugin loaded. Topology is this process only.",
        ),
    };

    json!({
        "available": true,
        "mode": mode.as_str(),
        "plugin": plugin_name,
        "plugin_active": plugin_active,
        "local_node_id": local_id,
        "leader_id": leader_id,
        "role": local_role,
        "peers": peers,
        "nodes": nodes,
        "membership": {
            "join": join,
            "leave": leave,
            "reason": reason,
        },
        "raft": raft_status,
        "note": "GET is read-only topology. POST /cluster/join and POST /cluster/leave return per-node results; 501 when the write API does not exist.",
    })
}

/// Additive `cluster` object for existing `/brokers` and `/nodes` items.
pub(crate) fn attach_cluster_field(mut value: Value, snapshot: &Value) -> Value {
    if value.get("ok") == Some(&json!(false)) {
        return value;
    }
    let node_id = value.get("node_id").and_then(|v| v.as_u64());
    let role = snapshot
        .get("nodes")
        .and_then(|n| n.as_array())
        .and_then(|arr| {
            arr.iter()
                .find(|n| n.get("node_id").and_then(|v| v.as_u64()) == node_id)
                .and_then(|n| n.get("role"))
        })
        .cloned()
        .unwrap_or_else(|| json!(snapshot.get("role").cloned().unwrap_or(json!("unknown"))));
    if let Some(obj) = value.as_object_mut() {
        obj.insert(
            "cluster".into(),
            json!({
                "mode": snapshot.get("mode"),
                "plugin": snapshot.get("plugin"),
                "plugin_active": snapshot.get("plugin_active"),
                "leader_id": snapshot.get("leader_id"),
                "role": role,
                "peers": snapshot.get("peers"),
            }),
        );
    }
    value
}

/// `GET /api/v1/cluster` — topology.
#[handler]
pub(crate) async fn cluster_get(
    _req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let scx = {
        let (scx, _) = get_scx_cfg(depot)?;
        scx.clone()
    };
    res.render(Json(cluster_snapshot(&scx).await));
    Ok(())
}

fn membership_unavailable(
    action: &str,
    node_id: NodeId,
    snapshot: &Value,
    message: &str,
) -> (StatusCode, Value) {
    let body = json!({
        "ok": false,
        "action": action,
        "available": false,
        "message": message,
        "membership": snapshot.get("membership"),
        "nodes": [cluster_node_failure(node_id, message)],
    });
    (StatusCode::NOT_IMPLEMENTED, body)
}

/// `POST /api/v1/cluster/join` — always 501 today (startup-only join).
#[handler]
pub(crate) async fn cluster_join(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let scx = {
        let (scx, _) = get_scx_cfg(depot)?;
        scx.clone()
    };
    let snap = cluster_snapshot(&scx).await;
    let node_id = scx.node.id();
    let msg = snap
        .get("membership")
        .and_then(|m| m.get("reason"))
        .and_then(|v| v.as_str())
        .unwrap_or("runtime cluster join is not supported");
    let message = format!(
        "runtime join is not supported: {msg} Configure raft_peer_addrs (or node_grpc_addrs for broadcast) and start the node."
    );
    audit::record(
        req,
        depot,
        "cluster_join",
        Some(node_id.to_string()),
        false,
        Some(json!({"gap": true, "mode": snap.get("mode")})),
    )
    .await;
    let (status, body) = membership_unavailable("join", node_id, &snap, &message);
    render_api_error_with(res, status, message, Some(body));
    Ok(())
}

/// `POST /api/v1/cluster/leave` — raft Plugin::send or 501.
#[handler]
pub(crate) async fn cluster_leave(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let scx = {
        let (scx, _) = get_scx_cfg(depot)?;
        scx.clone()
    };
    let snap = cluster_snapshot(&scx).await;
    let node_id = scx.node.id();
    let can_leave =
        snap.get("membership").and_then(|m| m.get("leave")).and_then(|v| v.as_bool()).unwrap_or(false);
    let plugin = snap.get("plugin").and_then(|v| v.as_str()).map(|s| s.to_string());

    if !can_leave {
        let msg = snap
            .get("membership")
            .and_then(|m| m.get("reason"))
            .and_then(|v| v.as_str())
            .unwrap_or("cluster leave is not supported");
        audit::record(
            req,
            depot,
            "cluster_leave",
            Some(node_id.to_string()),
            false,
            Some(json!({"gap": true, "mode": snap.get("mode")})),
        )
        .await;
        let (status, body) = membership_unavailable("leave", node_id, &snap, msg);
        render_api_error_with(res, status, msg.to_string(), Some(body));
        return Ok(());
    }

    let name = plugin.unwrap_or_else(|| PLUGIN_CLUSTER_RAFT.to_string());
    match scx.plugins.send(&name, json!({"op": "leave"})).await {
        Ok(result) => {
            audit::record(
                req,
                depot,
                "cluster_leave",
                Some(node_id.to_string()),
                true,
                Some(json!({"plugin": name})),
            )
            .await;
            res.render(Json(json!({
                "ok": true,
                "action": "leave",
                "available": true,
                "nodes": [{
                    "ok": true,
                    "node_id": node_id,
                    "result": result,
                }],
            })));
        }
        Err(e) => {
            audit::record(
                req,
                depot,
                "cluster_leave",
                Some(node_id.to_string()),
                false,
                Some(json!({"error": e.to_string()})),
            )
            .await;
            let body = json!({
                "ok": false,
                "action": "leave",
                "available": true,
                "nodes": [cluster_node_failure(node_id, &e)],
            });
            render_api_error_with(res, StatusCode::BAD_GATEWAY, e.to_string(), Some(body));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use salvo::test::{ResponseExt, TestClient};
    use tokio::sync::RwLock;

    use crate::api::{route, route_with_auth};
    use crate::auth::{AuthState, Role};
    use crate::config::PluginConfig;
    use crate::prome;

    fn test_cfg() -> PluginConfig {
        serde_json::from_value(json!({"http_bearer_token": null})).expect("PluginConfig defaults")
    }

    fn dashboard_cfg() -> PluginConfig {
        let mut cfg = test_cfg();
        cfg.dashboard_admin_username = "admin".into();
        cfg.dashboard_admin_password = Some("admin-secret".into());
        cfg
    }

    async fn open_service() -> Service {
        let scx = ServerContext::new().node_id(1).busy_check_enable(false).build().await;
        let cfg = Arc::new(RwLock::new(test_cfg()));
        Service::new(route(scx, cfg, None, prome::Monitor::new(), None))
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

    async fn login(svc: &Service, username: &str, password: &str) -> (Response, Value) {
        let mut resp = TestClient::post("http://127.0.0.1:0/api/v1/auth/login")
            .json(&json!({"username": username, "password": password}))
            .send(svc)
            .await;
        let body: Value = resp.take_json().await.unwrap_or(json!({}));
        (resp, body)
    }

    #[test]
    fn alarm_bus_refresh_and_ack() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let bus = AlarmBus::new(4);
            bus.refresh(
                vec![AlarmCondition {
                    id: "node_unhealthy:2".into(),
                    name: "node_unhealthy".into(),
                    level: "critical".into(),
                    node_id: Some(2),
                    message: "down".into(),
                    source: "health".into(),
                }],
                10,
            )
            .await;
            let cur = bus.current().await;
            assert_eq!(cur.len(), 1);
            assert_eq!(cur[0].id, "node_unhealthy:2");
            assert!(!cur[0].acknowledged);
            let acked = bus.acknowledge("node_unhealthy:2", "ops", 11).await.unwrap();
            assert!(acked.acknowledged);
            assert_eq!(acked.acknowledged_by.as_deref(), Some("ops"));

            bus.refresh(vec![], 12).await;
            assert!(bus.current().await.is_empty());
            let hist = bus.history().await;
            assert_eq!(hist.len(), 1);
            assert_eq!(hist[0].cleared_at, Some(12));
            assert!(hist[0].acknowledged);
        });
    }

    #[tokio::test]
    async fn p6_gaps_and_topic_metrics_and_cluster_readonly() {
        let svc = open_service().await;

        let mut logs = TestClient::get("http://127.0.0.1:0/api/v1/logs").send(&svc).await;
        assert_eq!(logs.status_code, Some(StatusCode::OK));
        let logs_body: Value = logs.take_json().await.unwrap();
        assert_eq!(logs_body["available"], false);
        assert_eq!(logs_body["kind"], "logs");

        let mut trace = TestClient::get("http://127.0.0.1:0/api/v1/trace").send(&svc).await;
        let trace_body: Value = trace.take_json().await.unwrap();
        assert_eq!(trace_body["available"], false);

        let start = TestClient::post("http://127.0.0.1:0/api/v1/trace").send(&svc).await;
        assert_eq!(start.status_code, Some(StatusCode::NOT_IMPLEMENTED));

        let mut slow = TestClient::get("http://127.0.0.1:0/api/v1/slow-subs").send(&svc).await;
        let slow_body: Value = slow.take_json().await.unwrap();
        assert_eq!(slow_body["available"], false);
        assert_eq!(slow_body["kind"], "slow_sub");

        let mut tm = TestClient::get("http://127.0.0.1:0/api/v1/topic-metrics").send(&svc).await;
        assert_eq!(tm.status_code, Some(StatusCode::OK));
        let tm_body: Value = tm.take_json().await.unwrap();
        assert_eq!(tm_body["available"], true);
        assert_eq!(tm_body["kind"], "route_derived");
        assert!(tm_body["items"].is_array());
        assert_eq!(tm_body["sys_topic"]["plugin"], PLUGIN_SYS_TOPIC);
        assert_eq!(tm_body["sys_topic"]["active"], false);

        let mut cluster = TestClient::get("http://127.0.0.1:0/api/v1/cluster").send(&svc).await;
        let cluster_body: Value = cluster.take_json().await.unwrap();
        assert_eq!(cluster_body["available"], true);
        assert_eq!(cluster_body["mode"], "standalone");
        assert_eq!(cluster_body["membership"]["join"], false);
        assert_eq!(cluster_body["membership"]["leave"], false);
        assert_eq!(cluster_body["local_node_id"], 1);
        assert!(cluster_body["nodes"].as_array().unwrap().iter().any(|n| n["node_id"] == 1));

        let mut join = TestClient::post("http://127.0.0.1:0/api/v1/cluster/join").send(&svc).await;
        assert_eq!(join.status_code, Some(StatusCode::NOT_IMPLEMENTED));
        let join_body: Value = join.take_json().await.unwrap();
        assert_eq!(join_body["code"], 501);
        assert!(join_body["details"]["nodes"].is_array());
        assert_eq!(join_body["details"]["nodes"][0]["ok"], false);
        assert_eq!(join_body["details"]["nodes"][0]["node_id"], 1);

        let mut leave = TestClient::post("http://127.0.0.1:0/api/v1/cluster/leave").send(&svc).await;
        assert_eq!(leave.status_code, Some(StatusCode::NOT_IMPLEMENTED));
        let leave_body: Value = leave.take_json().await.unwrap();
        assert_eq!(leave_body["details"]["nodes"][0]["ok"], false);

        let mut brokers = TestClient::get("http://127.0.0.1:0/api/v1/brokers").send(&svc).await;
        let brokers_body: Value = brokers.take_json().await.unwrap();
        assert!(brokers_body.is_array());
        assert_eq!(brokers_body[0]["node_id"], 1);
        assert_eq!(brokers_body[0]["cluster"]["mode"], "standalone");
        assert!(brokers_body[0]["version"].is_string());

        let mut nodes = TestClient::get("http://127.0.0.1:0/api/v1/nodes").send(&svc).await;
        let nodes_body: Value = nodes.take_json().await.unwrap();
        assert_eq!(nodes_body[0]["node_id"], 1);
        assert_eq!(nodes_body[0]["cluster"]["mode"], "standalone");
        assert!(nodes_body[0]["connections"].is_number());

        let mut alarms = TestClient::get("http://127.0.0.1:0/api/v1/alarms").send(&svc).await;
        assert_eq!(alarms.status_code, Some(StatusCode::OK));
        let alarms_body: Value = alarms.take_json().await.unwrap();
        assert_eq!(alarms_body["available"], true);
        assert_eq!(alarms_body["source"], "derived");
        assert!(alarms_body["items"].is_array());
    }

    #[tokio::test]
    async fn p6_rbac_ack_and_cluster_writes() {
        let scx = ServerContext::new().node_id(1).busy_check_enable(false).build().await;
        let state = Arc::new(AuthState::new(None));
        let cfg = Arc::new(RwLock::new(dashboard_cfg()));
        let svc = Service::new(route_with_auth(scx, cfg, state.clone(), prome::Monitor::new(), None));

        let (admin_resp, admin_body) = login(&svc, "admin", "admin-secret").await;
        assert_eq!(admin_resp.status_code, Some(StatusCode::OK), "{admin_body}");
        let admin = session_cookie(&admin_resp);
        state.insert_user("ops", "ops-secret-1", Role::Operator).await.unwrap();
        state.insert_user("viewer", "viewer-secret", Role::Viewer).await.unwrap();
        let (ops_resp, _) = login(&svc, "ops", "ops-secret-1").await;
        let ops = session_cookie(&ops_resp);
        let (viewer_resp, _) = login(&svc, "viewer", "viewer-secret").await;
        let viewer = session_cookie(&viewer_resp);

        let viewer_read = TestClient::get("http://127.0.0.1:0/api/v1/cluster")
            .add_header("cookie", &viewer, true)
            .send(&svc)
            .await;
        assert_eq!(viewer_read.status_code, Some(StatusCode::OK));

        let denied_leave = TestClient::post("http://127.0.0.1:0/api/v1/cluster/leave")
            .add_header("cookie", &viewer, true)
            .send(&svc)
            .await;
        assert_eq!(denied_leave.status_code, Some(StatusCode::FORBIDDEN));

        let denied_ack = TestClient::post("http://127.0.0.1:0/api/v1/alarms/x/acknowledge")
            .add_header("cookie", &viewer, true)
            .send(&svc)
            .await;
        assert_eq!(denied_ack.status_code, Some(StatusCode::FORBIDDEN));

        let missing = TestClient::post("http://127.0.0.1:0/api/v1/alarms/no-such/acknowledge")
            .add_header("cookie", &ops, true)
            .send(&svc)
            .await;
        assert_eq!(missing.status_code, Some(StatusCode::NOT_FOUND));

        let mut audit = TestClient::get("http://127.0.0.1:0/api/v1/audit?action=alarm_acknowledge")
            .add_header("cookie", &admin, true)
            .send(&svc)
            .await;
        assert_eq!(audit.status_code, Some(StatusCode::OK));
        let audit_body: Value = audit.take_json().await.unwrap();
        assert!(audit_body.as_array().unwrap().iter().any(|e| e["action"] == "alarm_acknowledge"));
    }

    struct RaftStub {
        name: String,
    }

    impl ferromq::plugin::PackageInfo for RaftStub {
        fn name(&self) -> &str {
            &self.name
        }
    }

    #[async_trait::async_trait]
    impl ferromq::plugin::Plugin for RaftStub {
        async fn send(&self, msg: Value) -> ferromq::Result<Value> {
            match msg.get("op").and_then(|v| v.as_str()) {
                Some("status") => Ok(json!({
                    "ok": true,
                    "op": "status",
                    "raft_status": {"id": 1, "leader_id": 1, "role": "Leader", "peers": {}},
                })),
                Some("leave") => Ok(json!({"ok": true, "op": "leave"})),
                Some("join") => Err(anyhow::anyhow!("runtime join is not supported")),
                other => Err(anyhow::anyhow!("unknown op {other:?}")),
            }
        }
        async fn attrs(&self) -> Value {
            json!({"raft_status": {"id": 1, "leader_id": 1}})
        }
    }

    #[tokio::test]
    async fn p6_raft_stub_leave_returns_per_node_ok() {
        let scx = ServerContext::new().node_id(1).busy_check_enable(false).build().await;
        scx.plugins
            .register(PLUGIN_CLUSTER_RAFT, true, false, || {
                Box::pin(async {
                    Ok(Box::new(RaftStub { name: PLUGIN_CLUSTER_RAFT.into() }) as ferromq::plugin::DynPlugin)
                })
            })
            .await
            .unwrap();
        let cfg = Arc::new(RwLock::new(test_cfg()));
        let svc = Service::new(route(scx, cfg, None, prome::Monitor::new(), None));

        let mut cluster = TestClient::get("http://127.0.0.1:0/api/v1/cluster").send(&svc).await;
        let cluster_body: Value = cluster.take_json().await.unwrap();
        assert_eq!(cluster_body["mode"], "raft");
        assert_eq!(cluster_body["plugin"], PLUGIN_CLUSTER_RAFT);
        assert_eq!(cluster_body["membership"]["leave"], true);
        assert_eq!(cluster_body["membership"]["join"], false);

        let join = TestClient::post("http://127.0.0.1:0/api/v1/cluster/join").send(&svc).await;
        assert_eq!(join.status_code, Some(StatusCode::NOT_IMPLEMENTED));

        let mut leave = TestClient::post("http://127.0.0.1:0/api/v1/cluster/leave").send(&svc).await;
        assert_eq!(leave.status_code, Some(StatusCode::OK));
        let leave_body: Value = leave.take_json().await.unwrap();
        assert_eq!(leave_body["ok"], true);
        assert_eq!(leave_body["action"], "leave");
        assert_eq!(leave_body["nodes"][0]["ok"], true);
        assert_eq!(leave_body["nodes"][0]["node_id"], 1);
        assert_eq!(leave_body["nodes"][0]["result"]["op"], "leave");

        let mut brokers = TestClient::get("http://127.0.0.1:0/api/v1/brokers").send(&svc).await;
        let brokers_body: Value = brokers.take_json().await.unwrap();
        assert_eq!(brokers_body[0]["cluster"]["mode"], "raft");
        assert_eq!(brokers_body[0]["cluster"]["plugin"], PLUGIN_CLUSTER_RAFT);
    }
}
