//! P5 access-control and integrations management APIs.
//!
//! Structured REST on top of P4 plugin-config write + `load_config`.
//! Secrets are redacted on GET unless `?reveal=1` and the caller is admin.
//! FerroMQ has no dedicated blacklist plugin; that gap is reported honestly.

use std::io::ErrorKind;
use std::net::IpAddr;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use salvo::http::StatusCode;
use salvo::prelude::*;
use serde_json::{json, Value};

use ferromq::context::ServerContext;
use ferromq::grpc::{Message as GrpcMessage, MessageReply as GrpcMessageReply, MessageSender};
use ferromq::types::NodeId;
use ferromq::Result;

use super::audit;
use super::config_mgmt::{
    assign_object_preserving_secrets, deny_reveal_if_needed, json_to_toml, local_write_plugin_lenient,
    merge_plugin_patch, parse_apply, read_local_plugin_file_json, redact_secrets, toml_to_json, wants_reveal,
    WriteResult,
};
use super::plugin;
use super::response::{
    render_api_error, render_api_error_with, render_list, status_for_plugin_error, ListPaging,
};
use super::types::{Message, MessageReply};
use super::PluginConfigType;

pub(crate) const PLUGIN_ACL: &str = "ferromq-acl";
pub(crate) const PLUGIN_AUTH_HTTP: &str = "ferromq-auth-http";
pub(crate) const PLUGIN_AUTH_JWT: &str = "ferromq-auth-jwt";
pub(crate) const PLUGIN_AUTO_SUB: &str = "ferromq-auto-subscription";
pub(crate) const PLUGIN_TOPIC_REWRITE: &str = "ferromq-topic-rewrite";
pub(crate) const PLUGIN_WEBHOOK: &str = "ferromq-web-hook";

const KNOWN_BRIDGES: &[&str] = &[
    "ferromq-bridge-egress-kafka",
    "ferromq-bridge-egress-mqtt",
    "ferromq-bridge-egress-nats",
    "ferromq-bridge-egress-pulsar",
    "ferromq-bridge-egress-reductstore",
    "ferromq-bridge-ingress-kafka",
    "ferromq-bridge-ingress-mqtt",
    "ferromq-bridge-ingress-nats",
    "ferromq-bridge-ingress-pulsar",
    "ferromq-bridge-origin",
];

const AUTH_PROVIDERS: &[(&str, &str)] = &[(PLUGIN_AUTH_HTTP, "http"), (PLUGIN_AUTH_JWT, "jwt")];

const WEBHOOK_HOOKS: &[&str] = &[
    "session_created",
    "session_terminated",
    "session_subscribed",
    "session_unsubscribed",
    "client_connect",
    "client_connack",
    "client_connected",
    "client_disconnected",
    "client_subscribe",
    "client_unsubscribe",
    "message_publish",
    "message_delivered",
    "message_acked",
    "message_dropped",
    "offline_message",
    "keepalive",
    "offline_inflight_messages",
    "message_expiry_check",
    "before_startup",
    "grpc_message_received",
];

const ACL_CONTROLS: &[&str] = &["all", "connect", "publish", "subscribe", "pubsub"];
const REWRITE_ACTIONS: &[&str] = &["all", "publish", "subscribe"];

fn scx_cfg(depot: &mut Depot) -> std::result::Result<(ServerContext, PluginConfigType), salvo::Error> {
    let pair = depot.obtain::<(ServerContext, PluginConfigType)>().map_err(|e| match e {
        None => salvo::Error::Io(std::io::Error::new(ErrorKind::NotFound, anyhow!("None"))),
        Some(e) => salvo::Error::Io(std::io::Error::new(ErrorKind::NotFound, format!("{e:?}"))),
    })?;
    Ok((pair.0.clone(), pair.1.clone()))
}

fn history_keep(cfg: &super::config::PluginConfig) -> usize {
    if cfg.config_history_keep == 0 {
        10
    } else {
        cfg.config_history_keep
    }
}

fn resolve_node(req: &Request, scx: &ServerContext) -> Result<NodeId> {
    if let Some(n) = req.query::<NodeId>("node") {
        return Ok(n);
    }
    if let Some(n) = req.param::<NodeId>("node") {
        return Ok(n);
    }
    Ok(scx.node.id())
}

fn wants_allow_private(req: &Request) -> bool {
    req.query::<String>("allow_private")
        .as_deref()
        .is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}

fn plugin_reloadable(scx: &ServerContext, name: &str) -> Option<bool> {
    scx.plugins.get(name).map(|entry| !entry.immutable() && entry.inited())
}

async fn plugin_info_json(scx: &ServerContext, name: &str) -> Option<Value> {
    plugin::get_plugin(scx, name).await.ok().flatten().and_then(|p| p.to_json().ok())
}

/// Whether a host/IP is unsafe for server-side fetch (SSRF).
pub(crate) fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v) => {
            v.is_private()
                || v.is_loopback()
                || v.is_link_local()
                || v.is_unspecified()
                || v.is_broadcast()
                || v.octets() == [169, 254, 169, 254]
                || v.octets()[0] == 0
        }
        IpAddr::V6(v) => {
            v.is_loopback()
                || v.is_unspecified()
                || v.is_unique_local()
                || (v.segments()[0] & 0xffc0) == 0xfe80
                || v.to_ipv4_mapped().is_some_and(|m| is_blocked_ip(IpAddr::V4(m)))
        }
    }
}

/// Parsed http(s) URL used for SSRF checks and TCP probes.
#[derive(Debug, Clone)]
pub(crate) struct HttpUrl {
    pub host: String,
    pub port: u16,
}

/// Validate an http(s) callback URL. Rejects file/gopher and (unless `allow_private`)
/// loopback / RFC1918 / link-local / cloud-metadata hosts.
pub(crate) fn validate_callback_url(raw: &str, allow_private: bool) -> std::result::Result<HttpUrl, String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err("url is empty".into());
    }
    if raw.len() > 2048 {
        return Err("url is too long".into());
    }
    let (scheme, rest) = raw.split_once("://").ok_or_else(|| "url must include a scheme".to_string())?;
    let scheme_l = scheme.to_ascii_lowercase();
    if scheme_l != "http" && scheme_l != "https" {
        return Err(format!("unsupported scheme '{scheme}'; only http and https are allowed"));
    }
    let after_auth = rest.split_once('@').map(|(_, hostport)| hostport).unwrap_or(rest);
    let authority = after_auth.split(['/', '?', '#']).next().unwrap_or("");
    if authority.is_empty() {
        return Err("url is missing a host".into());
    }
    let (host, port) = if let Some(h) = authority.strip_prefix('[') {
        let (host, rest) = h.split_once(']').ok_or_else(|| "invalid IPv6 host".to_string())?;
        let port = if let Some(p) = rest.strip_prefix(':') {
            p.parse::<u16>().map_err(|_| "invalid port".to_string())?
        } else {
            if scheme_l == "https" {
                443
            } else {
                80
            }
        };
        (host.to_string(), port)
    } else if let Some((h, p)) = authority.rsplit_once(':') {
        if h.chars().all(|c| c.is_ascii_digit() || c == '.') && p.chars().all(|c| c.is_ascii_digit()) {
            (h.to_string(), p.parse::<u16>().map_err(|_| "invalid port".to_string())?)
        } else if p.chars().all(|c| c.is_ascii_digit()) {
            (h.to_string(), p.parse::<u16>().map_err(|_| "invalid port".to_string())?)
        } else {
            (authority.to_string(), if scheme_l == "https" { 443 } else { 80 })
        }
    } else {
        (authority.to_string(), if scheme_l == "https" { 443 } else { 80 })
    };
    if host.is_empty() {
        return Err("url is missing a host".into());
    }
    let host_l = host.to_ascii_lowercase();
    if host_l == "metadata.google.internal"
        || host_l.ends_with(".metadata.google.internal")
        || host_l == "metadata.internal"
    {
        return Err("cloud metadata hosts are not allowed".into());
    }
    if !allow_private {
        if host_l == "localhost" || host_l.ends_with(".localhost") || host_l.ends_with(".local") {
            return Err("localhost / *.local is blocked (pass allow_private=1 to override)".into());
        }
        if let Ok(ip) = host.parse::<IpAddr>() {
            if is_blocked_ip(ip) {
                return Err("private, loopback, link-local, or metadata IP is blocked".into());
            }
        }
    }
    let _ = scheme_l;
    Ok(HttpUrl { host, port })
}

/// If the incoming URL has a redacted `***:***` userinfo, keep `existing`
/// when the host/path still matches. Never persist the literal `***`.
pub(crate) fn restore_webhook_url(
    existing: Option<&str>,
    incoming: &str,
) -> std::result::Result<String, String> {
    let incoming = normalize_webhook_url(incoming)?;
    if incoming.contains("***") {
        if let Some(prev) = existing {
            let prev_n = normalize_webhook_url(prev)?;
            if redact_url_userinfo(&prev_n) == redact_url_userinfo(&incoming)
                || redact_url_userinfo(&prev_n) == incoming
            {
                return Ok(prev_n);
            }
        }
        return Err("refusing to write redacted URL userinfo '***'".into());
    }
    Ok(incoming)
}

pub(crate) fn redact_url_userinfo(raw: &str) -> String {
    let Some((scheme, rest)) = raw.split_once("://") else {
        return raw.to_string();
    };
    if let Some((auth, hostport)) = rest.split_once('@') {
        if auth.contains(':') || !auth.is_empty() {
            return format!("{scheme}://***:***@{hostport}");
        }
    }
    raw.to_string()
}

fn redact_urls_in_value(value: Value) -> Value {
    match value {
        Value::String(s) => {
            if s.contains("://") {
                Value::String(redact_url_userinfo(&s))
            } else {
                Value::String(s)
            }
        }
        Value::Array(items) => Value::Array(items.into_iter().map(redact_urls_in_value).collect()),
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                out.insert(k, redact_urls_in_value(v));
            }
            Value::Object(out)
        }
        other => other,
    }
}

fn prepare_body(mut value: Value, reveal: bool) -> Value {
    if !reveal {
        value = redact_secrets(value);
        value = redact_urls_in_value(value);
    }
    value
}

/// After `lookup_host`, reject blocked IPs unless `allow_private`.
/// `tcp_probe` resolves first, then calls this, then connects (never HTTP GET).
pub(crate) fn reject_blocked_resolved_ips(
    addrs: impl IntoIterator<Item = IpAddr>,
    allow_private: bool,
) -> std::result::Result<(), String> {
    if allow_private {
        return Ok(());
    }
    for ip in addrs {
        if is_blocked_ip(ip) {
            return Err("private, loopback, link-local, or metadata IP is blocked".into());
        }
    }
    Ok(())
}

async fn tcp_probe(
    host: &str,
    port: u16,
    timeout: Duration,
    allow_private: bool,
) -> (bool, Option<u64>, Option<String>) {
    let start = Instant::now();
    let addrs = match tokio::time::timeout(timeout, tokio::net::lookup_host((host, port))).await {
        Ok(Ok(iter)) => iter.collect::<Vec<_>>(),
        Ok(Err(e)) => return (false, Some(start.elapsed().as_millis() as u64), Some(e.to_string())),
        Err(_) => return (false, Some(timeout.as_millis() as u64), Some("dns lookup timed out".into())),
    };
    if addrs.is_empty() {
        return (false, Some(start.elapsed().as_millis() as u64), Some("host did not resolve".into()));
    }
    if let Err(e) = reject_blocked_resolved_ips(addrs.iter().map(|a| a.ip()), allow_private) {
        return (false, Some(start.elapsed().as_millis() as u64), Some(e));
    }
    match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(addrs.as_slice())).await {
        Ok(Ok(_stream)) => (true, Some(start.elapsed().as_millis() as u64), None),
        Ok(Err(e)) => (false, Some(start.elapsed().as_millis() as u64), Some(e.to_string())),
        Err(_) => (false, Some(timeout.as_millis() as u64), Some("tcp connect timed out".into())),
    }
}

fn unbalanced_regex_parens(re: &str) -> bool {
    let mut depth = 0i32;
    let mut escaped = false;
    for c in re.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        if c == '\\' {
            escaped = true;
            continue;
        }
        if c == '(' {
            depth += 1;
        } else if c == ')' {
            depth -= 1;
            if depth < 0 {
                return true;
            }
        }
    }
    depth != 0
}

async fn read_plugin_json(
    scx: &ServerContext,
    node_id: NodeId,
    name: &str,
    message_type: u64,
) -> Result<Value> {
    if node_id == scx.node.id() {
        if let Some(v) = read_local_plugin_file_json(scx, name)? {
            return Ok(v);
        }
        if let Ok(raw) = plugin::get_plugin_config(scx, name).await {
            if let Ok(v) = serde_json::from_slice::<Value>(&raw) {
                return Ok(v);
            }
        }
        return Err(anyhow!(format!("config file not found: {name}")));
    }
    let c = crate::api::get_grpc_client(scx, node_id).await?;
    let encoded = Message::GetPluginConfigFile { name }.encode()?;
    let reply =
        MessageSender::new_quick(c, message_type, GrpcMessage::Data(encoded), Some(Duration::from_secs(15)))
            .send()
            .await?;
    match reply {
        GrpcMessageReply::Data(raw) => match MessageReply::decode(&raw)? {
            MessageReply::GetPluginConfigFile(toml_text) => toml_to_json(&toml_text),
            _ => Err(anyhow!("unexpected plugin-config-file gRPC reply")),
        },
        other => Err(anyhow!("invalid gRPC reply: {other:?}")),
    }
}

async fn write_plugin_json(
    scx: &ServerContext,
    node_id: NodeId,
    name: &str,
    json: &Value,
    apply: bool,
    keep: usize,
    message_type: u64,
) -> Result<WriteResult> {
    let toml_text = json_to_toml(json)?;
    if node_id == scx.node.id() {
        return local_write_plugin_lenient(scx, name, json, &toml_text, apply, keep).await;
    }
    let c = crate::api::get_grpc_client(scx, node_id).await?;
    let encoded = Message::WritePluginConfig { name, toml: &toml_text, apply }.encode()?;
    let reply =
        MessageSender::new_quick(c, message_type, GrpcMessage::Data(encoded), Some(Duration::from_secs(15)))
            .send()
            .await?;
    match reply {
        GrpcMessageReply::Data(raw) => match MessageReply::decode(&raw)? {
            MessageReply::WritePluginConfig(s) => {
                serde_json::from_str(&s).map_err(|e| anyhow!("decode remote write: {e}"))
            }
            _ => Err(anyhow!("unexpected plugin-config write gRPC reply")),
        },
        other => Err(anyhow!("invalid gRPC reply: {other:?}")),
    }
}

async fn load_or_unload_plugin(
    scx: &ServerContext,
    node_id: NodeId,
    name: &str,
    load: bool,
    message_type: u64,
) -> Result<bool> {
    if node_id == scx.node.id() {
        return if load { scx.plugins.start(name).await.map(|_| true) } else { scx.plugins.stop(name).await };
    }
    let c = crate::api::get_grpc_client(scx, node_id).await?;
    let msg = if load { Message::LoadPlugin { name } } else { Message::UnloadPlugin { name } };
    let encoded = msg.encode()?;
    let reply =
        MessageSender::new_quick(c, message_type, GrpcMessage::Data(encoded), Some(Duration::from_secs(15)))
            .send()
            .await?;
    match reply {
        GrpcMessageReply::Data(raw) => match MessageReply::decode(&raw)? {
            MessageReply::LoadPlugin => Ok(true),
            MessageReply::UnloadPlugin(ok) => Ok(ok),
            _ => Err(anyhow!("unexpected plugin load/unload gRPC reply")),
        },
        other => Err(anyhow!("invalid gRPC reply: {other:?}")),
    }
}

fn wrap_write(r: WriteResult, extra: Value) -> Value {
    let mut body = serde_json::to_value(&r).unwrap_or_else(|_| json!({}));
    if let Some(obj) = extra.as_object() {
        if let Some(dst) = body.as_object_mut() {
            for (k, v) in obj {
                dst.insert(k.clone(), v.clone());
            }
        }
    }
    body
}

fn parse_index(req: &Request) -> std::result::Result<usize, String> {
    req.param::<usize>("index").ok_or_else(|| "index not found".into())
}

fn array_mut<'a>(doc: &'a mut Value, key: &str) -> std::result::Result<&'a mut Vec<Value>, String> {
    if doc.get(key).is_none() {
        if let Some(obj) = doc.as_object_mut() {
            obj.insert(key.to_string(), json!([]));
        }
    }
    doc.get_mut(key).and_then(|v| v.as_array_mut()).ok_or_else(|| format!("{key} must be an array"))
}

fn require_object(v: &Value, what: &str) -> std::result::Result<(), String> {
    if v.is_object() {
        Ok(())
    } else {
        Err(format!("{what} must be a JSON object"))
    }
}

// ── ACL ──────────────────────────────────────────────────────────────────

pub(crate) fn normalize_acl_rule(input: &Value) -> std::result::Result<Value, String> {
    if let Some(raw) = input.get("raw") {
        return normalize_acl_raw(raw);
    }
    if input.is_array() {
        return normalize_acl_raw(input);
    }
    require_object(input, "ACL rule")?;
    let access = input
        .get("access")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "access is required (allow|deny)".to_string())?
        .to_ascii_lowercase();
    if access != "allow" && access != "deny" {
        return Err("access must be allow or deny".into());
    }
    let who =
        input.get("who").cloned().ok_or_else(|| "who is required (\"all\" or an object)".to_string())?;
    validate_acl_who(&who)?;
    let control = input.get("control").and_then(|v| v.as_str()).unwrap_or("all").to_ascii_lowercase();
    if !ACL_CONTROLS.contains(&control.as_str()) {
        return Err(format!("control must be one of {}", ACL_CONTROLS.join("|")));
    }
    let topics = input.get("topics").cloned();
    if let Some(ref t) = topics {
        validate_acl_topics(t)?;
    }
    let mut raw = vec![json!(access), who];
    if control != "all" || topics.is_some() {
        raw.push(json!(control));
    }
    if let Some(t) = topics {
        raw.push(t);
    }
    Ok(Value::Array(raw))
}

fn normalize_acl_raw(raw: &Value) -> std::result::Result<Value, String> {
    let arr = raw.as_array().ok_or_else(|| "ACL rule raw form must be an array".to_string())?;
    if arr.len() < 2 || arr.len() > 4 {
        return Err("ACL rule must have 2–4 columns: [access, who, control?, topics?]".into());
    }
    let access = arr[0].as_str().ok_or_else(|| "access must be a string".to_string())?.to_ascii_lowercase();
    if access != "allow" && access != "deny" {
        return Err("access must be allow or deny".into());
    }
    validate_acl_who(&arr[1])?;
    let mut out = vec![json!(access), arr[1].clone()];
    if let Some(c) = arr.get(2) {
        let control = c.as_str().ok_or_else(|| "control must be a string".to_string())?.to_ascii_lowercase();
        if !ACL_CONTROLS.contains(&control.as_str()) {
            return Err(format!("control must be one of {}", ACL_CONTROLS.join("|")));
        }
        out.push(json!(control));
    }
    if let Some(t) = arr.get(3) {
        validate_acl_topics(t)?;
        out.push(t.clone());
    }
    Ok(Value::Array(out))
}

fn validate_acl_who(who: &Value) -> std::result::Result<(), String> {
    match who {
        Value::String(s) if s.eq_ignore_ascii_case("all") => Ok(()),
        Value::Object(map) => {
            let known = ["user", "password", "superuser", "clientid", "ipaddr", "protocol"];
            if map.is_empty() {
                return Err("who object must set user, clientid, ipaddr, and/or protocol".into());
            }
            for k in map.keys() {
                if !known.contains(&k.as_str()) {
                    return Err(format!("unknown who field '{k}'"));
                }
            }
            if let Some(p) = map.get("protocol") {
                if !p.is_u64() && !p.is_i64() {
                    return Err("who.protocol must be an integer (3, 4, or 5)".into());
                }
            }
            if let Some(su) = map.get("superuser") {
                if !su.is_boolean() {
                    return Err("who.superuser must be a boolean".into());
                }
            }
            Ok(())
        }
        _ => Err("who must be \"all\" or an object".into()),
    }
}

fn validate_acl_topics(topics: &Value) -> std::result::Result<(), String> {
    let arr = topics.as_array().ok_or_else(|| "topics must be an array".to_string())?;
    for t in arr {
        match t {
            Value::String(s) if !s.is_empty() => {}
            Value::Object(m) => {
                if !m.get("eq").and_then(|v| v.as_str()).is_some_and(|s| !s.is_empty()) {
                    return Err("topic object must be { \"eq\": \"<topic>\" }".into());
                }
            }
            _ => return Err("each topic must be a string or { eq }".into()),
        }
    }
    Ok(())
}

pub(crate) fn acl_rule_view(raw: &Value, index: usize) -> Value {
    let arr = raw.as_array();
    let access = arr.and_then(|a| a.first()).and_then(|v| v.as_str()).unwrap_or("");
    let who = arr.and_then(|a| a.get(1)).cloned().unwrap_or(Value::Null);
    let control = arr.and_then(|a| a.get(2)).and_then(|v| v.as_str()).unwrap_or("all");
    let topics = arr.and_then(|a| a.get(3)).cloned();
    let mut v = json!({
        "index": index,
        "access": access,
        "who": who,
        "control": control,
        "raw": raw,
    });
    if let Some(t) = topics {
        v["topics"] = t;
    }
    v
}

fn acl_rules_mut(doc: &mut Value) -> std::result::Result<&mut Vec<Value>, String> {
    array_mut(doc, "rules")
}

async fn acl_doc(scx: &ServerContext, node_id: NodeId, message_type: u64) -> Result<Value> {
    match read_plugin_json(scx, node_id, PLUGIN_ACL, message_type).await {
        Ok(v) => Ok(v),
        Err(e) if e.to_string().contains("not found") => Ok(json!({
            "disconnect_if_pub_rejected": true,
            "priority": 10,
            "rules": []
        })),
        Err(e) => Err(e),
    }
}

fn acl_envelope(scx: &ServerContext, node_id: NodeId, doc: &Value, reveal: bool) -> Value {
    let rules = doc.get("rules").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let views: Vec<Value> = rules.iter().enumerate().map(|(i, r)| acl_rule_view(r, i)).collect();
    let info = futures_placeholder_info(scx, PLUGIN_ACL);
    let mut body = json!({
        "plugin": PLUGIN_ACL,
        "node": node_id,
        "available": info.0 || doc.get("rules").is_some(),
        "active": info.1,
        "inited": info.2,
        "reloadable": plugin_reloadable(scx, PLUGIN_ACL),
        "disconnect_if_pub_rejected": doc.get("disconnect_if_pub_rejected").cloned().unwrap_or(json!(true)),
        "priority": doc.get("priority").cloned().unwrap_or(json!(10)),
        "rules": views,
        "note": "Writes update ferromq-acl.toml and apply via plugin load_config when the plugin is loaded (effective=hot). This is not a ferromqd restart.",
    });
    body = prepare_body(body, reveal);
    body
}

fn futures_placeholder_info(scx: &ServerContext, name: &str) -> (bool, bool, bool) {
    match scx.plugins.get(name) {
        Some(e) => (true, e.active(), e.inited()),
        None => (false, false, false),
    }
}

#[handler]
pub(crate) async fn acl_get(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    if deny_reveal_if_needed(req, depot, res) {
        return Ok(());
    }
    let (scx, cfg) = scx_cfg(depot)?;
    let node_id = match resolve_node(req, &scx) {
        Ok(n) => n,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let message_type = cfg.read().await.message_type;
    match acl_doc(&scx, node_id, message_type).await {
        Ok(doc) => res.render(Json(acl_envelope(&scx, node_id, &doc, wants_reveal(req)))),
        Err(e) => render_api_error(res, status_for_plugin_error(&e), e.to_string()),
    }
    Ok(())
}

#[handler]
pub(crate) async fn acl_put(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    mutate_doc(req, depot, res, PLUGIN_ACL, "acl_config_update", |doc, body| {
        require_object(body, "ACL config")?;
        if let Some(v) = body.get("disconnect_if_pub_rejected") {
            if !v.is_boolean() {
                return Err("disconnect_if_pub_rejected must be a boolean".into());
            }
            doc["disconnect_if_pub_rejected"] = v.clone();
        }
        if let Some(v) = body.get("priority") {
            if !v.is_u64() && !v.is_i64() {
                return Err("priority must be an integer".into());
            }
            doc["priority"] = v.clone();
        }
        if let Some(rules) = body.get("rules") {
            let arr = rules.as_array().ok_or_else(|| "rules must be an array".to_string())?;
            let mut out = Vec::new();
            for r in arr {
                out.push(normalize_acl_rule(r)?);
            }
            doc["rules"] = Value::Array(out);
        }
        Ok(json!({"plugin": PLUGIN_ACL}))
    })
    .await
}

#[handler]
pub(crate) async fn acl_rules_list(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    if deny_reveal_if_needed(req, depot, res) {
        return Ok(());
    }
    let (scx, cfg) = scx_cfg(depot)?;
    let node_id = match resolve_node(req, &scx) {
        Ok(n) => n,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let message_type = cfg.read().await.message_type;
    match acl_doc(&scx, node_id, message_type).await {
        Ok(doc) => {
            let rules = doc.get("rules").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            let mut views: Vec<Value> = rules.iter().enumerate().map(|(i, r)| acl_rule_view(r, i)).collect();
            if !wants_reveal(req) {
                views = views.into_iter().map(|v| prepare_body(v, false)).collect();
            }
            let paging = ListPaging::from_request(req, 0, cfg.read().await.max_row_limit);
            let total = views.len();
            let (page, truncated) = paging.apply(views);
            render_list(req, res, page, paging, truncated, Some(total));
        }
        Err(e) => render_api_error(res, status_for_plugin_error(&e), e.to_string()),
    }
    Ok(())
}

#[handler]
pub(crate) async fn acl_rules_add(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    mutate_doc(req, depot, res, PLUGIN_ACL, "acl_rule_add", |doc, body| {
        let raw = normalize_acl_rule(body)?;
        let rules = acl_rules_mut(doc)?;
        let at = body.get("index").and_then(|v| v.as_u64()).map(|n| n as usize);
        let index = if let Some(i) = at {
            if i > rules.len() {
                return Err(format!("index {i} out of range (len {})", rules.len()));
            }
            rules.insert(i, raw.clone());
            i
        } else {
            rules.push(raw.clone());
            rules.len() - 1
        };
        Ok(json!({"rule": acl_rule_view(&raw, index)}))
    })
    .await
}

#[handler]
pub(crate) async fn acl_rules_update(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let index = match parse_index(req) {
        Ok(i) => i,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e);
            return Ok(());
        }
    };
    mutate_doc(req, depot, res, PLUGIN_ACL, "acl_rule_update", move |doc, body| {
        let raw = normalize_acl_rule(body)?;
        let rules = acl_rules_mut(doc)?;
        if index >= rules.len() {
            return Err(format!("ACL rule index {index} not found"));
        }
        let raw = merge_plugin_patch(rules[index].clone(), raw).map_err(|e| e.to_string())?;
        rules[index] = raw.clone();
        Ok(json!({"rule": acl_rule_view(&raw, index)}))
    })
    .await
}

#[handler]
pub(crate) async fn acl_rules_delete(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let index = match parse_index(req) {
        Ok(i) => i,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e);
            return Ok(());
        }
    };
    mutate_doc(req, depot, res, PLUGIN_ACL, "acl_rule_delete", move |doc, _body| {
        let rules = acl_rules_mut(doc)?;
        if index >= rules.len() {
            return Err(format!("ACL rule index {index} not found"));
        }
        let removed = rules.remove(index);
        Ok(json!({"removed": acl_rule_view(&removed, index)}))
    })
    .await
}

// ── generic mutate ───────────────────────────────────────────────────────

async fn mutate_doc<F>(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
    plugin: &str,
    action: &str,
    mutator: F,
) -> std::result::Result<(), salvo::Error>
where
    F: FnOnce(&mut Value, &Value) -> std::result::Result<Value, String>,
{
    let (scx, cfg) = scx_cfg(depot)?;
    let node_id = match resolve_node(req, &scx) {
        Ok(n) => n,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let apply = match parse_apply(req) {
        Ok(v) => v,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let body = match req.parse_json::<Value>().await {
        Ok(v) => v,
        Err(_) => {
            let bytes = match req.payload().await {
                Ok(b) => b.to_vec(),
                Err(e) => {
                    render_api_error(res, StatusCode::BAD_REQUEST, format!("read body: {e}"));
                    return Ok(());
                }
            };
            if bytes.is_empty() {
                json!({})
            } else {
                match serde_json::from_slice(&bytes) {
                    Ok(v) => v,
                    Err(e) => {
                        render_api_error(res, StatusCode::BAD_REQUEST, format!("invalid JSON: {e}"));
                        return Ok(());
                    }
                }
            }
        }
    };
    let keep = history_keep(&*cfg.read().await);
    let message_type = cfg.read().await.message_type;
    let mut doc = match read_plugin_json(&scx, node_id, plugin, message_type).await {
        Ok(v) => v,
        Err(e) if e.to_string().contains("not found") => json!({}),
        Err(e) => {
            render_api_error(res, status_for_plugin_error(&e), e.to_string());
            return Ok(());
        }
    };
    if !doc.is_object() {
        doc = json!({});
    }
    let extra = match mutator(&mut doc, &body) {
        Ok(v) => v,
        Err(e) => {
            let not_found = e.contains("not found");
            render_api_error(res, if not_found { StatusCode::NOT_FOUND } else { StatusCode::BAD_REQUEST }, e);
            return Ok(());
        }
    };
    match write_plugin_json(&scx, node_id, plugin, &doc, apply, keep, message_type).await {
        Ok(r) => {
            audit::record(
                req,
                depot,
                action,
                Some(format!("{node_id}/{plugin}")),
                r.ok && r.written,
                Some(json!({"effective": r.effective, "applied": r.applied, "diff": r.diff})),
            )
            .await;
            res.render(Json(wrap_write(r, extra)));
        }
        Err(e) => {
            audit::record(
                req,
                depot,
                action,
                Some(format!("{node_id}/{plugin}")),
                false,
                Some(json!({"error": e.to_string()})),
            )
            .await;
            render_api_error(res, status_for_plugin_error(&e), e.to_string());
        }
    }
    Ok(())
}

// ── Auth providers ───────────────────────────────────────────────────────

fn auth_kind(name: &str) -> Option<&'static str> {
    AUTH_PROVIDERS.iter().find(|(n, _)| *n == name).map(|(_, k)| *k)
}

fn normalize_auth_name(name: &str) -> Option<&'static str> {
    match name {
        "http" | "ferromq-auth-http" => Some(PLUGIN_AUTH_HTTP),
        "jwt" | "ferromq-auth-jwt" => Some(PLUGIN_AUTH_JWT),
        _ => AUTH_PROVIDERS.iter().find(|(n, _)| *n == name).map(|(n, _)| *n),
    }
}

#[handler]
pub(crate) async fn auth_providers_list(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, _cfg) = scx_cfg(depot)?;
    let node_id = match resolve_node(req, &scx) {
        Ok(n) => n,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let mut providers = Vec::new();
    for (name, kind) in AUTH_PROVIDERS {
        let info = plugin_info_json(&scx, name).await;
        let (available, active, inited, attrs) = match info {
            Some(v) => (
                true,
                v.get("active").and_then(|x| x.as_bool()).unwrap_or(false),
                v.get("inited").and_then(|x| x.as_bool()).unwrap_or(false),
                v.get("attrs").cloned().unwrap_or(Value::Null),
            ),
            None => (false, false, false, Value::Null),
        };
        providers.push(json!({
            "name": name,
            "kind": kind,
            "node": node_id,
            "available": available,
            "active": active,
            "inited": inited,
            "reloadable": plugin_reloadable(&scx, name),
            "attrs": attrs,
        }));
    }
    res.render(Json(json!({
        "node": node_id,
        "providers": providers,
        "note": "These are MQTT client auth plugins (ferromq-auth-http / ferromq-auth-jwt), not dashboard login.",
    })));
    Ok(())
}

#[handler]
pub(crate) async fn auth_provider_get(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    if deny_reveal_if_needed(req, depot, res) {
        return Ok(());
    }
    let Some(name) = req.param::<String>("name").and_then(|n| normalize_auth_name(&n).map(|s| s.to_string()))
    else {
        render_api_error(res, StatusCode::NOT_FOUND, "unknown auth provider (http|jwt)");
        return Ok(());
    };
    let (scx, cfg) = scx_cfg(depot)?;
    let node_id = match resolve_node(req, &scx) {
        Ok(n) => n,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let message_type = cfg.read().await.message_type;
    let doc = match read_plugin_json(&scx, node_id, &name, message_type).await {
        Ok(v) => v,
        Err(e) => {
            let status = if e.to_string().contains("not found") {
                StatusCode::NOT_FOUND
            } else {
                status_for_plugin_error(&e)
            };
            render_api_error(res, status, e.to_string());
            return Ok(());
        }
    };
    let info = plugin_info_json(&scx, &name).await;
    let mut body = json!({
        "name": name,
        "kind": auth_kind(&name),
        "node": node_id,
        "available": info.is_some(),
        "active": info.as_ref().and_then(|v| v.get("active")).cloned().unwrap_or(json!(false)),
        "inited": info.as_ref().and_then(|v| v.get("inited")).cloned().unwrap_or(json!(false)),
        "reloadable": plugin_reloadable(&scx, &name),
        "attrs": info.as_ref().and_then(|v| v.get("attrs")).cloned().unwrap_or(Value::Null),
        "config": doc,
        "note": "PUT writes the plugin TOML via P4 and hot-applies with load_config when possible.",
    });
    if !wants_reveal(req) {
        body = prepare_body(body, false);
    }
    res.render(Json(body));
    Ok(())
}

#[handler]
pub(crate) async fn auth_provider_put(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let Some(name) = req.param::<String>("name").and_then(|n| normalize_auth_name(&n).map(|s| s.to_string()))
    else {
        render_api_error(res, StatusCode::NOT_FOUND, "unknown auth provider (http|jwt)");
        return Ok(());
    };
    let name_owned = name.clone();
    mutate_doc(req, depot, res, &name, "auth_provider_update", move |doc, body| {
        require_object(body, "auth provider config")?;
        assign_object_preserving_secrets(doc, body);
        Ok(json!({"name": name_owned}))
    })
    .await
}

#[handler]
pub(crate) async fn auth_provider_test(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let Some(name) = req.param::<String>("name").and_then(|n| normalize_auth_name(&n).map(|s| s.to_string()))
    else {
        render_api_error(res, StatusCode::NOT_FOUND, "unknown auth provider (http|jwt)");
        return Ok(());
    };
    let (scx, cfg) = scx_cfg(depot)?;
    let node_id = match resolve_node(req, &scx) {
        Ok(n) => n,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let allow_private = wants_allow_private(req);
    if allow_private {
        match super::auth::identity_from_depot(depot) {
            Some(id) if id.can_admin() => {}
            Some(id) => {
                render_api_error_with(
                    res,
                    StatusCode::FORBIDDEN,
                    "forbidden",
                    Some(json!({"required_role": "admin", "role": id.role.as_str()})),
                );
                return Ok(());
            }
            None => {
                render_api_error(res, StatusCode::UNAUTHORIZED, "unauthorized");
                return Ok(());
            }
        }
    }
    let message_type = cfg.read().await.message_type;
    let doc = read_plugin_json(&scx, node_id, &name, message_type).await.unwrap_or_else(|_| json!({}));
    let body = req.parse_json::<Value>().await.unwrap_or(json!({}));
    let result = match name.as_str() {
        PLUGIN_AUTH_HTTP => test_auth_http(&doc, &body, allow_private).await,
        PLUGIN_AUTH_JWT => test_auth_jwt(&doc),
        _ => Err("unknown provider".into()),
    };
    match result {
        Ok(v) => {
            audit::record(
                req,
                depot,
                "auth_provider_test",
                Some(name),
                v["ok"].as_bool().unwrap_or(false),
                None,
            )
            .await;
            res.render(Json(v));
        }
        Err(e) => {
            audit::record(req, depot, "auth_provider_test", Some(name), false, Some(json!({"error": e})))
                .await;
            render_api_error(res, StatusCode::BAD_REQUEST, e);
        }
    }
    Ok(())
}

async fn test_auth_http(
    doc: &Value,
    body: &Value,
    allow_private: bool,
) -> std::result::Result<Value, String> {
    let url = body
        .get("url")
        .and_then(|v| v.as_str())
        .or_else(|| doc.pointer("/http_auth_req/url").and_then(|v| v.as_str()))
        .or_else(|| doc.pointer("/http_acl_req/url").and_then(|v| v.as_str()))
        .ok_or_else(|| "no URL to test (set http_auth_req.url or pass {\"url\"})".to_string())?;
    let parsed = validate_callback_url(url, allow_private)?;
    let host = parsed.host.clone();
    let port = parsed.port;
    let (ok, ms, err) = tcp_probe(&host, port, Duration::from_secs(3), allow_private).await;
    Ok(json!({
        "ok": ok,
        "kind": "tcp_connect",
        "plugin": PLUGIN_AUTH_HTTP,
        "url": redact_url_userinfo(url),
        "host": host,
        "port": port,
        "latency_ms": ms,
        "error": err,
        "note": "TCP connectivity stub only. FerroMQ does not expose an HTTP-auth handshake probe; no HTTP request is sent (SSRF).",
    }))
}

fn test_auth_jwt(doc: &Value) -> std::result::Result<Value, String> {
    let encrypt = doc.get("encrypt").and_then(|v| v.as_str()).unwrap_or("hmac-based");
    match encrypt {
        "hmac-based" => {
            let secret = doc.get("hmac_secret").and_then(|v| v.as_str()).unwrap_or("");
            if secret.is_empty() {
                return Ok(json!({
                    "ok": false,
                    "kind": "jwt_config",
                    "plugin": PLUGIN_AUTH_JWT,
                    "error": "hmac_secret is empty",
                    "note": "Local config check only. No token is verified against an IdP.",
                }));
            }
            if doc.get("hmac_base64").and_then(|v| v.as_bool()).unwrap_or(false) {
                use base64::Engine;
                if base64::engine::general_purpose::STANDARD.decode(secret).is_err()
                    && base64::engine::general_purpose::URL_SAFE.decode(secret).is_err()
                {
                    return Ok(json!({
                        "ok": false,
                        "kind": "jwt_config",
                        "plugin": PLUGIN_AUTH_JWT,
                        "error": "hmac_secret is not valid base64",
                        "note": "Local config check only.",
                    }));
                }
            }
            Ok(json!({
                "ok": true,
                "kind": "jwt_config",
                "plugin": PLUGIN_AUTH_JWT,
                "encrypt": "hmac-based",
                "note": "hmac_secret is present. This is not a live JWT issuer probe.",
            }))
        }
        "public-key" => {
            let path = doc.get("public_key").and_then(|v| v.as_str()).unwrap_or("");
            if path.is_empty() {
                return Ok(json!({
                    "ok": false,
                    "kind": "jwt_config",
                    "plugin": PLUGIN_AUTH_JWT,
                    "error": "public_key path is empty",
                    "note": "Local config check only.",
                }));
            }
            let exists = std::path::Path::new(path).is_file();
            Ok(json!({
                "ok": exists,
                "kind": "jwt_config",
                "plugin": PLUGIN_AUTH_JWT,
                "encrypt": "public-key",
                "public_key": path,
                "error": if exists { Value::Null } else { json!("public_key file not found") },
                "note": "Checks that the PEM path exists. The plugin parses RSA/EC/EdDSA on load.",
            }))
        }
        other => Err(format!("encrypt must be hmac-based or public-key, got {other}")),
    }
}

// ── Blacklist gap ────────────────────────────────────────────────────────

fn blacklist_gap() -> Value {
    json!({
        "available": false,
        "plugin": null,
        "items": [],
        "gap": "FerroMQ does not ship a dedicated blacklist or connection-policy plugin. Client/IP deny can be expressed as ferromq-acl rules with control=connect.",
        "alternatives": [
            {
                "plugin": PLUGIN_ACL,
                "how": "POST /api/v1/acl/rules with {\"access\":\"deny\",\"who\":{\"ipaddr\":\"x.x.x.x\"},\"control\":\"connect\"}",
            },
            {
                "plugin": PLUGIN_AUTH_HTTP,
                "how": "External HTTP ACL via ferromq-auth-http (http_acl_req).",
            }
        ],
    })
}

#[handler]
pub(crate) async fn blacklist_get(
    _req: &mut Request,
    _depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    res.render(Json(blacklist_gap()));
    Ok(())
}

#[handler]
pub(crate) async fn blacklist_write(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    audit::record(req, depot, "blacklist_write", None, false, Some(json!({"gap": true}))).await;
    render_api_error_with(
        res,
        StatusCode::NOT_IMPLEMENTED,
        "blacklist plugin is not available",
        Some(blacklist_gap()),
    );
    Ok(())
}

// ── Auto-subscription ────────────────────────────────────────────────────

pub(crate) fn normalize_auto_sub(input: &Value) -> std::result::Result<Value, String> {
    require_object(input, "auto-subscription")?;
    let topic_filter = input
        .get("topic_filter")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "topic_filter is required".to_string())?;
    if topic_filter.is_empty() {
        return Err("topic_filter must not be empty".into());
    }
    let qos = input
        .get("qos")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| "qos is required (0, 1, or 2)".to_string())?;
    if qos > 2 {
        return Err("qos must be 0, 1, or 2".into());
    }
    let no_local = input.get("no_local").and_then(|v| v.as_bool()).unwrap_or(false);
    let retain_as_published = input.get("retain_as_published").and_then(|v| v.as_bool()).unwrap_or(false);
    let retain_handling = input.get("retain_handling").and_then(|v| v.as_u64()).unwrap_or(0);
    if retain_handling > 2 {
        return Err("retain_handling must be 0, 1, or 2".into());
    }
    Ok(json!({
        "topic_filter": topic_filter,
        "qos": qos,
        "no_local": no_local,
        "retain_as_published": retain_as_published,
        "retain_handling": retain_handling,
    }))
}

fn indexed_list(doc: &Value, key: &str) -> Vec<Value> {
    doc.get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .enumerate()
                .map(|(i, item)| {
                    let mut v = item.clone();
                    if let Some(obj) = v.as_object_mut() {
                        obj.insert("index".into(), json!(i));
                    } else {
                        v = json!({"index": i, "value": item});
                    }
                    v
                })
                .collect()
        })
        .unwrap_or_default()
}

#[handler]
pub(crate) async fn auto_sub_get(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    list_plugin_array(req, depot, res, PLUGIN_AUTO_SUB, "subscribes").await
}

#[handler]
pub(crate) async fn auto_sub_add(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    mutate_doc(req, depot, res, PLUGIN_AUTO_SUB, "auto_subscription_add", |doc, body| {
        let item = normalize_auto_sub(body)?;
        let arr = array_mut(doc, "subscribes")?;
        arr.push(item.clone());
        Ok(json!({"item": item, "index": arr.len() - 1}))
    })
    .await
}

#[handler]
pub(crate) async fn auto_sub_update(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let index = match parse_index(req) {
        Ok(i) => i,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e);
            return Ok(());
        }
    };
    mutate_doc(req, depot, res, PLUGIN_AUTO_SUB, "auto_subscription_update", move |doc, body| {
        let item = normalize_auto_sub(body)?;
        let arr = array_mut(doc, "subscribes")?;
        if index >= arr.len() {
            return Err(format!("auto-subscription index {index} not found"));
        }
        arr[index] = item.clone();
        Ok(json!({"item": item, "index": index}))
    })
    .await
}

#[handler]
pub(crate) async fn auto_sub_delete(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let index = match parse_index(req) {
        Ok(i) => i,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e);
            return Ok(());
        }
    };
    mutate_doc(req, depot, res, PLUGIN_AUTO_SUB, "auto_subscription_delete", move |doc, _body| {
        let arr = array_mut(doc, "subscribes")?;
        if index >= arr.len() {
            return Err(format!("auto-subscription index {index} not found"));
        }
        let removed = arr.remove(index);
        Ok(json!({"removed": removed, "index": index}))
    })
    .await
}

// ── Topic rewrite ────────────────────────────────────────────────────────

pub(crate) fn normalize_rewrite(input: &Value) -> std::result::Result<Value, String> {
    require_object(input, "topic-rewrite rule")?;
    let action = input.get("action").and_then(|v| v.as_str()).unwrap_or("all").to_ascii_lowercase();
    if !REWRITE_ACTIONS.contains(&action.as_str()) {
        return Err("action must be all, publish, or subscribe".into());
    }
    let source = input
        .get("source_topic_filter")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "source_topic_filter is required".to_string())?;
    if source.is_empty() {
        return Err("source_topic_filter must not be empty".into());
    }
    let dest = input
        .get("dest_topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "dest_topic is required".to_string())?;
    if dest.is_empty() {
        return Err("dest_topic must not be empty".into());
    }
    let mut out = json!({
        "action": action,
        "source_topic_filter": source,
        "dest_topic": dest,
    });
    if let Some(re) = input.get("regex").and_then(|v| v.as_str()) {
        if !re.is_empty() {
            if unbalanced_regex_parens(re) {
                return Err("invalid regex: unbalanced parentheses".into());
            }
            out["regex"] = json!(re);
        }
    }
    Ok(out)
}

#[handler]
pub(crate) async fn topic_rewrite_get(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    list_plugin_array(req, depot, res, PLUGIN_TOPIC_REWRITE, "rules").await
}

#[handler]
pub(crate) async fn topic_rewrite_add(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    mutate_doc(req, depot, res, PLUGIN_TOPIC_REWRITE, "topic_rewrite_add", |doc, body| {
        let item = normalize_rewrite(body)?;
        let arr = array_mut(doc, "rules")?;
        let src = item["source_topic_filter"].as_str().unwrap_or_default();
        if arr.iter().any(|r| r.get("source_topic_filter").and_then(|v| v.as_str()) == Some(src)) {
            return Err(format!("duplicate source_topic_filter '{src}'"));
        }
        arr.push(item.clone());
        Ok(json!({"rule": item, "index": arr.len() - 1}))
    })
    .await
}

#[handler]
pub(crate) async fn topic_rewrite_update(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let index = match parse_index(req) {
        Ok(i) => i,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e);
            return Ok(());
        }
    };
    mutate_doc(req, depot, res, PLUGIN_TOPIC_REWRITE, "topic_rewrite_update", move |doc, body| {
        let item = normalize_rewrite(body)?;
        let arr = array_mut(doc, "rules")?;
        if index >= arr.len() {
            return Err(format!("topic-rewrite index {index} not found"));
        }
        let src = item["source_topic_filter"].as_str().unwrap_or_default();
        if arr
            .iter()
            .enumerate()
            .any(|(i, r)| i != index && r.get("source_topic_filter").and_then(|v| v.as_str()) == Some(src))
        {
            return Err(format!("duplicate source_topic_filter '{src}'"));
        }
        arr[index] = item.clone();
        Ok(json!({"rule": item, "index": index}))
    })
    .await
}

#[handler]
pub(crate) async fn topic_rewrite_delete(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let index = match parse_index(req) {
        Ok(i) => i,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e);
            return Ok(());
        }
    };
    mutate_doc(req, depot, res, PLUGIN_TOPIC_REWRITE, "topic_rewrite_delete", move |doc, _body| {
        let arr = array_mut(doc, "rules")?;
        if index >= arr.len() {
            return Err(format!("topic-rewrite index {index} not found"));
        }
        let removed = arr.remove(index);
        Ok(json!({"removed": removed, "index": index}))
    })
    .await
}

async fn list_plugin_array(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
    plugin: &str,
    key: &str,
) -> std::result::Result<(), salvo::Error> {
    if deny_reveal_if_needed(req, depot, res) {
        return Ok(());
    }
    let (scx, cfg) = scx_cfg(depot)?;
    let node_id = match resolve_node(req, &scx) {
        Ok(n) => n,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let message_type = cfg.read().await.message_type;
    match read_plugin_json(&scx, node_id, plugin, message_type).await {
        Ok(doc) => {
            let mut items = indexed_list(&doc, key);
            if !wants_reveal(req) {
                items = items.into_iter().map(|v| prepare_body(v, false)).collect();
            }
            let info = futures_placeholder_info(&scx, plugin);
            let body = json!({
                "plugin": plugin,
                "node": node_id,
                "available": info.0,
                "active": info.1,
                "inited": info.2,
                "reloadable": plugin_reloadable(&scx, plugin),
                "items": items,
            });
            res.render(Json(prepare_body(body, wants_reveal(req))));
        }
        Err(e) => {
            let status = if e.to_string().contains("not found") {
                StatusCode::NOT_FOUND
            } else {
                status_for_plugin_error(&e)
            };
            render_api_error(res, status, e.to_string());
        }
    }
    Ok(())
}

// ── Webhooks ─────────────────────────────────────────────────────────────

fn webhook_urls(doc: &Value) -> Vec<String> {
    doc.get("urls")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
        .unwrap_or_default()
}

fn webhook_rules_flat(doc: &Value) -> Vec<Value> {
    let Some(rule) = doc.get("rule").and_then(|v| v.as_object()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (hook, rules) in rule {
        if let Some(arr) = rules.as_array() {
            for (i, r) in arr.iter().enumerate() {
                let mut item = r.clone();
                if !item.is_object() {
                    item = json!({"value": r});
                }
                if let Some(obj) = item.as_object_mut() {
                    obj.insert("hook".into(), json!(hook));
                    obj.insert("index".into(), json!(i));
                }
                out.push(item);
            }
        }
    }
    out.sort_by(|a, b| {
        let ha = a.get("hook").and_then(|v| v.as_str()).unwrap_or("");
        let hb = b.get("hook").and_then(|v| v.as_str()).unwrap_or("");
        ha.cmp(hb)
    });
    out
}

pub(crate) fn normalize_webhook_url(raw: &str) -> std::result::Result<String, String> {
    let raw = raw.trim();
    if raw.starts_with("file://") {
        if raw.len() <= "file://".len() {
            return Err("file URL is missing a path".into());
        }
        return Ok(raw.to_string());
    }
    let _ = validate_callback_url(raw, true)?;
    Ok(raw.to_string())
}

pub(crate) fn normalize_webhook_rule(input: &Value) -> std::result::Result<(String, Value), String> {
    require_object(input, "webhook rule")?;
    let hook = input
        .get("hook")
        .or_else(|| input.get("action"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| "hook (or action) is required".to_string())?
        .to_ascii_lowercase();
    if hook.is_empty() {
        return Err("hook must not be empty".into());
    }
    if !WEBHOOK_HOOKS.contains(&hook.as_str()) && hook.chars().any(|c| !c.is_ascii_alphanumeric() && c != '_')
    {
        return Err("hook must be a known event or snake_case identifier".into());
    }
    let action = input.get("action").and_then(|v| v.as_str()).unwrap_or(hook.as_str());
    let mut rule = json!({ "action": action });
    if let Some(urls) = input.get("urls") {
        let arr = urls.as_array().ok_or_else(|| "urls must be an array".to_string())?;
        let mut out = Vec::new();
        for u in arr {
            let s = u.as_str().ok_or_else(|| "each url must be a string".to_string())?;
            out.push(normalize_webhook_url(s)?);
        }
        if !out.is_empty() {
            rule["urls"] = json!(out);
        }
    }
    if let Some(topics) = input.get("topics") {
        let arr = topics.as_array().ok_or_else(|| "topics must be an array".to_string())?;
        let topics: Vec<String> = arr
            .iter()
            .map(|t| {
                t.as_str().map(|s| s.to_string()).ok_or_else(|| "each topic must be a string".to_string())
            })
            .collect::<std::result::Result<_, _>>()?;
        if !topics.is_empty() {
            rule["topics"] = json!(topics);
        }
    }
    Ok((hook, rule))
}

#[handler]
pub(crate) async fn webhooks_get(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    if deny_reveal_if_needed(req, depot, res) {
        return Ok(());
    }
    let (scx, cfg) = scx_cfg(depot)?;
    let node_id = match resolve_node(req, &scx) {
        Ok(n) => n,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let message_type = cfg.read().await.message_type;
    match read_plugin_json(&scx, node_id, PLUGIN_WEBHOOK, message_type).await {
        Ok(doc) => {
            let info = plugin_info_json(&scx, PLUGIN_WEBHOOK).await;
            let mut body = json!({
                "plugin": PLUGIN_WEBHOOK,
                "node": node_id,
                "available": info.is_some(),
                "active": info.as_ref().and_then(|v| v.get("active")).cloned().unwrap_or(json!(false)),
                "inited": info.as_ref().and_then(|v| v.get("inited")).cloned().unwrap_or(json!(false)),
                "reloadable": plugin_reloadable(&scx, PLUGIN_WEBHOOK),
                "attrs": info.as_ref().and_then(|v| v.get("attrs")).cloned().unwrap_or(Value::Null),
                "queue_capacity": doc.get("queue_capacity"),
                "concurrency_limit": doc.get("concurrency_limit"),
                "http_timeout": doc.get("http_timeout"),
                "retry_max_elapsed_time": doc.get("retry_max_elapsed_time"),
                "retry_multiplier": doc.get("retry_multiplier"),
                "urls": webhook_urls(&doc),
                "rules": webhook_rules_flat(&doc),
                "note": "queue_capacity and concurrency_limit require a plugin restart to take effect (plugin docs).",
            });
            body = prepare_body(body, wants_reveal(req));
            res.render(Json(body));
        }
        Err(e) => {
            let status = if e.to_string().contains("not found") {
                StatusCode::NOT_FOUND
            } else {
                status_for_plugin_error(&e)
            };
            render_api_error(res, status, e.to_string());
        }
    }
    Ok(())
}

#[handler]
pub(crate) async fn webhooks_put(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    mutate_doc(req, depot, res, PLUGIN_WEBHOOK, "webhook_config_update", |doc, body| {
        require_object(body, "webhook config")?;
        for key in [
            "queue_capacity",
            "concurrency_limit",
            "http_timeout",
            "retry_max_elapsed_time",
            "retry_multiplier",
        ] {
            if let Some(v) = body.get(key) {
                doc[key] = v.clone();
            }
        }
        if let Some(urls) = body.get("urls") {
            let arr = urls.as_array().ok_or_else(|| "urls must be an array".to_string())?;
            let existing = doc.get("urls").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            let mut out = Vec::new();
            for (i, u) in arr.iter().enumerate() {
                let incoming = u.as_str().ok_or_else(|| "each url must be a string".to_string())?;
                let prev = existing.get(i).and_then(|v| v.as_str());
                out.push(restore_webhook_url(prev, incoming)?);
            }
            doc["urls"] = json!(out);
        }
        Ok(json!({"plugin": PLUGIN_WEBHOOK}))
    })
    .await
}

#[handler]
pub(crate) async fn webhook_urls_add(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    mutate_doc(req, depot, res, PLUGIN_WEBHOOK, "webhook_url_add", |doc, body| {
        let raw = body
            .get("url")
            .and_then(|v| v.as_str())
            .or_else(|| body.as_str())
            .ok_or_else(|| "url is required".to_string())?;
        let url = restore_webhook_url(None, raw)?;
        let arr = array_mut(doc, "urls")?;
        if arr.iter().any(|v| v.as_str() == Some(url.as_str())) {
            return Err("url already present".into());
        }
        arr.push(json!(url.clone()));
        Ok(json!({"url": redact_url_userinfo(&url), "index": arr.len() - 1}))
    })
    .await
}

#[handler]
pub(crate) async fn webhook_urls_delete(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let index = match parse_index(req) {
        Ok(i) => i,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e);
            return Ok(());
        }
    };
    mutate_doc(req, depot, res, PLUGIN_WEBHOOK, "webhook_url_delete", move |doc, _body| {
        let arr = array_mut(doc, "urls")?;
        if index >= arr.len() {
            return Err(format!("webhook url index {index} not found"));
        }
        let removed = arr.remove(index);
        Ok(json!({"removed": removed, "index": index}))
    })
    .await
}

#[handler]
pub(crate) async fn webhook_rules_add(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    mutate_doc(req, depot, res, PLUGIN_WEBHOOK, "webhook_rule_add", |doc, body| {
        let (hook, rule) = normalize_webhook_rule(body)?;
        if doc.get("rule").is_none() {
            doc["rule"] = json!({});
        }
        let rule_map = doc.get_mut("rule").and_then(|v| v.as_object_mut()).ok_or("rule must be a table")?;
        let entry = rule_map.entry(hook.clone()).or_insert_with(|| json!([]));
        let arr = entry.as_array_mut().ok_or("rule hook must be an array")?;
        arr.push(rule.clone());
        Ok(json!({"hook": hook, "index": arr.len() - 1, "rule": rule}))
    })
    .await
}

#[handler]
pub(crate) async fn webhook_rules_update(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let hook = match req.param::<String>("hook") {
        Some(h) => h,
        None => {
            render_api_error(res, StatusCode::BAD_REQUEST, "hook not found");
            return Ok(());
        }
    };
    let index = match parse_index(req) {
        Ok(i) => i,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e);
            return Ok(());
        }
    };
    mutate_doc(req, depot, res, PLUGIN_WEBHOOK, "webhook_rule_update", move |doc, body| {
        let mut body = body.clone();
        if body.get("hook").is_none() {
            if let Some(obj) = body.as_object_mut() {
                obj.insert("hook".into(), json!(hook.clone()));
            }
        }
        let (new_hook, rule) = normalize_webhook_rule(&body)?;
        let rule_map = doc.get_mut("rule").and_then(|v| v.as_object_mut()).ok_or("no webhook rules")?;
        let arr = rule_map
            .get_mut(&hook)
            .and_then(|v| v.as_array_mut())
            .ok_or_else(|| format!("webhook hook '{hook}' not found"))?;
        if index >= arr.len() {
            return Err(format!("webhook rule {hook}/{index} not found"));
        }
        if new_hook != hook {
            arr.remove(index);
            let dest = rule_map.entry(new_hook.clone()).or_insert_with(|| json!([]));
            dest.as_array_mut().ok_or("rule hook must be an array")?.push(rule.clone());
        } else {
            arr[index] = rule.clone();
        }
        Ok(json!({"hook": new_hook, "index": index, "rule": rule}))
    })
    .await
}

#[handler]
pub(crate) async fn webhook_rules_delete(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let hook = match req.param::<String>("hook") {
        Some(h) => h,
        None => {
            render_api_error(res, StatusCode::BAD_REQUEST, "hook not found");
            return Ok(());
        }
    };
    let index = match parse_index(req) {
        Ok(i) => i,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e);
            return Ok(());
        }
    };
    mutate_doc(req, depot, res, PLUGIN_WEBHOOK, "webhook_rule_delete", move |doc, _body| {
        let rule_map = doc.get_mut("rule").and_then(|v| v.as_object_mut()).ok_or("no webhook rules")?;
        let arr = rule_map
            .get_mut(&hook)
            .and_then(|v| v.as_array_mut())
            .ok_or_else(|| format!("webhook hook '{hook}' not found"))?;
        if index >= arr.len() {
            return Err(format!("webhook rule {hook}/{index} not found"));
        }
        let removed = arr.remove(index);
        Ok(json!({"removed": removed, "hook": hook, "index": index}))
    })
    .await
}

#[handler]
pub(crate) async fn webhook_test(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = scx_cfg(depot)?;
    let node_id = match resolve_node(req, &scx) {
        Ok(n) => n,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let allow_private = wants_allow_private(req);
    if allow_private {
        match super::auth::identity_from_depot(depot) {
            Some(id) if id.can_admin() => {}
            Some(id) => {
                render_api_error_with(
                    res,
                    StatusCode::FORBIDDEN,
                    "forbidden",
                    Some(json!({"required_role": "admin", "role": id.role.as_str()})),
                );
                return Ok(());
            }
            None => {
                render_api_error(res, StatusCode::UNAUTHORIZED, "unauthorized");
                return Ok(());
            }
        }
    }
    let message_type = cfg.read().await.message_type;
    let doc =
        read_plugin_json(&scx, node_id, PLUGIN_WEBHOOK, message_type).await.unwrap_or_else(|_| json!({}));
    let body = req.parse_json::<Value>().await.unwrap_or(json!({}));
    let fallback = webhook_urls(&doc).into_iter().find(|u| u.starts_with("http"));
    let url = body
        .get("url")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or(fallback)
        .ok_or_else(|| "no http(s) url to test".to_string());
    let url = match url {
        Ok(u) => u,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e);
            return Ok(());
        }
    };
    let parsed = match validate_callback_url(&url, allow_private) {
        Ok(u) => u,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e);
            return Ok(());
        }
    };
    let host = parsed.host.clone();
    let port = parsed.port;
    let (ok, ms, err) = tcp_probe(&host, port, Duration::from_secs(3), allow_private).await;
    let result = json!({
        "ok": ok,
        "kind": "tcp_connect",
        "plugin": PLUGIN_WEBHOOK,
        "url": redact_url_userinfo(&url),
        "host": host,
        "port": port,
        "latency_ms": ms,
        "error": err,
        "note": "TCP connectivity stub. No HTTP POST is sent (SSRF). Use allow_private=1 as admin to test loopback.",
    });
    audit::record(req, depot, "webhook_test", Some(redact_url_userinfo(&url)), ok, None).await;
    res.render(Json(result));
    Ok(())
}

// ── Bridges ──────────────────────────────────────────────────────────────

pub(crate) fn is_bridge_plugin(name: &str) -> bool {
    name == "ferromq-bridge-origin" || name.starts_with("ferromq-bridge-")
}

pub(crate) fn bridge_kind(name: &str) -> Value {
    if name == "ferromq-bridge-origin" {
        return json!({"direction": "origin", "transport": "origin"});
    }
    let rest = name.strip_prefix("ferromq-bridge-").unwrap_or(name);
    let (direction, transport) = if let Some(t) = rest.strip_prefix("egress-") {
        ("egress", t)
    } else if let Some(t) = rest.strip_prefix("ingress-") {
        ("ingress", t)
    } else {
        ("unknown", rest)
    };
    json!({"direction": direction, "transport": transport})
}

async fn collect_bridge_names(scx: &ServerContext) -> Vec<String> {
    let mut names: Vec<String> = KNOWN_BRIDGES.iter().map(|s| (*s).to_string()).collect();
    if let Ok(list) = plugin::get_plugins(scx).await {
        for p in list {
            if is_bridge_plugin(&p.name) && !names.iter().any(|n| n == &p.name) {
                names.push(p.name);
            }
        }
    }
    if let Ok(dir) = scx.plugins.config_dir().ok_or(()) {
        if let Ok(rd) = std::fs::read_dir(dir) {
            for ent in rd.flatten() {
                let fname = ent.file_name();
                let fname = fname.to_string_lossy();
                if let Some(stem) = fname.strip_suffix(".toml") {
                    if is_bridge_plugin(stem) && !names.iter().any(|n| n == stem) {
                        names.push(stem.to_string());
                    }
                }
            }
        }
    }
    names.sort();
    names
}

#[handler]
pub(crate) async fn bridges_list(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, _cfg) = scx_cfg(depot)?;
    let node_id = match resolve_node(req, &scx) {
        Ok(n) => n,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let names = collect_bridge_names(&scx).await;
    let mut items = Vec::new();
    for name in names {
        let info = plugin_info_json(&scx, &name).await;
        items.push(json!({
            "name": name,
            "kind": bridge_kind(&name),
            "node": node_id,
            "available": info.is_some(),
            "active": info.as_ref().and_then(|v| v.get("active")).cloned().unwrap_or(json!(false)),
            "inited": info.as_ref().and_then(|v| v.get("inited")).cloned().unwrap_or(json!(false)),
            "immutable": info.as_ref().and_then(|v| v.get("immutable")).cloned().unwrap_or(json!(false)),
            "reloadable": plugin_reloadable(&scx, &name),
            "attrs": info.as_ref().and_then(|v| v.get("attrs")).cloned().unwrap_or(Value::Null),
        }));
    }
    res.render(Json(json!({
        "node": node_id,
        "items": items,
        "note": "Status/errors come from each bridge plugin's attrs() when loaded. PUT uses the P4 plugin-config write path.",
    })));
    Ok(())
}

#[handler]
pub(crate) async fn bridge_get(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    if deny_reveal_if_needed(req, depot, res) {
        return Ok(());
    }
    let Some(name) = req.param::<String>("plugin") else {
        render_api_error(res, StatusCode::BAD_REQUEST, "plugin not found");
        return Ok(());
    };
    if !is_bridge_plugin(&name) {
        render_api_error(res, StatusCode::NOT_FOUND, format!("not a bridge plugin: {name}"));
        return Ok(());
    }
    let (scx, cfg) = scx_cfg(depot)?;
    let node_id = match resolve_node(req, &scx) {
        Ok(n) => n,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let message_type = cfg.read().await.message_type;
    let info = plugin_info_json(&scx, &name).await;
    let doc = match read_plugin_json(&scx, node_id, &name, message_type).await {
        Ok(v) => Some(v),
        Err(e) if e.to_string().contains("not found") => None,
        Err(e) => {
            render_api_error(res, status_for_plugin_error(&e), e.to_string());
            return Ok(());
        }
    };
    if info.is_none() && doc.is_none() {
        render_api_error(res, StatusCode::NOT_FOUND, format!("bridge not found: {name}"));
        return Ok(());
    }
    let mut body = json!({
        "name": name,
        "kind": bridge_kind(&name),
        "node": node_id,
        "available": info.is_some(),
        "active": info.as_ref().and_then(|v| v.get("active")).cloned().unwrap_or(json!(false)),
        "inited": info.as_ref().and_then(|v| v.get("inited")).cloned().unwrap_or(json!(false)),
        "immutable": info.as_ref().and_then(|v| v.get("immutable")).cloned().unwrap_or(json!(false)),
        "reloadable": plugin_reloadable(&scx, &name),
        "attrs": info.as_ref().and_then(|v| v.get("attrs")).cloned().unwrap_or(Value::Null),
        "config": doc.unwrap_or(json!({})),
        "note": "PUT writes {plugin}.toml via P4. load/unload call the existing plugin start/stop hooks.",
    });
    body = prepare_body(body, wants_reveal(req));
    res.render(Json(body));
    Ok(())
}

#[handler]
pub(crate) async fn bridge_put(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let Some(name) = req.param::<String>("plugin") else {
        render_api_error(res, StatusCode::BAD_REQUEST, "plugin not found");
        return Ok(());
    };
    if !is_bridge_plugin(&name) {
        render_api_error(res, StatusCode::NOT_FOUND, format!("not a bridge plugin: {name}"));
        return Ok(());
    }
    let name_owned = name.clone();
    mutate_doc(req, depot, res, &name, "bridge_config_update", move |doc, body| {
        require_object(body, "bridge config")?;
        assign_object_preserving_secrets(doc, body);
        Ok(json!({"name": name_owned, "kind": bridge_kind(&name_owned)}))
    })
    .await
}

#[handler]
pub(crate) async fn bridge_load(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    bridge_toggle(req, depot, res, true).await
}

#[handler]
pub(crate) async fn bridge_unload(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    bridge_toggle(req, depot, res, false).await
}

async fn bridge_toggle(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
    load: bool,
) -> std::result::Result<(), salvo::Error> {
    let Some(name) = req.param::<String>("plugin") else {
        render_api_error(res, StatusCode::BAD_REQUEST, "plugin not found");
        return Ok(());
    };
    if !is_bridge_plugin(&name) {
        render_api_error(res, StatusCode::NOT_FOUND, format!("not a bridge plugin: {name}"));
        return Ok(());
    }
    let (scx, cfg) = scx_cfg(depot)?;
    let node_id = match resolve_node(req, &scx) {
        Ok(n) => n,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let message_type = cfg.read().await.message_type;
    let action = if load { "bridge_load" } else { "bridge_unload" };
    match load_or_unload_plugin(&scx, node_id, &name, load, message_type).await {
        Ok(ok) => {
            audit::record(req, depot, action, Some(format!("{node_id}/{name}")), ok, None).await;
            res.render(Json(json!({"ok": ok, "name": name, "node": node_id, "loaded": load})));
        }
        Err(e) => {
            audit::record(
                req,
                depot,
                action,
                Some(format!("{node_id}/{name}")),
                false,
                Some(json!({"error": e.to_string()})),
            )
            .await;
            render_api_error(res, status_for_plugin_error(&e), e.to_string());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acl_rule_structured_and_raw() {
        let structured = json!({
            "access": "allow",
            "who": {"user": "dashboard", "password": "s3cret"},
            "control": "subscribe",
            "topics": ["$SYS/#"]
        });
        let raw = normalize_acl_rule(&structured).unwrap();
        assert_eq!(raw[0], "allow");
        assert_eq!(raw[1]["user"], "dashboard");
        assert_eq!(raw[2], "subscribe");

        let again = normalize_acl_rule(&raw).unwrap();
        assert_eq!(again[0], "allow");

        assert!(normalize_acl_rule(&json!({"access": "maybe", "who": "all"})).is_err());
        assert!(normalize_acl_rule(&json!(["allow"])).is_err());
    }

    #[test]
    fn acl_who_and_topics_validate() {
        assert!(validate_acl_who(&json!("all")).is_ok());
        assert!(validate_acl_who(&json!({"ipaddr": "10.0.0.1"})).is_ok());
        assert!(validate_acl_who(&json!({"nope": 1})).is_err());
        assert!(validate_acl_topics(&json!(["a/b", {"eq": "#"}])).is_ok());
        assert!(validate_acl_topics(&json!([1])).is_err());
    }

    #[test]
    fn auto_sub_and_rewrite_validate() {
        let sub = normalize_auto_sub(&json!({"topic_filter": "x/#", "qos": 1})).unwrap();
        assert_eq!(sub["qos"], 1);
        assert!(normalize_auto_sub(&json!({"topic_filter": "x/#", "qos": 9})).is_err());

        let rw = normalize_rewrite(&json!({
            "action": "all",
            "source_topic_filter": "x/+/#",
            "dest_topic": "xx/$1/$2",
            "regex": "^x/(.+)/(.+)$"
        }))
        .unwrap();
        assert_eq!(rw["action"], "all");
        assert!(normalize_rewrite(&json!({
            "source_topic_filter": "a",
            "dest_topic": "b",
            "regex": "("
        }))
        .is_err());
    }

    #[test]
    fn webhook_url_and_rule() {
        assert!(normalize_webhook_url("https://hooks.example.com/mqtt").is_ok());
        assert!(normalize_webhook_url("file:///var/log/ferromq/hook.log").is_ok());
        assert!(normalize_webhook_url("gopher://x").is_err());
        let (hook, rule) = normalize_webhook_rule(&json!({
            "hook": "message_publish",
            "action": "message_publish",
            "topics": ["#"]
        }))
        .unwrap();
        assert_eq!(hook, "message_publish");
        assert_eq!(rule["topics"][0], "#");
    }

    #[test]
    fn ssrf_blocks_private_and_metadata() {
        assert!(validate_callback_url("https://example.com/hook", false).is_ok());
        assert!(validate_callback_url("http://127.0.0.1:8080/x", false).is_err());
        assert!(validate_callback_url("http://10.0.0.1/x", false).is_err());
        assert!(validate_callback_url("http://169.254.169.254/latest", false).is_err());
        assert!(validate_callback_url("http://metadata.google.internal/", false).is_err());
        assert!(validate_callback_url("file:///etc/passwd", false).is_err());
        assert!(validate_callback_url("http://127.0.0.1:8080/x", true).is_ok());
        assert!(is_blocked_ip("192.168.1.1".parse().unwrap()));
        assert!(!is_blocked_ip("1.1.1.1".parse().unwrap()));
        // tcp_probe: lookup_host → reject_blocked_resolved_ips → TcpStream::connect.
        // Unit-test the resolve-then-check helper (no live DNS).
        assert!(reject_blocked_resolved_ips(["127.0.0.1".parse().unwrap()], false).is_err());
        assert!(reject_blocked_resolved_ips(["127.0.0.1".parse().unwrap()], true).is_ok());
        assert!(reject_blocked_resolved_ips(["1.1.1.1".parse().unwrap()], false).is_ok());
        assert!(reject_blocked_resolved_ips(["169.254.169.254".parse().unwrap()], false).is_err());
    }

    #[test]
    fn acl_update_preserves_omitted_or_redacted_password() {
        let existing = json!(["allow", {"user": "dashboard", "password": "s3cret"}, "subscribe", ["$SYS/#"]]);
        let omitted = normalize_acl_rule(&json!({
            "access": "allow",
            "who": {"user": "dashboard"},
            "control": "subscribe",
            "topics": ["$SYS/#"]
        }))
        .unwrap();
        let merged = merge_plugin_patch(existing.clone(), omitted).unwrap();
        assert_eq!(merged[1]["password"], "s3cret");

        let redacted = normalize_acl_rule(&json!({
            "access": "allow",
            "who": {"user": "dashboard", "password": "***"},
            "control": "subscribe",
            "topics": ["$SYS/#"]
        }))
        .unwrap();
        let merged = merge_plugin_patch(existing, redacted).unwrap();
        assert_eq!(merged[1]["password"], "s3cret");
        assert_ne!(merged[1]["password"], "***");
    }

    #[test]
    fn auth_and_bridge_put_skip_redacted_secrets() {
        let mut jwt = json!({"hmac_secret": "keep", "encrypt": "hmac-based"});
        assign_object_preserving_secrets(&mut jwt, &json!({"hmac_secret": "***", "hmac_base64": true}));
        assert_eq!(jwt["hmac_secret"], "keep");
        assert_eq!(jwt["hmac_base64"], true);

        let mut bridge = json!({"password": "old", "server": "mqtt.example.com"});
        assign_object_preserving_secrets(
            &mut bridge,
            &json!({"password": "***", "server": "other.example.com"}),
        );
        assert_eq!(bridge["password"], "old");
        assert_eq!(bridge["server"], "other.example.com");
    }

    #[test]
    fn webhook_url_restores_redacted_userinfo() {
        let prev = "https://user:hunter2@hooks.example.com/mqtt";
        let incoming = redact_url_userinfo(prev);
        let restored = restore_webhook_url(Some(prev), &incoming).unwrap();
        assert_eq!(restored, prev);
        assert!(restore_webhook_url(None, &incoming).is_err());
    }

    #[test]
    fn redact_userinfo_in_url() {
        let r = redact_url_userinfo("https://user:hunter2@hooks.example.com/x");
        assert!(!r.contains("hunter2"));
        assert!(r.contains("***"));
    }

    #[test]
    fn bridge_name_and_kind() {
        assert!(is_bridge_plugin("ferromq-bridge-egress-mqtt"));
        assert!(is_bridge_plugin("ferromq-bridge-origin"));
        assert!(!is_bridge_plugin("ferromq-acl"));
        assert_eq!(bridge_kind("ferromq-bridge-egress-mqtt")["direction"], "egress");
        assert_eq!(bridge_kind("ferromq-bridge-ingress-kafka")["transport"], "kafka");
        assert_eq!(bridge_kind("ferromq-bridge-origin")["direction"], "origin");
    }

    #[test]
    fn prepare_body_redacts_password_and_url() {
        let v = json!({
            "who": {"user": "u", "password": "p"},
            "urls": ["https://user:secret@example.com/h"]
        });
        let r = prepare_body(v, false);
        assert_eq!(r["who"]["password"], "***");
        assert!(!r["urls"][0].as_str().unwrap().contains("secret"));
    }
}
