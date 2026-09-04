//! In-memory audit log for dashboard / HTTP API write operations.
//!
//! P3b: records config-ish writes (kick, publish, plugin load/unload/reload,
//! password change, API key create/delete, user create/disable) plus optional
//! login failures. Events live in a process-local ring buffer. When
//! `audit_file` is set they are also appended as JSON Lines. `GET /api/v1/audit`
//! is admin-only and supports the same `_limit` / `_offset` / `format=page`
//! paging as other list endpoints.

use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use salvo::http::StatusCode;
use salvo::prelude::*;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::RwLock;

use super::auth::{identity_from_depot, AuthIdentity, Role, AUTH_IDENTITY};
use super::config::PluginConfig;
use super::response::{render_api_error, render_list, ListPaging, DEPOT_REQUEST_ID};
use super::PluginConfigType;

/// Depot key for [`AuditLog`].
pub(crate) const AUDIT_LOG: &str = "AUDIT_LOG";

const DEFAULT_MAX_EVENTS: usize = 10_000;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct AuditEvent {
    pub id: u64,
    pub ts: i64,
    pub request_id: String,
    pub username: String,
    pub role: String,
    pub auth: String,
    pub action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
    pub ip: String,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

/// Ring-buffer audit store with optional JSONL persistence.
pub(crate) struct AuditLog {
    events: RwLock<VecDeque<AuditEvent>>,
    next_id: AtomicU64,
    max_events: usize,
    file: Option<PathBuf>,
}

impl AuditLog {
    pub(crate) fn new(max_events: usize, file: Option<PathBuf>) -> Self {
        Self {
            events: RwLock::new(VecDeque::new()),
            next_id: AtomicU64::new(1),
            max_events: max_events.max(1),
            file,
        }
    }

    pub(crate) fn from_config(cfg: &PluginConfig) -> Self {
        let file = cfg.audit_file.as_deref().and_then(|s| {
            let t = s.trim();
            if t.is_empty() {
                None
            } else {
                Some(PathBuf::from(t))
            }
        });
        Self::new(cfg.audit_max_events, file)
    }

    pub(crate) async fn record(&self, event: AuditEvent) {
        if let Some(path) = &self.file {
            append_jsonl(path, &event);
        }
        let mut events = self.events.write().await;
        events.push_back(event);
        while events.len() > self.max_events {
            events.pop_front();
        }
    }

    pub(crate) async fn query(
        &self,
        action: Option<&str>,
        username: Option<&str>,
        success: Option<bool>,
    ) -> Vec<AuditEvent> {
        let events = self.events.read().await;
        events
            .iter()
            .rev()
            .filter(|e| action.is_none_or(|a| e.action == a))
            .filter(|e| username.is_none_or(|u| e.username == u))
            .filter(|e| success.is_none_or(|s| e.success == s))
            .cloned()
            .collect()
    }
}

fn append_jsonl(path: &PathBuf, event: &AuditEvent) {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            let _ = std::fs::create_dir_all(parent);
        }
    }
    let Ok(mut f) = OpenOptions::new().create(true).append(true).open(path) else {
        log::warn!("audit: cannot open {} for append", path.display());
        return;
    };
    match serde_json::to_string(event) {
        Ok(line) => {
            if let Err(e) = writeln!(f, "{line}") {
                log::warn!("audit: write {}: {e}", path.display());
            }
        }
        Err(e) => log::warn!("audit: serialize event: {e}"),
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn request_id_from_depot(depot: &Depot) -> String {
    depot.get::<String>(DEPOT_REQUEST_ID).ok().cloned().unwrap_or_else(|| "unknown".into())
}

fn client_ip(req: &Request) -> String {
    req.remote_addr().ip().map(|ip| ip.to_string()).unwrap_or_else(|| "unknown".into())
}

fn audit_log(depot: &Depot) -> Option<Arc<AuditLog>> {
    depot.get::<Arc<AuditLog>>(AUDIT_LOG).ok().cloned()
}

/// Record an audit event using the request identity when present.
pub(crate) async fn record(
    req: &Request,
    depot: &Depot,
    action: &str,
    resource: Option<String>,
    success: bool,
    details: Option<Value>,
) {
    let id = identity_from_depot(depot).or_else(|| depot.get::<AuthIdentity>(AUTH_IDENTITY).ok().cloned());
    let (username, role, auth) = match &id {
        Some(id) => (id.username.clone(), id.role.as_str().to_string(), id.source.as_str().to_string()),
        None => ("anonymous".into(), Role::Admin.as_str().to_string(), "anonymous".into()),
    };
    record_raw(req, depot, &username, &role, &auth, action, resource, success, details).await;
}

/// Record without requiring a resolved identity (login failure, etc.).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn record_raw(
    req: &Request,
    depot: &Depot,
    username: &str,
    role: &str,
    auth: &str,
    action: &str,
    resource: Option<String>,
    success: bool,
    details: Option<Value>,
) {
    let Some(log) = audit_log(depot) else {
        return;
    };
    let event = AuditEvent {
        id: log.next_id.fetch_add(1, Ordering::Relaxed),
        ts: now_millis(),
        request_id: request_id_from_depot(depot),
        username: username.to_string(),
        role: role.to_string(),
        auth: auth.to_string(),
        action: action.to_string(),
        resource,
        ip: client_ip(req),
        success,
        details,
    };
    log.record(event).await;
}

/// `GET /api/v1/audit` — admin only; newest first.
#[handler]
pub(crate) async fn list_audit(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let Some(log) = audit_log(depot) else {
        render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "audit not configured");
        return;
    };
    let max_row_limit = match depot.obtain::<(ferromq::context::ServerContext, PluginConfigType)>() {
        Ok((_, cfg)) => cfg.read().await.max_row_limit,
        Err(_) => DEFAULT_MAX_EVENTS,
    };
    let action = req.query::<String>("action").filter(|s| !s.is_empty());
    let username = req.query::<String>("username").filter(|s| !s.is_empty());
    let success = req.query::<String>("success").and_then(|s| match s.as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    });
    let all = log.query(action.as_deref(), username.as_deref(), success).await;
    let total = all.len();
    let requested = req.query::<usize>("_limit").unwrap_or(0);
    let paging = ListPaging::from_request(req, requested, max_row_limit);
    let (page, truncated) = paging.apply(all);
    render_list(req, res, page, paging, truncated, Some(total));
}
