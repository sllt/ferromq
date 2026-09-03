//! Dashboard session authentication for the HTTP API plugin.
//!
//! P3a: username/password login with an HttpOnly session cookie, bcrypt
//! password hashes, in-memory user + session stores, idle/absolute expiry,
//! and a simple per-IP login rate limit.
//!
//! P3b: hashed API keys (Bearer), admin user CRUD, and three roles
//! (`admin` / `operator` / `viewer`). Compatibility:
//! `Authorization: Bearer <http_bearer_token>` remains a superuser
//! credential (username `operator`, role `admin`). MQTT client auth
//! plugins are not involved — this module is scoped to http-api only.
//!
//! Cluster limitation: users, sessions, and API keys live in process
//! memory on this node. Restarting the HTTP server drops them (bootstrap
//! from config again). A load-balanced cluster needs sticky sessions.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rand::RngCore;
use salvo::http::cookie::{Cookie, SameSite};
use salvo::http::StatusCode;
use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

use super::audit;
use super::config::PluginConfig;
use super::response::{render_api_error, render_api_error_with, render_list, ListPaging};
use super::PluginConfigType;

/// Depot key for [`AuthState`].
pub(crate) const AUTH_STATE: &str = "AUTH_STATE";
/// Depot key for the resolved [`AuthIdentity`] of this request.
pub(crate) const AUTH_IDENTITY: &str = "AUTH_IDENTITY";

/// Username reported for a valid `http_bearer_token`.
pub(crate) const BEARER_USERNAME: &str = "operator";
/// Username reported when the API is open (no bearer, no dashboard users).
pub(crate) const ANONYMOUS_USERNAME: &str = "anonymous";

const MIN_PASSWORD_LEN: usize = 8;
const MAX_USERNAME_LEN: usize = 64;
const API_KEY_PREFIX: &str = "fmqk_";

/// Dashboard / API-key role.
///
/// * `admin` — users, API keys, audit, plus every operator action.
/// * `operator` — kick / publish / plugins; cannot manage users or keys.
/// * `viewer` — read-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Role {
    Admin,
    Operator,
    Viewer,
}

impl Role {
    pub(crate) fn can_write(self) -> bool {
        matches!(self, Role::Admin | Role::Operator)
    }

    pub(crate) fn can_admin(self) -> bool {
        matches!(self, Role::Admin)
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Role::Admin => "admin",
            Role::Operator => "operator",
            Role::Viewer => "viewer",
        }
    }

    pub(crate) fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "admin" => Some(Role::Admin),
            "operator" => Some(Role::Operator),
            "viewer" => Some(Role::Viewer),
            _ => None,
        }
    }
}

/// How the request was authenticated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AuthSource {
    Session,
    Bearer,
    ApiKey,
    Anonymous,
}

impl AuthSource {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            AuthSource::Session => "session",
            AuthSource::Bearer => "bearer",
            AuthSource::ApiKey => "api_key",
            AuthSource::Anonymous => "anonymous",
        }
    }
}

/// Resolved identity inserted into the depot by [`AuthGuard`].
#[derive(Debug, Clone)]
pub(crate) struct AuthIdentity {
    pub username: String,
    pub role: Role,
    pub source: AuthSource,
    /// Remaining idle seconds for a session; `None` for bearer / anonymous / API key.
    pub expires_in: Option<u64>,
    /// Present when [`AuthSource::ApiKey`].
    pub key_id: Option<String>,
}

impl AuthIdentity {
    pub(crate) fn can_write(&self) -> bool {
        self.role.can_write()
    }

    pub(crate) fn can_admin(&self) -> bool {
        self.role.can_admin()
    }

    pub(crate) fn to_json(&self) -> serde_json::Value {
        let mut v = json!({
            "username": self.username,
            "role": self.role.as_str(),
            "auth": self.source.as_str(),
        });
        if let Some(secs) = self.expires_in {
            v["expires_in"] = json!(secs);
        }
        if let Some(id) = &self.key_id {
            v["key_id"] = json!(id);
        }
        v
    }
}

#[derive(Debug, Clone)]
struct DashboardUser {
    username: String,
    password_hash: String,
    role: Role,
    enabled: bool,
}

#[derive(Debug, Clone)]
struct SessionRecord {
    username: String,
    /// Role captured at login. Authorization uses the live `DashboardUser.role`.
    #[allow(dead_code)]
    role: Role,
    created_at: Instant,
    last_seen: Instant,
}

#[derive(Debug, Clone)]
struct LoginWindow {
    started: Instant,
    count: u32,
}

#[derive(Debug, Clone)]
struct ApiKeyRecord {
    id: String,
    name: String,
    role: Role,
    secret_hash: String,
    created_at: i64,
    created_by: String,
    last_used_at: Option<i64>,
}

/// In-memory user + session + API-key + login-rate store (single-node).
pub(crate) struct AuthState {
    /// Bearer token snapshotted when the HTTP server started. Live config is
    /// also consulted so a hot-reload of `http_bearer_token` is honoured.
    bearer_token: Option<String>,
    users: RwLock<HashMap<String, DashboardUser>>,
    sessions: RwLock<HashMap<String, SessionRecord>>,
    login_attempts: RwLock<HashMap<String, LoginWindow>>,
    api_keys: RwLock<HashMap<String, ApiKeyRecord>>,
}

impl AuthState {
    pub(crate) fn new(bearer_token: Option<String>) -> Self {
        let bearer_token = bearer_token.filter(|s| !s.is_empty());
        Self {
            bearer_token,
            users: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            login_attempts: RwLock::new(HashMap::new()),
            api_keys: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn bearer_token<'a>(&'a self, cfg: &'a PluginConfig) -> Option<&'a str> {
        cfg.http_bearer_token.as_deref().filter(|s| !s.is_empty()).or(self.bearer_token.as_deref())
    }

    pub(crate) async fn has_users(&self) -> bool {
        !self.users.read().await.is_empty()
    }

    async fn has_api_keys(&self) -> bool {
        !self.api_keys.read().await.is_empty()
    }

    /// Auth is required when a bearer token is configured, a bootstrap
    /// password is set, at least one dashboard user exists, or any API key exists.
    pub(crate) async fn auth_required(&self, cfg: &PluginConfig) -> bool {
        self.bearer_token(cfg).is_some()
            || cfg.dashboard_admin_password.as_deref().is_some_and(|s| !s.is_empty())
            || self.has_users().await
            || self.has_api_keys().await
    }

    #[cfg(test)]
    pub(crate) async fn insert_user(&self, username: &str, password: &str, role: Role) -> Result<(), String> {
        let hash = hash_password(password)?;
        self.users.write().await.insert(
            username.to_string(),
            DashboardUser { username: username.to_string(), password_hash: hash, role, enabled: true },
        );
        Ok(())
    }

    async fn get_user(&self, username: &str) -> Option<DashboardUser> {
        self.users.read().await.get(username).cloned()
    }

    async fn upsert_user(&self, username: String, password_hash: String, role: Role, enabled: bool) {
        self.users
            .write()
            .await
            .insert(username.clone(), DashboardUser { username, password_hash, role, enabled });
    }

    async fn list_users(&self) -> Vec<DashboardUser> {
        let mut users: Vec<_> = self.users.read().await.values().cloned().collect();
        users.sort_by_key(|a| a.username.clone());
        users
    }

    async fn create_user(
        &self,
        username: String,
        password: &str,
        role: Role,
    ) -> Result<DashboardUser, String> {
        if self.users.read().await.contains_key(&username) {
            return Err("exists".into());
        }
        let hash = hash_password(password)?;
        let user = DashboardUser { username: username.clone(), password_hash: hash, role, enabled: true };
        self.users.write().await.insert(username, user.clone());
        Ok(user)
    }

    async fn set_user_enabled(&self, username: &str, enabled: bool) -> Result<DashboardUser, String> {
        let mut users = self.users.write().await;
        if !users.contains_key(username) {
            return Err("not_found".into());
        }
        if users.get(username).is_some_and(|u| u.role == Role::Admin) && !enabled {
            let other_admins = users
                .values()
                .filter(|u| u.role == Role::Admin && u.enabled && u.username != username)
                .count();
            if other_admins == 0 {
                return Err("last_admin".into());
            }
        }
        let user = users.get_mut(username).expect("user exists");
        user.enabled = enabled;
        Ok(user.clone())
    }

    async fn revoke_sessions_for(&self, username: &str) {
        self.sessions.write().await.retain(|_, rec| rec.username != username);
    }

    #[cfg(test)]
    async fn enabled_admin_count(&self) -> usize {
        self.users.read().await.values().filter(|u| u.role == Role::Admin && u.enabled).count()
    }

    #[cfg(test)]
    pub(crate) async fn set_user_role(&self, username: &str, role: Role) -> Result<(), String> {
        let mut users = self.users.write().await;
        let user = users.get_mut(username).ok_or_else(|| "not_found".to_string())?;
        user.role = role;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn remove_user(&self, username: &str) {
        self.users.write().await.remove(username);
    }

    async fn create_session(&self, username: String, role: Role) -> String {
        self.purge_expired(Duration::from_secs(30 * 60), Duration::from_secs(12 * 60 * 60)).await;
        let id = new_session_id();
        let now = Instant::now();
        self.sessions
            .write()
            .await
            .insert(id.clone(), SessionRecord { username, role, created_at: now, last_seen: now });
        id
    }

    async fn get_session(&self, id: &str, idle: Duration, absolute: Duration) -> Option<SessionRecord> {
        self.purge_expired(idle, absolute).await;
        let mut sessions = self.sessions.write().await;
        let rec = sessions.get_mut(id)?;
        let now = Instant::now();
        if now.duration_since(rec.created_at) > absolute || now.duration_since(rec.last_seen) > idle {
            sessions.remove(id);
            return None;
        }
        rec.last_seen = now;
        Some(rec.clone())
    }

    async fn remove_session(&self, id: &str) {
        self.sessions.write().await.remove(id);
    }

    async fn purge_expired(&self, idle: Duration, absolute: Duration) {
        let now = Instant::now();
        self.sessions.write().await.retain(|_, rec| {
            now.duration_since(rec.created_at) <= absolute && now.duration_since(rec.last_seen) <= idle
        });
    }

    async fn list_api_keys(&self) -> Vec<ApiKeyRecord> {
        let mut keys: Vec<_> = self.api_keys.read().await.values().cloned().collect();
        keys.sort_by_key(|a| std::cmp::Reverse(a.created_at));
        keys
    }

    async fn get_api_key(&self, id: &str) -> Option<ApiKeyRecord> {
        self.api_keys.read().await.get(id).cloned()
    }

    async fn create_api_key(&self, name: String, role: Role, created_by: String) -> (ApiKeyRecord, String) {
        let id = new_key_id();
        let secret = format!("{API_KEY_PREFIX}{id}_{}", random_hex(16));
        let rec = ApiKeyRecord {
            id: id.clone(),
            name,
            role,
            secret_hash: hash_api_key(&secret),
            created_at: now_millis(),
            created_by,
            last_used_at: None,
        };
        self.api_keys.write().await.insert(id, rec.clone());
        (rec, secret)
    }

    async fn delete_api_key(&self, id: &str) -> bool {
        self.api_keys.write().await.remove(id).is_some()
    }

    async fn lookup_api_key(&self, secret: &str) -> Option<ApiKeyRecord> {
        let hash = hash_api_key(secret);
        let mut keys = self.api_keys.write().await;
        let id = keys.values().find(|k| k.secret_hash == hash).map(|k| k.id.clone())?;
        if let Some(rec) = keys.get_mut(&id) {
            rec.last_used_at = Some(now_millis());
            return Some(rec.clone());
        }
        None
    }

    /// Returns `true` when the caller is still under the rate limit (and
    /// consumes one attempt).
    async fn allow_login_attempt(&self, key: &str, limit: u32, window: Duration) -> bool {
        if limit == 0 {
            return true;
        }
        let now = Instant::now();
        let mut map = self.login_attempts.write().await;
        map.retain(|_, w| now.duration_since(w.started) <= window);
        let entry = map.entry(key.to_string()).or_insert(LoginWindow { started: now, count: 0 });
        if now.duration_since(entry.started) > window {
            entry.started = now;
            entry.count = 0;
        }
        if entry.count >= limit {
            return false;
        }
        entry.count = entry.count.saturating_add(1);
        true
    }
}

fn now_millis() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn random_hex(nbytes: usize) -> String {
    let mut bytes = vec![0u8; nbytes];
    rand::rng().fill_bytes(&mut bytes);
    let mut s = String::with_capacity(nbytes * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn new_session_id() -> String {
    random_hex(32)
}

fn new_key_id() -> String {
    random_hex(8)
}

fn hash_api_key(secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    let out = hasher.finalize();
    let mut s = String::with_capacity(64);
    for b in out {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn api_key_to_json(rec: &ApiKeyRecord, secret: Option<&str>) -> serde_json::Value {
    let mut v = json!({
        "id": rec.id,
        "name": rec.name,
        "role": rec.role.as_str(),
        "created_at": rec.created_at,
        "created_by": rec.created_by,
        "last_used_at": rec.last_used_at,
    });
    if let Some(secret) = secret {
        v["secret"] = json!(secret);
    }
    v
}

fn user_to_json(user: &DashboardUser) -> serde_json::Value {
    json!({
        "username": user.username,
        "role": user.role.as_str(),
        "enabled": user.enabled,
    })
}

/// bcrypt cost 10: production-reasonable and fast enough for crate tests.
const BCRYPT_COST: u32 = 10;

fn hash_password(password: &str) -> Result<String, String> {
    bcrypt::hash(password, BCRYPT_COST).map_err(|e| e.to_string())
}

fn verify_password(password: &str, hash: &str) -> bool {
    bcrypt::verify(password, hash).unwrap_or(false)
}

fn normalize_username(raw: &str) -> Option<String> {
    let name = raw.trim();
    if name.is_empty() || name.len() > MAX_USERNAME_LEN {
        return None;
    }
    if name.chars().any(|c| c.is_control() || c == '/' || c == '\\') {
        return None;
    }
    Some(name.to_string())
}

fn client_ip(req: &Request) -> String {
    let addr = req.remote_addr().to_string();
    if addr.is_empty() || addr == "unknown" {
        "unknown".into()
    } else {
        addr
    }
}

fn cookie_from_request(req: &Request, name: &str) -> Option<String> {
    req.cookie(name).map(|c| c.value().to_string()).filter(|v| !v.is_empty())
}

fn is_unsafe_http_method(method: &salvo::http::Method) -> bool {
    matches!(
        *method,
        salvo::http::Method::POST
            | salvo::http::Method::PUT
            | salvo::http::Method::PATCH
            | salvo::http::Method::DELETE
    )
}

/// Host-port from `Origin` (`https://h:p`) or `Referer` (full URL).
fn host_from_origin_or_referer(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() || raw.eq_ignore_ascii_case("null") {
        return None;
    }
    let rest = if let Some((_, rest)) = raw.split_once("://") { rest } else { raw };
    let hostport = rest.split(['/', '?', '#']).next().unwrap_or("").trim();
    if hostport.is_empty() {
        return None;
    }
    Some(hostport.to_ascii_lowercase())
}

fn request_host(req: &Request) -> Option<String> {
    req.headers().get("host").and_then(|v| v.to_str().ok()).map(|s| s.trim().to_ascii_lowercase())
}

/// Cookie CSRF: missing Origin and Referer is allowed (non-browser / API clients).
/// When either header is present, its host must match the request `Host`.
/// Bearer / API-key clients skip this check in [`AuthGuard`].
pub(crate) fn cookie_csrf_ok(req: &Request) -> bool {
    let origin = req.headers().get("origin").and_then(|v| v.to_str().ok());
    let referer = req.headers().get("referer").and_then(|v| v.to_str().ok());
    if origin.is_none() && referer.is_none() {
        return true;
    }
    let Some(host) = request_host(req) else {
        return true;
    };
    let presented = origin.or(referer).unwrap_or("");
    match host_from_origin_or_referer(presented) {
        Some(oh) => oh == host,
        None => false,
    }
}

fn set_session_cookie(res: &mut Response, cfg: &PluginConfig, session_id: &str) {
    let max_age = time::Duration::seconds(cfg.dashboard_session_max_age.as_secs() as i64);
    let cookie = Cookie::build((cfg.dashboard_cookie_name.clone(), session_id.to_string()))
        .path("/")
        .http_only(true)
        .same_site(SameSite::Lax)
        .secure(cfg.dashboard_cookie_secure)
        .max_age(max_age)
        .build();
    res.add_cookie(cookie);
}

fn clear_session_cookie(res: &mut Response, cfg: &PluginConfig) {
    let cookie = Cookie::build((cfg.dashboard_cookie_name.clone(), String::new()))
        .path("/")
        .http_only(true)
        .same_site(SameSite::Lax)
        .secure(cfg.dashboard_cookie_secure)
        .max_age(time::Duration::ZERO)
        .build();
    res.add_cookie(cookie);
}

fn bearer_from_header(req: &Request) -> Option<String> {
    let raw = req.headers().get("authorization")?.to_str().ok()?;
    let token = raw.strip_prefix("Bearer ").or_else(|| raw.strip_prefix("bearer "))?;
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

fn auth_state(depot: &Depot) -> Option<Arc<AuthState>> {
    depot.get::<Arc<AuthState>>(AUTH_STATE).ok().cloned()
}

pub(crate) fn identity_from_depot(depot: &Depot) -> Option<AuthIdentity> {
    depot.get::<AuthIdentity>(AUTH_IDENTITY).ok().cloned()
}

fn plugin_cfg(depot: &Depot) -> Option<PluginConfig> {
    depot
        .obtain::<(ferromq::context::ServerContext, PluginConfigType)>()
        .ok()
        .map(|(_, cfg)| cfg)
        .and_then(|cfg| cfg.try_read().ok().map(|g| g.clone()))
}

async fn resolve_identity(req: &Request, state: &AuthState, cfg: &PluginConfig) -> Option<AuthIdentity> {
    if let Some(token) = bearer_from_header(req) {
        // Static shared secret first — never shadowed by a hashed API key.
        if state.bearer_token(cfg).is_some_and(|expected| expected == token) {
            return Some(AuthIdentity {
                username: BEARER_USERNAME.into(),
                role: Role::Admin,
                source: AuthSource::Bearer,
                expires_in: None,
                key_id: None,
            });
        }
        if let Some(key) = state.lookup_api_key(&token).await {
            return Some(AuthIdentity {
                username: format!("apikey:{}", key.name),
                role: key.role,
                source: AuthSource::ApiKey,
                expires_in: None,
                key_id: Some(key.id),
            });
        }
    }
    if let Some(sid) = cookie_from_request(req, &cfg.dashboard_cookie_name) {
        if let Some(rec) =
            state.get_session(&sid, cfg.dashboard_session_idle_timeout, cfg.dashboard_session_max_age).await
        {
            let Some(user) = state.get_user(&rec.username).await else {
                state.remove_session(&sid).await;
                return None;
            };
            if !user.enabled {
                state.remove_session(&sid).await;
                return None;
            }
            let remaining = cfg
                .dashboard_session_idle_timeout
                .saturating_sub(Instant::now().duration_since(rec.last_seen))
                .as_secs();
            return Some(AuthIdentity {
                username: rec.username,
                role: user.role,
                source: AuthSource::Session,
                expires_in: Some(remaining),
                key_id: None,
            });
        }
    }
    None
}

/// Authenticate every request on the protected `/api/v1` subtree.
///
/// When neither bearer nor dashboard auth is configured, the request is
/// treated as an anonymous admin so existing open-access deployments keep
/// working.
pub(crate) struct AuthGuard {
    state: Arc<AuthState>,
}

impl AuthGuard {
    pub(crate) fn new(state: Arc<AuthState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl Handler for AuthGuard {
    async fn handle(&self, req: &mut Request, depot: &mut Depot, res: &mut Response, ctrl: &mut FlowCtrl) {
        let cfg = match depot.obtain::<(ferromq::context::ServerContext, PluginConfigType)>() {
            Ok((_, cfg)) => cfg.read().await.clone(),
            Err(_) => {
                render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "auth not configured");
                ctrl.skip_rest();
                return;
            }
        };
        if let Some(id) = resolve_identity(req, &self.state, &cfg).await {
            if id.source == AuthSource::Session
                && is_unsafe_http_method(req.method())
                && !cookie_csrf_ok(req)
            {
                render_api_error(
                    res,
                    StatusCode::FORBIDDEN,
                    "csrf: Origin/Referer does not match Host (cookie session; same-origin browsers only)",
                );
                ctrl.skip_rest();
                return;
            }
            depot.insert(AUTH_IDENTITY, id);
            ctrl.call_next(req, depot, res).await;
            return;
        }
        if self.state.auth_required(&cfg).await {
            render_api_error(res, StatusCode::UNAUTHORIZED, "unauthorized");
            ctrl.skip_rest();
            return;
        }
        depot.insert(
            AUTH_IDENTITY,
            AuthIdentity {
                username: ANONYMOUS_USERNAME.into(),
                role: Role::Admin,
                source: AuthSource::Anonymous,
                expires_in: None,
                key_id: None,
            },
        );
        ctrl.call_next(req, depot, res).await;
    }
}

/// Deny write operations (kick / publish / plugin load) for `viewer`.
/// `admin` and `operator` may proceed.
#[handler]
pub(crate) async fn require_write(depot: &mut Depot, res: &mut Response, ctrl: &mut FlowCtrl) {
    match identity_from_depot(depot) {
        Some(id) if id.can_write() => {}
        Some(id) => {
            render_api_error_with(
                res,
                StatusCode::FORBIDDEN,
                "forbidden",
                Some(json!({
                    "required_role": "operator",
                    "role": id.role.as_str(),
                    "username": id.username,
                })),
            );
            ctrl.skip_rest();
        }
        None => {
            render_api_error(res, StatusCode::UNAUTHORIZED, "unauthorized");
            ctrl.skip_rest();
        }
    }
}

/// Deny user / API-key / audit management for anyone who is not `admin`.
#[handler]
pub(crate) async fn require_admin(depot: &mut Depot, res: &mut Response, ctrl: &mut FlowCtrl) {
    match identity_from_depot(depot) {
        Some(id) if id.can_admin() => {}
        Some(id) => {
            render_api_error_with(
                res,
                StatusCode::FORBIDDEN,
                "forbidden",
                Some(json!({
                    "required_role": "admin",
                    "role": id.role.as_str(),
                    "username": id.username,
                })),
            );
            ctrl.skip_rest();
        }
        None => {
            render_api_error(res, StatusCode::UNAUTHORIZED, "unauthorized");
            ctrl.skip_rest();
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct LoginBody {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct ChangePasswordBody {
    pub old_password: String,
    pub new_password: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateUserBody {
    pub username: String,
    pub password: String,
    pub role: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateApiKeyBody {
    pub name: String,
    #[serde(default)]
    pub role: Option<String>,
}

async fn bootstrap_user_if_needed(
    state: &AuthState,
    cfg: &PluginConfig,
    username: &str,
    password: &str,
) -> Result<Option<Role>, String> {
    if state.has_users().await {
        return Ok(None);
    }
    let admin_user = cfg.dashboard_admin_username.trim();
    let admin_pass = cfg.dashboard_admin_password.as_deref().unwrap_or("").trim();
    let admin_configured = !admin_user.is_empty() && !admin_pass.is_empty();
    if admin_configured {
        if admin_user == username && admin_pass == password {
            let hash = hash_password(password)?;
            state.upsert_user(username.to_string(), hash, Role::Admin, true).await;
            if let (Some(vuser), Some(vpass)) =
                (cfg.dashboard_viewer_username.as_deref(), cfg.dashboard_viewer_password.as_deref())
            {
                let vuser = vuser.trim();
                if !vuser.is_empty() && !vpass.is_empty() && vuser != username {
                    if let Ok(vhash) = hash_password(vpass) {
                        state.upsert_user(vuser.to_string(), vhash, Role::Viewer, true).await;
                    }
                }
            }
            return Ok(Some(Role::Admin));
        }
        // Admin credentials exist: a viewer (or anyone else) must not consume
        // the empty-store bootstrap and lock out admin initialization.
        return Ok(None);
    }
    let viewer_user = cfg.dashboard_viewer_username.as_deref().unwrap_or("").trim();
    if !viewer_user.is_empty()
        && viewer_user == username
        && cfg.dashboard_viewer_password.as_deref().is_some_and(|p| p == password)
    {
        let hash = hash_password(password)?;
        state.upsert_user(username.to_string(), hash, Role::Viewer, true).await;
        return Ok(Some(Role::Viewer));
    }
    Ok(None)
}

/// `POST /api/v1/auth/login`
#[handler]
pub(crate) async fn auth_login(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let Some(state) = auth_state(depot) else {
        render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "auth not configured");
        return;
    };
    let cfg = match depot.obtain::<(ferromq::context::ServerContext, PluginConfigType)>() {
        Ok((_, cfg)) => cfg.read().await.clone(),
        Err(_) => {
            render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "auth not configured");
            return;
        }
    };
    let ip = client_ip(req);
    if !state.allow_login_attempt(&ip, cfg.dashboard_login_rate_limit, cfg.dashboard_login_rate_window).await
    {
        render_api_error(res, StatusCode::TOO_MANY_REQUESTS, "too many login attempts");
        return;
    }
    let body = match req.parse_json::<LoginBody>().await {
        Ok(b) => b,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return;
        }
    };
    let Some(username) = normalize_username(&body.username) else {
        audit::record_raw(
            req,
            depot,
            body.username.trim(),
            "unknown",
            "session",
            "login_failed",
            None,
            false,
            Some(json!({"reason": "invalid_username"})),
        )
        .await;
        render_api_error(res, StatusCode::UNAUTHORIZED, "invalid username or password");
        return;
    };
    if body.password.is_empty() {
        audit::record_raw(
            req,
            depot,
            &username,
            "unknown",
            "session",
            "login_failed",
            None,
            false,
            Some(json!({"reason": "empty_password"})),
        )
        .await;
        render_api_error(res, StatusCode::UNAUTHORIZED, "invalid username or password");
        return;
    }

    let role = if let Some(user) = state.get_user(&username).await {
        if !user.enabled {
            audit::record_raw(
                req,
                depot,
                &username,
                user.role.as_str(),
                "session",
                "login_failed",
                None,
                false,
                Some(json!({"reason": "disabled"})),
            )
            .await;
            render_api_error(res, StatusCode::UNAUTHORIZED, "invalid username or password");
            return;
        }
        if !verify_password(&body.password, &user.password_hash) {
            audit::record_raw(
                req,
                depot,
                &username,
                user.role.as_str(),
                "session",
                "login_failed",
                None,
                false,
                Some(json!({"reason": "bad_password"})),
            )
            .await;
            render_api_error(res, StatusCode::UNAUTHORIZED, "invalid username or password");
            return;
        }
        user.role
    } else {
        match bootstrap_user_if_needed(&state, &cfg, &username, &body.password).await {
            Ok(Some(role)) => role,
            Ok(None) => {
                audit::record_raw(
                    req,
                    depot,
                    &username,
                    "unknown",
                    "session",
                    "login_failed",
                    None,
                    false,
                    Some(json!({"reason": "unknown_user"})),
                )
                .await;
                render_api_error(res, StatusCode::UNAUTHORIZED, "invalid username or password");
                return;
            }
            Err(e) => {
                render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, e);
                return;
            }
        }
    };

    audit::record_raw(req, depot, &username, role.as_str(), "session", "login", None, true, None).await;
    let session_id = state.create_session(username.clone(), role).await;
    set_session_cookie(res, &cfg, &session_id);
    let expires_in = cfg.dashboard_session_idle_timeout.as_secs();
    res.render(Json(json!({
        "username": username,
        "role": role.as_str(),
        "auth": "session",
        "expires_in": expires_in,
    })));
}

/// `POST /api/v1/auth/logout`
#[handler]
pub(crate) async fn auth_logout(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let cfg = match depot.obtain::<(ferromq::context::ServerContext, PluginConfigType)>() {
        Ok((_, cfg)) => cfg.read().await.clone(),
        Err(_) => {
            render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "auth not configured");
            return;
        }
    };
    let mut username = ANONYMOUS_USERNAME.to_string();
    if let Some(state) = auth_state(depot) {
        if let Some(sid) = cookie_from_request(req, &cfg.dashboard_cookie_name) {
            if let Some(rec) = state
                .get_session(&sid, cfg.dashboard_session_idle_timeout, cfg.dashboard_session_max_age)
                .await
            {
                username = rec.username;
            }
            state.remove_session(&sid).await;
        }
    }
    audit::record_raw(req, depot, &username, "unknown", "session", "logout", None, true, None).await;
    clear_session_cookie(res, &cfg);
    res.render(Json(json!({ "ok": true })));
}

/// `POST /api/v1/auth/init` — one-time bootstrap from config credentials.
///
/// Creates the configured admin (and optional viewer) when no dashboard
/// users exist yet. Does **not** accept a caller-supplied password as the
/// new admin secret (that would be an unauthenticated takeover).
#[handler]
pub(crate) async fn auth_init(depot: &mut Depot, res: &mut Response) {
    let Some(state) = auth_state(depot) else {
        render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "auth not configured");
        return;
    };
    let cfg = match depot.obtain::<(ferromq::context::ServerContext, PluginConfigType)>() {
        Ok((_, cfg)) => cfg.read().await.clone(),
        Err(_) => {
            render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "auth not configured");
            return;
        }
    };
    if state.has_users().await {
        render_api_error(res, StatusCode::CONFLICT, "dashboard users already initialized");
        return;
    }
    let username = cfg.dashboard_admin_username.trim();
    let password = cfg.dashboard_admin_password.as_deref().unwrap_or("").trim();
    if username.is_empty() || password.is_empty() {
        render_api_error(
            res,
            StatusCode::BAD_REQUEST,
            "dashboard_admin_username / dashboard_admin_password are not configured",
        );
        return;
    }
    match bootstrap_user_if_needed(&state, &cfg, username, password).await {
        Ok(Some(role)) => {
            res.render(Json(json!({
                "username": username,
                "role": role.as_str(),
                "created": true,
            })));
        }
        Ok(None) => {
            render_api_error(res, StatusCode::CONFLICT, "dashboard users already initialized");
        }
        Err(e) => render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

/// `GET /api/v1/auth/me`
#[handler]
pub(crate) async fn auth_me(depot: &mut Depot, res: &mut Response) {
    match identity_from_depot(depot) {
        Some(id) => res.render(Json(id.to_json())),
        None => render_api_error(res, StatusCode::UNAUTHORIZED, "unauthorized"),
    }
}

/// `POST /api/v1/auth/change-password`
#[handler]
pub(crate) async fn auth_change_password(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let Some(id) = identity_from_depot(depot) else {
        render_api_error(res, StatusCode::UNAUTHORIZED, "unauthorized");
        return;
    };
    if id.source != AuthSource::Session {
        render_api_error(
            res,
            StatusCode::BAD_REQUEST,
            "password change requires a dashboard session (not bearer / anonymous)",
        );
        return;
    }
    let Some(state) = auth_state(depot) else {
        render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "auth not configured");
        return;
    };
    let body = match req.parse_json::<ChangePasswordBody>().await {
        Ok(b) => b,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return;
        }
    };
    if body.new_password.len() < MIN_PASSWORD_LEN {
        render_api_error(
            res,
            StatusCode::BAD_REQUEST,
            format!("new_password must be at least {MIN_PASSWORD_LEN} characters"),
        );
        return;
    }
    let Some(user) = state.get_user(&id.username).await else {
        render_api_error(res, StatusCode::UNAUTHORIZED, "unauthorized");
        return;
    };
    if !verify_password(&body.old_password, &user.password_hash) {
        render_api_error(res, StatusCode::UNAUTHORIZED, "invalid old_password");
        return;
    }
    let cfg = match plugin_cfg(depot) {
        Some(c) => c,
        None => {
            render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "auth not configured");
            return;
        }
    };
    match hash_password(&body.new_password) {
        Ok(hash) => {
            state.upsert_user(user.username.clone(), hash, user.role, user.enabled).await;
            state.revoke_sessions_for(&user.username).await;
            let session_id = state.create_session(user.username.clone(), user.role).await;
            set_session_cookie(res, &cfg, &session_id);
            audit::record(req, depot, "change_password", Some(user.username), true, None).await;
            res.render(Json(json!({ "ok": true, "session_rotated": true })));
        }
        Err(e) => render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

/// `GET /api/v1/users` — admin only.
#[handler]
pub(crate) async fn list_users(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let Some(state) = auth_state(depot) else {
        render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "auth not configured");
        return;
    };
    let max_row_limit = plugin_cfg(depot).map(|c| c.max_row_limit).unwrap_or(10_000);
    let users: Vec<_> = state.list_users().await.iter().map(user_to_json).collect();
    let total = users.len();
    let requested = req.query::<usize>("_limit").unwrap_or(0);
    let paging = ListPaging::from_request(req, requested, max_row_limit);
    let (page, truncated) = paging.apply(users);
    render_list(req, res, page, paging, truncated, Some(total));
}

/// `POST /api/v1/users` — admin only.
#[handler]
pub(crate) async fn create_user(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let Some(state) = auth_state(depot) else {
        render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "auth not configured");
        return;
    };
    let body = match req.parse_json::<CreateUserBody>().await {
        Ok(b) => b,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return;
        }
    };
    let Some(username) = normalize_username(&body.username) else {
        render_api_error(res, StatusCode::BAD_REQUEST, "invalid username");
        return;
    };
    if body.password.len() < MIN_PASSWORD_LEN {
        render_api_error(
            res,
            StatusCode::BAD_REQUEST,
            format!("password must be at least {MIN_PASSWORD_LEN} characters"),
        );
        return;
    }
    let Some(role) = Role::parse(&body.role) else {
        render_api_error(res, StatusCode::BAD_REQUEST, "role must be admin, operator, or viewer");
        return;
    };
    match state.create_user(username.clone(), &body.password, role).await {
        Ok(user) => {
            audit::record(
                req,
                depot,
                "user_create",
                Some(username),
                true,
                Some(json!({"role": role.as_str()})),
            )
            .await;
            res.status_code(StatusCode::CREATED);
            res.render(Json(user_to_json(&user)));
        }
        Err(e) if e == "exists" => {
            render_api_error(res, StatusCode::CONFLICT, "user already exists");
        }
        Err(e) => render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

/// `POST /api/v1/users/{username}/disable` — admin only.
#[handler]
pub(crate) async fn disable_user(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    set_user_enabled_handler(req, depot, res, false).await;
}

/// `POST /api/v1/users/{username}/enable` — admin only.
#[handler]
pub(crate) async fn enable_user(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    set_user_enabled_handler(req, depot, res, true).await;
}

async fn set_user_enabled_handler(req: &mut Request, depot: &mut Depot, res: &mut Response, enabled: bool) {
    let Some(state) = auth_state(depot) else {
        render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "auth not configured");
        return;
    };
    let Some(username) = req.param::<String>("username") else {
        render_api_error(res, StatusCode::BAD_REQUEST, "username required");
        return;
    };
    if let Some(id) = identity_from_depot(depot) {
        if !enabled && id.username == username {
            render_api_error(res, StatusCode::BAD_REQUEST, "cannot disable the current user");
            return;
        }
    }
    match state.set_user_enabled(&username, enabled).await {
        Ok(user) => {
            if !enabled {
                state.revoke_sessions_for(&username).await;
            }
            let action = if enabled { "user_enable" } else { "user_disable" };
            audit::record(req, depot, action, Some(username), true, Some(json!({"enabled": enabled}))).await;
            res.render(Json(user_to_json(&user)));
        }
        Err(e) if e == "not_found" => render_api_error(res, StatusCode::NOT_FOUND, "user not found"),
        Err(e) if e == "last_admin" => {
            render_api_error(res, StatusCode::CONFLICT, "cannot disable the last enabled admin");
        }
        Err(e) => render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

/// `GET /api/v1/api-keys` — admin only.
#[handler]
pub(crate) async fn list_api_keys(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let Some(state) = auth_state(depot) else {
        render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "auth not configured");
        return;
    };
    let max_row_limit = plugin_cfg(depot).map(|c| c.max_row_limit).unwrap_or(10_000);
    let keys: Vec<_> = state.list_api_keys().await.iter().map(|k| api_key_to_json(k, None)).collect();
    let total = keys.len();
    let requested = req.query::<usize>("_limit").unwrap_or(0);
    let paging = ListPaging::from_request(req, requested, max_row_limit);
    let (page, truncated) = paging.apply(keys);
    render_list(req, res, page, paging, truncated, Some(total));
}

/// `POST /api/v1/api-keys` — admin only. Secret is returned once.
#[handler]
pub(crate) async fn create_api_key(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let Some(state) = auth_state(depot) else {
        render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "auth not configured");
        return;
    };
    let body = match req.parse_json::<CreateApiKeyBody>().await {
        Ok(b) => b,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return;
        }
    };
    let name = body.name.trim().to_string();
    if name.is_empty() || name.len() > MAX_USERNAME_LEN {
        render_api_error(res, StatusCode::BAD_REQUEST, "name is required (1–64 characters)");
        return;
    }
    let role = match body.role.as_deref() {
        None | Some("") => Role::Operator,
        Some(raw) => match Role::parse(raw) {
            Some(r) => r,
            None => {
                render_api_error(res, StatusCode::BAD_REQUEST, "role must be admin, operator, or viewer");
                return;
            }
        },
    };
    let created_by =
        identity_from_depot(depot).map(|id| id.username).unwrap_or_else(|| ANONYMOUS_USERNAME.into());
    let (rec, secret) = state.create_api_key(name.clone(), role, created_by).await;
    audit::record(
        req,
        depot,
        "api_key_create",
        Some(rec.id.clone()),
        true,
        Some(json!({"name": name, "role": role.as_str()})),
    )
    .await;
    res.status_code(StatusCode::CREATED);
    res.render(Json(api_key_to_json(&rec, Some(&secret))));
}

/// `GET /api/v1/api-keys/{id}` — admin only. Never returns the secret.
#[handler]
pub(crate) async fn get_api_key(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let Some(state) = auth_state(depot) else {
        render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "auth not configured");
        return;
    };
    let Some(id) = req.param::<String>("id") else {
        render_api_error(res, StatusCode::BAD_REQUEST, "id required");
        return;
    };
    match state.get_api_key(&id).await {
        Some(rec) => res.render(Json(api_key_to_json(&rec, None))),
        None => render_api_error(res, StatusCode::NOT_FOUND, "api key not found"),
    }
}

/// `DELETE /api/v1/api-keys/{id}` — admin only.
#[handler]
pub(crate) async fn delete_api_key(req: &mut Request, depot: &mut Depot, res: &mut Response) {
    let Some(state) = auth_state(depot) else {
        render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, "auth not configured");
        return;
    };
    let Some(id) = req.param::<String>("id") else {
        render_api_error(res, StatusCode::BAD_REQUEST, "id required");
        return;
    };
    if state.delete_api_key(&id).await {
        audit::record(req, depot, "api_key_delete", Some(id), true, None).await;
        res.render(Json(json!({ "ok": true })));
    } else {
        render_api_error(res, StatusCode::NOT_FOUND, "api key not found");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_hash_is_not_plaintext_and_verifies() {
        let hash = hash_password("s3cret-pass").expect("hash");
        assert!(!hash.contains("s3cret-pass"), "hash must not embed plaintext");
        assert!(hash.starts_with("$2"), "expected bcrypt encoded hash, got {hash}");
        assert!(verify_password("s3cret-pass", &hash));
        assert!(!verify_password("wrong", &hash));
    }

    #[test]
    fn api_key_hash_is_sha256_hex_and_not_secret() {
        let secret = "fmqk_deadbeef_secret";
        let hash = hash_api_key(secret);
        assert_eq!(hash.len(), 64);
        assert!(!hash.contains("secret"));
        assert_eq!(hash, hash_api_key(secret));
        assert_ne!(hash, hash_api_key("other"));
    }

    #[test]
    fn roles_write_and_admin_gates() {
        assert!(!Role::Viewer.can_write());
        assert!(Role::Operator.can_write());
        assert!(Role::Admin.can_write());
        assert!(!Role::Viewer.can_admin());
        assert!(!Role::Operator.can_admin());
        assert!(Role::Admin.can_admin());
        let viewer = AuthIdentity {
            username: "v".into(),
            role: Role::Viewer,
            source: AuthSource::Session,
            expires_in: None,
            key_id: None,
        };
        assert!(!viewer.can_write());
        assert!(!viewer.can_admin());
    }

    #[test]
    fn normalize_username_rejects_empty_and_controls() {
        assert!(normalize_username("  ").is_none());
        assert!(normalize_username("a\nb").is_none());
        assert_eq!(normalize_username("  admin  ").as_deref(), Some("admin"));
    }

    #[tokio::test]
    async fn session_expires_on_idle() {
        let state = AuthState::new(None);
        let sid = state.create_session("admin".into(), Role::Admin).await;
        assert!(state.get_session(&sid, Duration::from_secs(60), Duration::from_secs(3600)).await.is_some());
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(state.get_session(&sid, Duration::from_millis(1), Duration::from_secs(3600)).await.is_none());
    }

    #[tokio::test]
    async fn login_rate_limit_trips() {
        let state = AuthState::new(None);
        let window = Duration::from_secs(60);
        for _ in 0..3 {
            assert!(state.allow_login_attempt("1.2.3.4", 3, window).await);
        }
        assert!(!state.allow_login_attempt("1.2.3.4", 3, window).await);
        assert!(state.allow_login_attempt("5.6.7.8", 3, window).await);
    }

    #[tokio::test]
    async fn cannot_disable_last_admin() {
        let state = AuthState::new(None);
        state.insert_user("admin", "admin-secret", Role::Admin).await.unwrap();
        assert_eq!(state.enabled_admin_count().await, 1);
        assert_eq!(state.set_user_enabled("admin", false).await.unwrap_err(), "last_admin");
    }

    #[tokio::test]
    async fn api_key_lookup_matches_created_secret() {
        let state = AuthState::new(None);
        let (rec, secret) = state.create_api_key("ci".into(), Role::Operator, "admin".into()).await;
        assert!(secret.starts_with(API_KEY_PREFIX));
        assert!(!secret.is_empty());
        let found = state.lookup_api_key(&secret).await.expect("lookup");
        assert_eq!(found.id, rec.id);
        assert_eq!(found.role, Role::Operator);
        assert!(state.lookup_api_key("fmqk_nope").await.is_none());
    }

    #[test]
    fn origin_host_parsing_and_same_host() {
        assert_eq!(host_from_origin_or_referer("https://127.0.0.1:6060").as_deref(), Some("127.0.0.1:6060"));
        assert_eq!(
            host_from_origin_or_referer("http://example.com/dashboard/#/acl").as_deref(),
            Some("example.com")
        );
        assert!(host_from_origin_or_referer("null").is_none());
        assert!(host_from_origin_or_referer("").is_none());
    }

    #[tokio::test]
    async fn viewer_bootstrap_does_not_lock_out_configured_admin() {
        let state = AuthState::new(None);
        let cfg = PluginConfig {
            dashboard_admin_username: "admin".into(),
            dashboard_admin_password: Some("admin-secret".into()),
            dashboard_viewer_username: Some("viewer".into()),
            dashboard_viewer_password: Some("viewer-secret".into()),
            ..serde_json::from_value(serde_json::json!({"http_bearer_token": null})).expect("defaults")
        };
        assert!(bootstrap_user_if_needed(&state, &cfg, "viewer", "viewer-secret").await.unwrap().is_none());
        assert!(!state.has_users().await);
        assert_eq!(
            bootstrap_user_if_needed(&state, &cfg, "admin", "admin-secret").await.unwrap(),
            Some(Role::Admin)
        );
        assert!(state.get_user("admin").await.is_some());
        assert!(state.get_user("viewer").await.is_some());
    }
}
