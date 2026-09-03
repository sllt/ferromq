//! Dashboard session authentication for the HTTP API plugin.
//!
//! P3a: username/password login with an HttpOnly session cookie, bcrypt
//! password hashes, in-memory user + session stores, idle/absolute expiry,
//! and a simple per-IP login rate limit.
//!
//! Compatibility: `Authorization: Bearer <http_bearer_token>` remains a
//! superuser/operator credential for automation. MQTT client auth plugins
//! are not involved — this module is scoped to http-api only.
//!
//! Cluster limitation: users and sessions live in process memory on this
//! node. Restarting the HTTP server drops them (bootstrap from config
//! again). A load-balanced cluster needs sticky sessions. P3b will add
//! API keys and an audit log; hooks are marked below.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::RngCore;
use salvo::http::cookie::{Cookie, SameSite};
use salvo::http::StatusCode;
use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::RwLock;

use super::config::PluginConfig;
use super::response::{render_api_error, render_api_error_with};
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

/// Dashboard role stored on the user record.
///
/// `admin` may kick / publish / load plugins. `viewer` is read-only.
/// P3b: extra roles / API-key scopes can be added without changing this enum
/// shape if serialized as snake_case strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Role {
    Admin,
    Viewer,
}

impl Role {
    pub(crate) fn can_write(self) -> bool {
        matches!(self, Role::Admin)
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Role::Admin => "admin",
            Role::Viewer => "viewer",
        }
    }
}

/// How the request was authenticated.
///
/// P3b: add `ApiKey { key_id: String }` here when API keys land.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AuthSource {
    Session,
    Bearer,
    Anonymous,
}

impl AuthSource {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            AuthSource::Session => "session",
            AuthSource::Bearer => "bearer",
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
    /// Remaining idle seconds for a session; `None` for bearer / anonymous.
    pub expires_in: Option<u64>,
}

impl AuthIdentity {
    pub(crate) fn can_write(&self) -> bool {
        self.role.can_write()
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
        v
    }
}

#[derive(Debug, Clone)]
struct DashboardUser {
    username: String,
    password_hash: String,
    role: Role,
}

#[derive(Debug, Clone)]
struct SessionRecord {
    username: String,
    role: Role,
    created_at: Instant,
    last_seen: Instant,
}

#[derive(Debug, Clone)]
struct LoginWindow {
    started: Instant,
    count: u32,
}

/// In-memory user + session + login-rate store (single-node).
pub(crate) struct AuthState {
    /// Bearer token snapshotted when the HTTP server started. Live config is
    /// also consulted so a hot-reload of `http_bearer_token` is honoured.
    bearer_token: Option<String>,
    users: RwLock<HashMap<String, DashboardUser>>,
    sessions: RwLock<HashMap<String, SessionRecord>>,
    login_attempts: RwLock<HashMap<String, LoginWindow>>,
}

impl AuthState {
    pub(crate) fn new(bearer_token: Option<String>) -> Self {
        let bearer_token = bearer_token.filter(|s| !s.is_empty());
        Self {
            bearer_token,
            users: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            login_attempts: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn bearer_token<'a>(&'a self, cfg: &'a PluginConfig) -> Option<&'a str> {
        cfg.http_bearer_token.as_deref().filter(|s| !s.is_empty()).or(self.bearer_token.as_deref())
    }

    pub(crate) async fn has_users(&self) -> bool {
        !self.users.read().await.is_empty()
    }

    /// Auth is required when a bearer token is configured, a bootstrap
    /// password is set, or at least one dashboard user already exists.
    pub(crate) async fn auth_required(&self, cfg: &PluginConfig) -> bool {
        self.bearer_token(cfg).is_some()
            || cfg.dashboard_admin_password.as_deref().is_some_and(|s| !s.is_empty())
            || self.has_users().await
    }

    #[cfg(test)]
    pub(crate) async fn insert_user(&self, username: &str, password: &str, role: Role) -> Result<(), String> {
        let hash = hash_password(password)?;
        self.users.write().await.insert(
            username.to_string(),
            DashboardUser { username: username.to_string(), password_hash: hash, role },
        );
        Ok(())
    }

    async fn get_user(&self, username: &str) -> Option<DashboardUser> {
        self.users.read().await.get(username).cloned()
    }

    async fn upsert_user(&self, username: String, password_hash: String, role: Role) {
        self.users.write().await.insert(username.clone(), DashboardUser { username, password_hash, role });
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

fn new_session_id() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    let mut s = String::with_capacity(64);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
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

fn identity_from_depot(depot: &Depot) -> Option<AuthIdentity> {
    depot.get::<AuthIdentity>(AUTH_IDENTITY).ok().cloned()
}

async fn resolve_identity(req: &Request, state: &AuthState, cfg: &PluginConfig) -> Option<AuthIdentity> {
    if let Some(token) = bearer_from_header(req) {
        if state.bearer_token(cfg).is_some_and(|expected| expected == token) {
            return Some(AuthIdentity {
                username: BEARER_USERNAME.into(),
                role: Role::Admin,
                source: AuthSource::Bearer,
                expires_in: None,
            });
        }
    }
    if let Some(sid) = cookie_from_request(req, &cfg.dashboard_cookie_name) {
        if let Some(rec) =
            state.get_session(&sid, cfg.dashboard_session_idle_timeout, cfg.dashboard_session_max_age).await
        {
            let remaining = cfg
                .dashboard_session_idle_timeout
                .saturating_sub(Instant::now().duration_since(rec.last_seen))
                .as_secs();
            return Some(AuthIdentity {
                username: rec.username,
                role: rec.role,
                source: AuthSource::Session,
                expires_in: Some(remaining),
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
            },
        );
        ctrl.call_next(req, depot, res).await;
    }
}

/// Deny write operations (kick / publish / plugin load) for `viewer`.
///
/// P3b: replace the coarse `can_write` check with per-action scopes and
/// emit an audit event (`identity`, `action`, `request_id`) here.
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
    if !admin_user.is_empty()
        && admin_user == username
        && cfg.dashboard_admin_password.as_deref().is_some_and(|p| p == password)
    {
        let hash = hash_password(password)?;
        state.upsert_user(username.to_string(), hash, Role::Admin).await;
        if let (Some(vuser), Some(vpass)) =
            (cfg.dashboard_viewer_username.as_deref(), cfg.dashboard_viewer_password.as_deref())
        {
            let vuser = vuser.trim();
            if !vuser.is_empty() && !vpass.is_empty() && vuser != username {
                if let Ok(vhash) = hash_password(vpass) {
                    state.upsert_user(vuser.to_string(), vhash, Role::Viewer).await;
                }
            }
        }
        return Ok(Some(Role::Admin));
    }
    let viewer_user = cfg.dashboard_viewer_username.as_deref().unwrap_or("").trim();
    if !viewer_user.is_empty()
        && viewer_user == username
        && cfg.dashboard_viewer_password.as_deref().is_some_and(|p| p == password)
    {
        let hash = hash_password(password)?;
        state.upsert_user(username.to_string(), hash, Role::Viewer).await;
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
        render_api_error(res, StatusCode::UNAUTHORIZED, "invalid username or password");
        return;
    };
    if body.password.is_empty() {
        render_api_error(res, StatusCode::UNAUTHORIZED, "invalid username or password");
        return;
    }

    let role = if let Some(user) = state.get_user(&username).await {
        if !verify_password(&body.password, &user.password_hash) {
            render_api_error(res, StatusCode::UNAUTHORIZED, "invalid username or password");
            return;
        }
        user.role
    } else {
        match bootstrap_user_if_needed(&state, &cfg, &username, &body.password).await {
            Ok(Some(role)) => role,
            Ok(None) => {
                render_api_error(res, StatusCode::UNAUTHORIZED, "invalid username or password");
                return;
            }
            Err(e) => {
                render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, e);
                return;
            }
        }
    };

    // P3b: audit::record(login, username, ip, request_id)
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
    if let Some(state) = auth_state(depot) {
        if let Some(sid) = cookie_from_request(req, &cfg.dashboard_cookie_name) {
            state.remove_session(&sid).await;
        }
    }
    // P3b: audit::record(logout, ...)
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
    match hash_password(&body.new_password) {
        Ok(hash) => {
            state.upsert_user(user.username, hash, user.role).await;
            // P3b: audit::record(change_password, username, request_id)
            res.render(Json(json!({ "ok": true })));
        }
        Err(e) => render_api_error(res, StatusCode::INTERNAL_SERVER_ERROR, e),
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
    fn viewer_cannot_write_admin_can() {
        assert!(!Role::Viewer.can_write());
        assert!(Role::Admin.can_write());
        let viewer = AuthIdentity {
            username: "v".into(),
            role: Role::Viewer,
            source: AuthSource::Session,
            expires_in: None,
        };
        assert!(!viewer.can_write());
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
        // Force last_seen into the past via a tiny idle window after a sleep.
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
}
