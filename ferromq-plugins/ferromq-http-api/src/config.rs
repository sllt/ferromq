//! Configuration for the HTTP API plugin.
//!
//! Defines [`PluginConfig`] with HTTP server settings, message expiry,
//! metrics sampling, Prometheus cache intervals, and history flush.

use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use ferromq::{
    grpc::MessageType,
    utils::{deserialize_addr, deserialize_duration},
    Result,
};

/// Top-level configuration for the HTTP API plugin.
///
/// Specifies the HTTP listen address, bearer token, message type for gRPC,
/// metrics/Prometheus settings, request logging options, and optional
/// history flush configuration.
#[derive(Clone, Deserialize, Serialize)]
pub struct PluginConfig {
    #[serde(default = "PluginConfig::max_row_limit_default")]
    pub max_row_limit: usize,

    #[serde(default = "PluginConfig::http_laddr_default", deserialize_with = "deserialize_addr")]
    pub http_laddr: SocketAddr,

    #[serde(
        default = "PluginConfig::metrics_sample_interval_default",
        deserialize_with = "deserialize_duration"
    )]
    pub metrics_sample_interval: Duration,

    #[serde(default)]
    pub http_bearer_token: Option<String>,

    /// Bootstrap admin username used when no dashboard users exist yet.
    #[serde(default = "PluginConfig::dashboard_admin_username_default")]
    pub dashboard_admin_username: String,

    /// Bootstrap admin password (plaintext in config only). Hashed on first
    /// login / `POST /api/v1/auth/init`. Never written back to disk.
    #[serde(default)]
    pub dashboard_admin_password: Option<String>,

    /// Optional bootstrap viewer username (created together with admin on init).
    #[serde(default)]
    pub dashboard_viewer_username: Option<String>,

    /// Optional bootstrap viewer password (plaintext in config only).
    #[serde(default)]
    pub dashboard_viewer_password: Option<String>,

    /// Session cookie name. Default: `ferromq_session`.
    #[serde(default = "PluginConfig::dashboard_cookie_name_default")]
    pub dashboard_cookie_name: String,

    /// Set the `Secure` flag on the session cookie (enable when serving HTTPS).
    #[serde(default)]
    pub dashboard_cookie_secure: bool,

    /// Idle session timeout. Default: 30 minutes.
    #[serde(
        default = "PluginConfig::dashboard_session_idle_timeout_default",
        deserialize_with = "deserialize_duration"
    )]
    pub dashboard_session_idle_timeout: Duration,

    /// Absolute session lifetime from login. Default: 12 hours.
    #[serde(
        default = "PluginConfig::dashboard_session_max_age_default",
        deserialize_with = "deserialize_duration"
    )]
    pub dashboard_session_max_age: Duration,

    /// Max login attempts per client IP per window. Default: 10.
    #[serde(default = "PluginConfig::dashboard_login_rate_limit_default")]
    pub dashboard_login_rate_limit: u32,

    /// Login rate-limit window. Default: 1 minute.
    #[serde(
        default = "PluginConfig::dashboard_login_rate_window_default",
        deserialize_with = "deserialize_duration"
    )]
    pub dashboard_login_rate_window: Duration,

    /// In-memory audit ring-buffer size. Default: 10000.
    #[serde(default = "PluginConfig::audit_max_events_default")]
    pub audit_max_events: usize,

    /// Optional JSON Lines file for durable audit events (append-only).
    /// When unset, the audit log lives only in process memory.
    #[serde(default)]
    pub audit_file: Option<String>,

    #[serde(default = "PluginConfig::message_type_default")]
    pub message_type: MessageType,

    #[serde(default = "PluginConfig::http_reuseaddr_default")]
    pub http_reuseaddr: bool,

    #[serde(default = "PluginConfig::http_reuseport_default")]
    pub http_reuseport: bool,

    #[serde(default = "PluginConfig::http_request_log_default")]
    pub http_request_log: bool,

    #[serde(
        default = "PluginConfig::message_expiry_interval_default",
        deserialize_with = "deserialize_duration"
    )]
    pub message_expiry_interval: Duration,

    #[serde(
        default = "PluginConfig::prometheus_metrics_cache_interval_default",
        deserialize_with = "deserialize_duration"
    )]
    pub prometheus_metrics_cache_interval: Duration,

    /// Optional static directory for the Dashboard SPA.
    /// If set and the path exists, http-api serves that directory at
    /// `/dashboard/` (and `/`) instead of the rust-embed `dashboard-dist/`
    /// assets. Missing path falls back to the embed.
    #[serde(default)]
    pub dashboard_static_dir: Option<String>,

    // ── History flush configuration ──────────────────────────────────────
    /// Stats/Metrics history storage config.
    /// When `None`, the history flush and query APIs are disabled.
    #[serde(default)]
    pub storage: Option<ferromq_storage::Config>,

    /// Interval between history flush writes.
    /// Default: 5 seconds.
    #[serde(default = "PluginConfig::flush_interval_default", deserialize_with = "deserialize_duration")]
    pub flush_interval: Duration,

    /// TTL for each history data point.
    /// After this duration, the storage backend automatically evicts it.
    /// Default: 7 days.
    #[serde(default = "PluginConfig::history_retention_default", deserialize_with = "deserialize_duration")]
    pub history_retention: Duration,

    /// How many previous plugin / broker config files to keep on overwrite.
    /// Default: 10.
    #[serde(default = "PluginConfig::config_history_keep_default")]
    pub config_history_keep: usize,

    /// Optional path to this node's `ferromq.toml`. Used by the read-only
    /// broker/listener/log overview and the honest write path (file only;
    /// `ferromqd` is never claimed to hot-restart).
    #[serde(default)]
    pub broker_config_file: Option<String>,
}

impl PluginConfig {
    #[inline]
    fn max_row_limit_default() -> usize {
        10_000
    }

    #[inline]
    fn http_laddr_default() -> SocketAddr {
        ([0, 0, 0, 0], 6060).into()
    }

    #[inline]
    fn metrics_sample_interval_default() -> Duration {
        Duration::from_secs(5)
    }

    #[inline]
    fn message_type_default() -> MessageType {
        99
    }

    #[inline]
    fn http_reuseaddr_default() -> bool {
        true
    }

    #[inline]
    fn http_reuseport_default() -> bool {
        false
    }

    #[inline]
    fn http_request_log_default() -> bool {
        false
    }

    #[inline]
    fn message_expiry_interval_default() -> Duration {
        Duration::from_secs(300)
    }

    #[inline]
    fn prometheus_metrics_cache_interval_default() -> Duration {
        Duration::from_secs(5)
    }

    #[inline]
    fn flush_interval_default() -> Duration {
        Duration::from_secs(5)
    }

    #[inline]
    fn history_retention_default() -> Duration {
        Duration::from_secs(7 * 24 * 60 * 60)
    }

    #[inline]
    fn dashboard_admin_username_default() -> String {
        "admin".into()
    }

    #[inline]
    fn dashboard_cookie_name_default() -> String {
        "ferromq_session".into()
    }

    #[inline]
    fn dashboard_session_idle_timeout_default() -> Duration {
        Duration::from_secs(30 * 60)
    }

    #[inline]
    fn dashboard_session_max_age_default() -> Duration {
        Duration::from_secs(12 * 60 * 60)
    }

    #[inline]
    fn dashboard_login_rate_limit_default() -> u32 {
        10
    }

    #[inline]
    fn dashboard_login_rate_window_default() -> Duration {
        Duration::from_secs(60)
    }

    #[inline]
    fn audit_max_events_default() -> usize {
        10_000
    }

    #[inline]
    fn config_history_keep_default() -> usize {
        10
    }

    /// Serializes the configuration to a JSON value, redacting secrets.
    #[inline]
    pub fn to_json(&self) -> Result<serde_json::Value> {
        let mut v = serde_json::to_value(self)?;
        if let Some(obj) = v.as_object_mut() {
            for key in ["http_bearer_token", "dashboard_admin_password", "dashboard_viewer_password"] {
                if obj.get(key).and_then(|x| x.as_str()).is_some_and(|s| !s.is_empty()) {
                    obj.insert(key.to_string(), serde_json::Value::String("***".into()));
                }
            }
        }
        Ok(v)
    }

    /// Returns `true` if any config values that require a hot-reload
    /// (without restart) have changed. Auth-related fields are included so a
    /// `load_config` of `http_bearer_token` / dashboard credentials updates
    /// the in-memory snapshot instead of silently no-oping.
    #[inline]
    pub fn changed(&self, other: &Self) -> bool {
        self.max_row_limit != other.max_row_limit
            || self.http_laddr != other.http_laddr
            || self.metrics_sample_interval != other.metrics_sample_interval
            || self.http_request_log != other.http_request_log
            || self.prometheus_metrics_cache_interval != other.prometheus_metrics_cache_interval
            || self.dashboard_static_dir != other.dashboard_static_dir
            || self.config_history_keep != other.config_history_keep
            || self.broker_config_file != other.broker_config_file
            || self.http_bearer_token != other.http_bearer_token
            || self.dashboard_admin_username != other.dashboard_admin_username
            || self.dashboard_admin_password != other.dashboard_admin_password
            || self.dashboard_viewer_username != other.dashboard_viewer_username
            || self.dashboard_viewer_password != other.dashboard_viewer_password
            || self.dashboard_cookie_name != other.dashboard_cookie_name
            || self.dashboard_cookie_secure != other.dashboard_cookie_secure
            || self.dashboard_session_idle_timeout != other.dashboard_session_idle_timeout
            || self.dashboard_session_max_age != other.dashboard_session_max_age
            || self.dashboard_login_rate_limit != other.dashboard_login_rate_limit
            || self.dashboard_login_rate_window != other.dashboard_login_rate_window
            || self.audit_max_events != other.audit_max_events
            || self.audit_file != other.audit_file
            || self.http_reuseaddr != other.http_reuseaddr
            || self.http_reuseport != other.http_reuseport
            || self.message_expiry_interval != other.message_expiry_interval
            || self.message_type != other.message_type
    }

    /// Returns `true` if a full HTTP server restart is required (listen
    /// address / socket flags / static dashboard dir changed).
    #[inline]
    pub fn restart_enable(&self, other: &Self) -> bool {
        self.http_laddr != other.http_laddr
            || self.dashboard_static_dir != other.dashboard_static_dir
            || self.http_reuseaddr != other.http_reuseaddr
            || self.http_reuseport != other.http_reuseport
    }

    /// One-line summary safe for logs: secrets are never included.
    pub fn log_summary(&self) -> String {
        format!(
            "http_laddr={} auth_bearer={} dashboard_admin_password={} users_bootstrap={} max_row_limit={}",
            self.http_laddr,
            if self.http_bearer_token.as_deref().is_some_and(|s| !s.is_empty()) { "set" } else { "unset" },
            if self.dashboard_admin_password.as_deref().is_some_and(|s| !s.is_empty()) {
                "set"
            } else {
                "unset"
            },
            self.dashboard_admin_username,
            self.max_row_limit
        )
    }
}

impl fmt::Debug for PluginConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PluginConfig")
            .field("max_row_limit", &self.max_row_limit)
            .field("http_laddr", &self.http_laddr)
            .field("http_bearer_token", &redacted_opt(&self.http_bearer_token))
            .field("dashboard_admin_username", &self.dashboard_admin_username)
            .field("dashboard_admin_password", &redacted_opt(&self.dashboard_admin_password))
            .field("dashboard_viewer_username", &self.dashboard_viewer_username)
            .field("dashboard_viewer_password", &redacted_opt(&self.dashboard_viewer_password))
            .field("dashboard_cookie_name", &self.dashboard_cookie_name)
            .field("dashboard_cookie_secure", &self.dashboard_cookie_secure)
            .field("http_request_log", &self.http_request_log)
            .field("dashboard_static_dir", &self.dashboard_static_dir)
            .finish_non_exhaustive()
    }
}

fn redacted_opt(value: &Option<String>) -> Option<&'static str> {
    value.as_deref().filter(|s| !s.is_empty()).map(|_| "***")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_and_log_summary_never_include_secrets() {
        let cfg = PluginConfig {
            http_bearer_token: Some("super-bearer-secret".into()),
            dashboard_admin_password: Some("admin-plain-password".into()),
            dashboard_viewer_password: Some("viewer-plain-password".into()),
            ..serde_json::from_value(serde_json::json!({"http_bearer_token": null})).expect("defaults")
        };
        let dbg = format!("{cfg:?}");
        let summary = cfg.log_summary();
        for leak in ["super-bearer-secret", "admin-plain-password", "viewer-plain-password"] {
            assert!(!dbg.contains(leak), "Debug leaked {leak}: {dbg}");
            assert!(!summary.contains(leak), "log_summary leaked {leak}: {summary}");
        }
        assert!(dbg.contains("***"));
        assert!(summary.contains("auth_bearer=set"));
        assert!(summary.contains("dashboard_admin_password=set"));
    }

    #[test]
    fn changed_detects_bearer_token() {
        let a =
            serde_json::from_value::<PluginConfig>(serde_json::json!({"http_bearer_token": "a"})).expect("a");
        let b =
            serde_json::from_value::<PluginConfig>(serde_json::json!({"http_bearer_token": "b"})).expect("b");
        assert!(a.changed(&b));
        assert!(!a.restart_enable(&b));
    }
}
