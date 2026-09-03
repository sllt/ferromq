//! P4 online configuration management.
//!
//! Plugin config write / validate / versioning, secret redaction on GET, and
//! a read-first broker (`ferromq.toml`) overview. Writable MQTT / listener /
//! log sections update the file only and always report `restart_required`.
//! This module never claims a hot restart of `ferromqd`.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use salvo::http::StatusCode;
use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use ferromq::context::ServerContext;
use ferromq::grpc::{Message as GrpcMessage, MessageReply as GrpcMessageReply, MessageSender, MessageType};
use ferromq::types::NodeId;
use ferromq::Result;

use super::audit;
use super::auth::identity_from_depot;
use super::response::{render_api_error, render_api_error_with, render_not_found, status_for_plugin_error};
use super::types::{Message, MessageReply};
use super::PluginConfigType;

const REDACTED: &str = "***";
const DEFAULT_HISTORY_KEEP: usize = 10;
const BROKER_WRITABLE: &[&str] = &["mqtt", "listener", "log"];

/// How a written (or validated) config becomes effective.
///
/// * `hot` — already applied in this process via the plugin `load_config` hook.
///   This is **not** a `ferromqd` process restart.
/// * `reload` — file is written; call `PUT .../config/reload` (or re-PUT with
///   `apply=reload`) so the plugin picks it up.
/// * `restart_required` — the running process will not use the new values
///   until `ferromqd` (or an immutable plugin on next start) is restarted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EffectiveMode {
    Hot,
    Reload,
    RestartRequired,
}

impl EffectiveMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Hot => "hot",
            Self::Reload => "reload",
            Self::RestartRequired => "restart_required",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ConfigDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<String>,
}

impl ConfigDiff {
    fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.changed.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WriteResult {
    pub ok: bool,
    pub written: bool,
    pub applied: bool,
    pub effective: EffectiveMode,
    pub diff: ConfigDiff,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backup: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub apply_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plugin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<NodeId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub section: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ValidateResult {
    pub ok: bool,
    pub valid: bool,
    pub effective: EffectiveMode,
    pub diff: ConfigDiff,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plugin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<NodeId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub section: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct VersionInfo {
    pub version: String,
    pub ts: i64,
    pub size: u64,
}

/// Query `?reveal=1` / `true` / `yes`.
pub(crate) fn wants_reveal(req: &Request) -> bool {
    req.query::<String>("reveal")
        .as_deref()
        .is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}

/// Query `?apply=none|reload`. Default `reload`.
fn parse_apply(req: &Request) -> Result<bool> {
    match req.query::<String>("apply").as_deref().map(str::trim).unwrap_or("reload") {
        "" | "reload" | "1" | "true" | "yes" => Ok(true),
        "none" | "0" | "false" | "no" => Ok(false),
        other => Err(anyhow!("invalid apply={other}; expected none or reload")),
    }
}

/// Keys that must never be echoed unless `?reveal=1` and the caller is admin.
pub(crate) fn is_secret_key(key: &str) -> bool {
    let k = key.trim().to_ascii_lowercase().replace('-', "_");
    if k.is_empty() {
        return false;
    }
    if k == "jwt" || k.starts_with("jwt_") || k.contains("_jwt") {
        return true;
    }
    for needle in ["password", "token", "private_key", "secret"] {
        if k == needle || k.ends_with(&format!("_{needle}")) || k.contains(&format!("{needle}_")) {
            return true;
        }
        if k.contains(needle) {
            return true;
        }
    }
    false
}

/// Recursively replace secret string values with `***`.
pub(crate) fn redact_secrets(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                if is_secret_key(&k) {
                    out.insert(k, redact_secret_value(v));
                } else {
                    out.insert(k, redact_secrets(v));
                }
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(redact_secrets).collect()),
        other => other,
    }
}

fn redact_secret_value(v: Value) -> Value {
    match v {
        Value::Null => Value::Null,
        Value::String(s) if s.is_empty() => Value::String(String::new()),
        Value::Object(_) | Value::Array(_) => redact_secrets(v),
        _ => Value::String(REDACTED.into()),
    }
}

/// Prepare a GET config body: redact unless `reveal` (already authorized).
pub(crate) fn prepare_get_config(runtime: &[u8], file: Option<Value>, reveal: bool) -> Result<Vec<u8>> {
    let runtime_val: Value = match serde_json::from_slice(runtime) {
        Ok(v) => v,
        Err(_) => {
            return Ok(runtime.to_vec());
        }
    };
    let body = if reveal { file.unwrap_or(runtime_val) } else { redact_secrets(runtime_val) };
    Ok(serde_json::to_vec(&body)?)
}

fn flatten_keys(value: &Value, prefix: &str, out: &mut BTreeMap<String, String>) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let path = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                flatten_keys(v, &path, out);
            }
        }
        Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                let path = if prefix.is_empty() { format!("[{i}]") } else { format!("{prefix}[{i}]") };
                flatten_keys(v, &path, out);
            }
        }
        Value::Null => {
            out.insert(prefix.to_string(), "null".into());
        }
        other => {
            out.insert(prefix.to_string(), other.to_string());
        }
    }
}

pub(crate) fn diff_json(old: &Value, new: &Value) -> ConfigDiff {
    let mut a = BTreeMap::new();
    let mut b = BTreeMap::new();
    flatten_keys(old, "", &mut a);
    flatten_keys(new, "", &mut b);
    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();
    for (k, bv) in &b {
        match a.get(k) {
            None => added.push(k.clone()),
            Some(av) if av != bv => changed.push(k.clone()),
            Some(_) => {}
        }
    }
    for k in a.keys() {
        if !b.contains_key(k) {
            removed.push(k.clone());
        }
    }
    added.sort();
    removed.sort();
    changed.sort();
    ConfigDiff { added, removed, changed }
}

fn strip_nulls(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                if !v.is_null() {
                    out.insert(k, strip_nulls(v));
                }
            }
            Value::Object(out)
        }
        Value::Array(items) => {
            Value::Array(items.into_iter().filter(|v| !v.is_null()).map(strip_nulls).collect())
        }
        other => other,
    }
}

fn json_to_toml(value: &Value) -> Result<String> {
    let cleaned = strip_nulls(value.clone());
    if !cleaned.is_object() {
        return Err(anyhow!("config body must be a JSON object or a TOML table"));
    }
    toml::to_string_pretty(&cleaned).map_err(|e| anyhow!("serialize TOML: {e}"))
}

fn toml_to_json(text: &str) -> Result<Value> {
    let val: toml::Value = toml::from_str(text).map_err(|e| anyhow!("invalid TOML: {e}"))?;
    serde_json::to_value(val).map_err(|e| anyhow!("TOML to JSON: {e}"))
}

/// Parse a PUT/POST body: JSON object, `{ "toml": "..." }`, or raw TOML.
pub(crate) fn parse_config_body(bytes: &[u8], content_type: Option<&str>) -> Result<(Value, String)> {
    let text = std::str::from_utf8(bytes).map_err(|_| anyhow!("config body must be UTF-8"))?.trim();
    if text.is_empty() {
        return Err(anyhow!("config body is empty"));
    }
    let ct = content_type.unwrap_or("").to_ascii_lowercase();
    let prefer_toml = ct.contains("toml") || ct.contains("text/plain");
    if prefer_toml {
        let json = toml_to_json(text)?;
        return Ok((json, normalize_toml(text)?));
    }
    if let Ok(v) = serde_json::from_str::<Value>(text) {
        if let Some(toml_str) = v.get("toml").and_then(|x| x.as_str()) {
            if v.as_object().map(|o| o.len() == 1).unwrap_or(false) {
                let json = toml_to_json(toml_str)?;
                return Ok((json, normalize_toml(toml_str)?));
            }
        }
        if !v.is_object() {
            return Err(anyhow!("JSON config body must be an object"));
        }
        let toml = json_to_toml(&v)?;
        return Ok((v, toml));
    }
    let json = toml_to_json(text)?;
    Ok((json, normalize_toml(text)?))
}

fn normalize_toml(text: &str) -> Result<String> {
    let json = toml_to_json(text)?;
    json_to_toml(&json)
}

fn sanitize_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.contains('/')
        || name.contains('\\')
        || name.contains("..")
        || name.contains('\0')
    {
        return Err(anyhow!("invalid plugin name"));
    }
    Ok(())
}

fn now_version() -> String {
    let ms = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
    ms.to_string()
}

fn plugin_toml_path(dir: &str, name: &str) -> PathBuf {
    let dir = dir.trim_end_matches(['/', '\\']);
    PathBuf::from(format!("{dir}/{name}.toml"))
}

fn history_dir(dir: &str, name: &str) -> PathBuf {
    let dir = dir.trim_end_matches(['/', '\\']);
    PathBuf::from(format!("{dir}/.config-history/{name}"))
}

fn atomic_write(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("toml.tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(contents.as_bytes())?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

fn backup_current(path: &Path, hist: &Path, keep: usize) -> Result<Option<String>> {
    if !path.is_file() {
        return Ok(None);
    }
    fs::create_dir_all(hist)?;
    let mut version = now_version();
    let mut dest = hist.join(format!("{version}.toml"));
    let mut n = 0u32;
    while dest.exists() && n < 16 {
        n += 1;
        version = format!("{version}-{n}");
        dest = hist.join(format!("{version}.toml"));
    }
    fs::copy(path, &dest)?;
    prune_history(hist, keep)?;
    Ok(Some(version))
}

fn prune_history(hist: &Path, keep: usize) -> Result<()> {
    let keep = keep.max(1);
    let mut vers = list_history(hist)?;
    if vers.len() <= keep {
        return Ok(());
    }
    vers.sort_by(|a, b| b.version.cmp(&a.version));
    for extra in vers.into_iter().skip(keep) {
        let p = hist.join(format!("{}.toml", extra.version));
        let _ = fs::remove_file(p);
    }
    Ok(())
}

fn list_history(hist: &Path) -> Result<Vec<VersionInfo>> {
    let mut out = Vec::new();
    let rd = match fs::read_dir(hist) {
        Ok(rd) => rd,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e.into()),
    };
    for ent in rd {
        let ent = ent?;
        let name = ent.file_name();
        let name = name.to_string_lossy();
        if let Some(ver) = name.strip_suffix(".toml") {
            if ver.chars().all(|c| c.is_ascii_digit() || c == '-') {
                let meta = ent.metadata()?;
                let ts = ver.split('-').next().and_then(|s| s.parse::<i64>().ok()).unwrap_or(0) / 1000;
                out.push(VersionInfo { version: ver.to_string(), ts, size: meta.len() });
            }
        }
    }
    out.sort_by(|a, b| b.version.cmp(&a.version));
    Ok(out)
}

fn read_file_json(path: &Path) -> Result<Option<Value>> {
    if !path.is_file() {
        return Ok(None);
    }
    let text = fs::read_to_string(path)?;
    Ok(Some(toml_to_json(&text)?))
}

fn plugin_config_dir(scx: &ServerContext) -> Result<&str> {
    scx.plugins.config_dir().filter(|p| !p.is_empty()).ok_or_else(|| {
        anyhow!("plugin config directory is not configured (ServerContext::plugins_config_dir)")
    })
}

fn plugin_reloadable(scx: &ServerContext, name: &str) -> Result<bool> {
    match scx.plugins.get(name) {
        Some(entry) => Ok(!entry.immutable() && entry.inited()),
        None => Err(anyhow!(format!("{name} the plug-in does not exist"))),
    }
}

/// Mode a write *would* have if `apply` is honored and `load_config` works.
fn planned_effective(reloadable: bool, apply: bool) -> EffectiveMode {
    if reloadable && apply {
        EffectiveMode::Hot
    } else if reloadable {
        EffectiveMode::Reload
    } else {
        EffectiveMode::RestartRequired
    }
}

/// Mode after an actual write. `applied` means `load_config` succeeded.
fn after_write_effective(reloadable: bool, apply: bool, applied: bool) -> EffectiveMode {
    if applied {
        EffectiveMode::Hot
    } else if reloadable && !apply {
        EffectiveMode::Reload
    } else {
        EffectiveMode::RestartRequired
    }
}

pub(crate) fn local_validate_plugin(
    scx: &ServerContext,
    name: &str,
    json: &Value,
    apply: bool,
) -> Result<ValidateResult> {
    sanitize_name(name)?;
    if !json.is_object() {
        return Ok(ValidateResult {
            ok: false,
            valid: false,
            effective: EffectiveMode::RestartRequired,
            diff: ConfigDiff::default(),
            errors: vec!["config body must be an object / table".into()],
            plugin: Some(name.into()),
            node: Some(scx.node.id()),
            section: None,
            note: None,
        });
    }
    let _toml = json_to_toml(json)?;
    let reloadable = plugin_reloadable(scx, name)?;
    let old = match plugin_config_dir(scx) {
        Ok(dir) => read_file_json(&plugin_toml_path(dir, name))?.unwrap_or(Value::Object(Default::default())),
        Err(_) => Value::Object(Default::default()),
    };
    let effective = planned_effective(reloadable, apply);
    let note = match effective {
        EffectiveMode::Hot => {
            Some("Would write the file and apply via plugin load_config (not a ferromqd restart).".into())
        }
        EffectiveMode::Reload => {
            Some("File would be written; PUT .../config/reload (or apply=reload) applies it.".into())
        }
        EffectiveMode::RestartRequired => {
            Some("Plugin cannot apply this at runtime (immutable or not initialized).".into())
        }
    };
    Ok(ValidateResult {
        ok: true,
        valid: true,
        effective,
        diff: diff_json(&old, json),
        errors: vec![],
        plugin: Some(name.into()),
        node: Some(scx.node.id()),
        section: None,
        note,
    })
}

pub(crate) async fn local_write_plugin(
    scx: &ServerContext,
    name: &str,
    json: &Value,
    toml_text: &str,
    apply: bool,
    keep: usize,
) -> Result<WriteResult> {
    sanitize_name(name)?;
    let reloadable = plugin_reloadable(scx, name)?;
    let dir = plugin_config_dir(scx)?;
    let path = plugin_toml_path(dir, name);
    let hist = history_dir(dir, name);
    let old = read_file_json(&path)?.unwrap_or(Value::Object(Default::default()));
    let backup = backup_current(&path, &hist, keep)?;
    atomic_write(&path, toml_text)?;

    let mut applied = false;
    let mut apply_error = None;
    if apply && reloadable {
        match scx.plugins.load_config(name).await {
            Ok(()) => applied = true,
            Err(e) => apply_error = Some(e.to_string()),
        }
    }
    let effective = after_write_effective(reloadable, apply, applied);
    let note = if applied {
        Some("Applied via plugin load_config (in-process). This is not a ferromqd restart.".into())
    } else if effective == EffectiveMode::Reload {
        Some("File written. Call PUT .../config/reload to apply.".into())
    } else {
        Some("File written. Runtime apply is not available; restart is required.".into())
    };
    Ok(WriteResult {
        ok: true,
        written: true,
        applied,
        effective,
        diff: diff_json(&old, json),
        backup,
        apply_error,
        plugin: Some(name.into()),
        node: Some(scx.node.id()),
        section: None,
        note,
    })
}

pub(crate) fn local_list_plugin_versions(scx: &ServerContext, name: &str) -> Result<Vec<VersionInfo>> {
    sanitize_name(name)?;
    let _ = plugin_reloadable(scx, name)?;
    let dir = plugin_config_dir(scx)?;
    list_history(&history_dir(dir, name))
}

pub(crate) async fn local_rollback_plugin(
    scx: &ServerContext,
    name: &str,
    version: &str,
    apply: bool,
    keep: usize,
) -> Result<WriteResult> {
    sanitize_name(name)?;
    if version.is_empty()
        || version.contains('/')
        || version.contains('\\')
        || version.contains("..")
        || !version.chars().all(|c| c.is_ascii_digit() || c == '-')
    {
        return Err(anyhow!("invalid version id"));
    }
    let reloadable = plugin_reloadable(scx, name)?;
    let dir = plugin_config_dir(scx)?;
    let path = plugin_toml_path(dir, name);
    let hist = history_dir(dir, name);
    let src = hist.join(format!("{version}.toml"));
    if !src.is_file() {
        return Err(anyhow!(format!("version not found: {version}")));
    }
    let toml_text = fs::read_to_string(&src)?;
    let json = toml_to_json(&toml_text)?;
    let old = read_file_json(&path)?.unwrap_or(Value::Object(Default::default()));
    let backup = backup_current(&path, &hist, keep)?;
    atomic_write(&path, &toml_text)?;

    let mut applied = false;
    let mut apply_error = None;
    if apply && reloadable {
        match scx.plugins.load_config(name).await {
            Ok(()) => applied = true,
            Err(e) => apply_error = Some(e.to_string()),
        }
    }
    let effective = after_write_effective(reloadable, apply, applied);
    Ok(WriteResult {
        ok: true,
        written: true,
        applied,
        effective,
        diff: diff_json(&old, &json),
        backup,
        apply_error,
        plugin: Some(name.into()),
        node: Some(scx.node.id()),
        section: None,
        note: Some(format!("Rolled back to version {version}")),
    })
}

fn resolve_broker_file(cfg_path: Option<&str>) -> Option<PathBuf> {
    if let Some(p) = cfg_path.filter(|s| !s.is_empty()) {
        return Some(PathBuf::from(p));
    }
    if let Some(p) = std::env::var_os("FERROMQ_CONFIG") {
        return Some(PathBuf::from(p));
    }
    for cand in ["ferromq.toml", "/etc/ferromq/ferromq.toml"] {
        let p = PathBuf::from(cand);
        if p.is_file() {
            return Some(p);
        }
    }
    None
}

fn read_broker_doc(path: &Path) -> Result<Value> {
    let text =
        fs::read_to_string(path).map_err(|e| anyhow!("cannot read broker config {}: {e}", path.display()))?;
    toml_to_json(&text)
}

fn section_of<'a>(doc: &'a Value, section: &str) -> Result<&'a Value> {
    match section {
        "mqtt" | "listener" | "log" => Ok(doc.get(section).unwrap_or(&Value::Null)),
        "listeners" => Ok(doc.get("listener").unwrap_or(&Value::Null)),
        other => Err(anyhow!("unknown broker section '{other}'; expected mqtt, listener, or log")),
    }
}

fn normalize_section(section: &str) -> Result<&'static str> {
    match section {
        "mqtt" => Ok("mqtt"),
        "listener" | "listeners" => Ok("listener"),
        "log" => Ok("log"),
        other => Err(anyhow!("unknown broker section '{other}'; expected mqtt, listener, or log")),
    }
}

fn validate_broker_section(section: &str, value: &Value) -> Result<()> {
    if !value.is_object() && !value.is_null() {
        return Err(anyhow!("{section} must be a table / object"));
    }
    match section {
        "mqtt" => {
            let obj = value.as_object().cloned().unwrap_or_default();
            for k in obj.keys() {
                if !matches!(k.as_str(), "delayed_publish_max" | "delayed_publish_immediate" | "max_sessions")
                {
                    return Err(anyhow!("unknown mqtt key '{k}'"));
                }
            }
            if let Some(v) = obj.get("delayed_publish_max") {
                if !v.is_u64() && !v.is_i64() {
                    return Err(anyhow!("mqtt.delayed_publish_max must be an integer"));
                }
            }
            if let Some(v) = obj.get("delayed_publish_immediate") {
                if !v.is_boolean() {
                    return Err(anyhow!("mqtt.delayed_publish_immediate must be a boolean"));
                }
            }
            if let Some(v) = obj.get("max_sessions") {
                if !v.is_i64() && !v.is_u64() {
                    return Err(anyhow!("mqtt.max_sessions must be an integer"));
                }
            }
        }
        "log" => {
            let obj = value.as_object().cloned().unwrap_or_default();
            for k in obj.keys() {
                if !matches!(k.as_str(), "to" | "level" | "dir" | "file") {
                    return Err(anyhow!("unknown log key '{k}'"));
                }
            }
            if let Some(v) = obj.get("to").and_then(|x| x.as_str()) {
                if !matches!(v, "off" | "file" | "console" | "both") {
                    return Err(anyhow!("log.to must be off|file|console|both"));
                }
            }
            if let Some(v) = obj.get("level").and_then(|x| x.as_str()) {
                if !matches!(v, "trace" | "debug" | "info" | "warn" | "error") {
                    return Err(anyhow!("log.level must be trace|debug|info|warn|error"));
                }
            }
        }
        "listener" => {
            if !value.is_object() {
                return Err(anyhow!("listener must be a table / object"));
            }
        }
        _ => {}
    }
    Ok(())
}

fn merge_object(base: Value, patch: Value) -> Value {
    match (base, patch) {
        (Value::Object(mut a), Value::Object(b)) => {
            for (k, v) in b {
                let next = match a.remove(&k) {
                    Some(prev) if prev.is_object() && v.is_object() => merge_object(prev, v),
                    _ => v,
                };
                a.insert(k, next);
            }
            Value::Object(a)
        }
        (_, patch) => patch,
    }
}

fn write_broker_section(path: &Path, section: &str, patch: &Value, keep: usize) -> Result<WriteResult> {
    let section = normalize_section(section)?;
    validate_broker_section(section, patch)?;
    let old_doc = read_broker_doc(path)?;
    let old_sec = old_doc.get(section).cloned().unwrap_or(json!({}));
    let new_sec = merge_object(old_sec.clone(), patch.clone());
    validate_broker_section(section, &new_sec)?;
    let mut new_doc = old_doc.clone();
    if let Some(obj) = new_doc.as_object_mut() {
        obj.insert(section.to_string(), new_sec.clone());
    }
    let hist = history_dir(
        path.parent().and_then(|p| p.to_str()).unwrap_or("."),
        path.file_stem().and_then(|s| s.to_str()).unwrap_or("ferromq"),
    );
    let backup = backup_current(path, &hist, keep)?;
    let toml_text = json_to_toml(&new_doc)?;
    atomic_write(path, &toml_text)?;
    Ok(WriteResult {
        ok: true,
        written: true,
        applied: false,
        effective: EffectiveMode::RestartRequired,
        diff: diff_json(&old_sec, &new_sec),
        backup,
        apply_error: None,
        plugin: None,
        node: None,
        section: Some(section.into()),
        note: Some(
            "Wrote ferromq.toml only. ferromqd does not hot-apply mqtt/listener/log; restart the process."
                .into(),
        ),
    })
}

fn broker_overview(path: &Path, reveal: bool) -> Result<Value> {
    let doc = read_broker_doc(path)?;
    let mqtt = doc.get("mqtt").cloned().unwrap_or(json!({}));
    let listener = doc.get("listener").cloned().unwrap_or(json!({}));
    let log = doc.get("log").cloned().unwrap_or(json!({}));
    let mut body = json!({
        "file": path.display().to_string(),
        "writable_sections": BROKER_WRITABLE,
        "effective": EffectiveMode::RestartRequired.as_str(),
        "note": "Read from ferromq.toml. Writing mqtt/listener/log updates the file only; ferromqd does not hot-restart.",
        "mqtt": mqtt,
        "listener": listener,
        "log": log,
    });
    if !reveal {
        body = redact_secrets(body);
    }
    Ok(body)
}

fn history_keep(cfg: &super::config::PluginConfig) -> usize {
    if cfg.config_history_keep == 0 {
        DEFAULT_HISTORY_KEEP
    } else {
        cfg.config_history_keep
    }
}

fn scx_cfg(depot: &mut Depot) -> std::result::Result<(ServerContext, PluginConfigType), salvo::Error> {
    let pair = depot.obtain::<(ServerContext, PluginConfigType)>().map_err(|e| match e {
        None => salvo::Error::Io(std::io::Error::new(ErrorKind::NotFound, anyhow!("None"))),
        Some(e) => salvo::Error::Io(std::io::Error::new(ErrorKind::NotFound, format!("{e:?}"))),
    })?;
    Ok((pair.0.clone(), pair.1.clone()))
}

fn path_node_plugin(req: &Request, res: &mut Response) -> Option<(NodeId, String)> {
    let node_id = match req.param::<NodeId>("node") {
        Some(n) => n,
        None => {
            render_not_found(res, "node not found");
            return None;
        }
    };
    let name = match req.param::<String>("plugin") {
        Some(n) => n,
        None => {
            render_not_found(res, "plugin not found");
            return None;
        }
    };
    Some((node_id, name))
}

async fn read_body(req: &mut Request) -> Result<(Vec<u8>, Option<String>)> {
    let ct = req.content_type().map(|m| m.to_string());
    let bytes = req.payload().await.map(|b| b.to_vec()).map_err(|e| anyhow!("read body: {e}"))?;
    Ok((bytes, ct))
}

async fn remote_json(
    scx: &ServerContext,
    node_id: NodeId,
    message_type: MessageType,
    msg: Message<'_>,
) -> Result<Value> {
    let c = crate::api::get_grpc_client(scx, node_id).await?;
    let encoded = msg.encode()?;
    let reply = MessageSender::new_quick(
        c,
        message_type,
        GrpcMessage::Data(encoded),
        Some(std::time::Duration::from_secs(15)),
    )
    .send()
    .await?;
    match reply {
        GrpcMessageReply::Data(raw) => match MessageReply::decode(&raw)? {
            MessageReply::WritePluginConfig(s)
            | MessageReply::ValidatePluginConfig(s)
            | MessageReply::ListPluginConfigVersions(s)
            | MessageReply::RollbackPluginConfig(s) => {
                serde_json::from_str(&s).map_err(|e| anyhow!("decode remote config reply: {e}"))
            }
            _ => Err(anyhow!("unexpected plugin-config gRPC reply")),
        },
        other => Err(anyhow!("invalid gRPC reply: {other:?}")),
    }
}

/// Enhance GET /plugins/{node}/{plugin}/config with secret redaction.
pub(crate) async fn redact_get_config(
    scx: &ServerContext,
    name: &str,
    raw: Vec<u8>,
    reveal: bool,
) -> Result<Vec<u8>> {
    let file = match scx.plugins.config_dir() {
        Some(dir) => read_file_json(&plugin_toml_path(dir, name)).ok().flatten(),
        None => None,
    };
    prepare_get_config(&raw, file, reveal)
}

#[handler]
pub(crate) async fn node_plugin_config_update(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = scx_cfg(depot)?;
    let Some((node_id, name)) = path_node_plugin(req, res) else {
        return Ok(());
    };
    let apply = match parse_apply(req) {
        Ok(v) => v,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let (bytes, ct) = match read_body(req).await {
        Ok(v) => v,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let (json, toml_text) = match parse_config_body(&bytes, ct.as_deref()) {
        Ok(v) => v,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let keep = history_keep(&*cfg.read().await);
    let message_type = cfg.read().await.message_type;
    let result = if node_id == scx.node.id() {
        local_write_plugin(&scx, &name, &json, &toml_text, apply, keep).await
    } else {
        remote_json(
            &scx,
            node_id,
            message_type,
            Message::WritePluginConfig { name: &name, toml: &toml_text, apply },
        )
        .await
        .and_then(|v| serde_json::from_value(v).map_err(|e| anyhow!(e)))
    };
    match result {
        Ok(r) => {
            audit::record(
                req,
                depot,
                "plugin_config_update",
                Some(format!("{node_id}/{name}")),
                r.ok && r.written,
                Some(json!({
                    "effective": r.effective,
                    "applied": r.applied,
                    "diff": r.diff,
                    "apply_error": r.apply_error,
                })),
            )
            .await;
            res.render(Json(r));
        }
        Err(e) => {
            audit::record(
                req,
                depot,
                "plugin_config_update",
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

#[handler]
pub(crate) async fn node_plugin_config_validate(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = scx_cfg(depot)?;
    let Some((node_id, name)) = path_node_plugin(req, res) else {
        return Ok(());
    };
    let apply = parse_apply(req).unwrap_or(true);
    let (bytes, ct) = match read_body(req).await {
        Ok(v) => v,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let (json, toml_text) = match parse_config_body(&bytes, ct.as_deref()) {
        Ok(v) => v,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let message_type = cfg.read().await.message_type;
    let result = if node_id == scx.node.id() {
        local_validate_plugin(&scx, &name, &json, apply)
    } else {
        remote_json(
            &scx,
            node_id,
            message_type,
            Message::ValidatePluginConfig { name: &name, toml: &toml_text },
        )
        .await
        .and_then(|v| serde_json::from_value(v).map_err(|e| anyhow!(e)))
    };
    match result {
        Ok(r) => {
            if r.valid {
                res.render(Json(r));
            } else {
                res.status_code(StatusCode::BAD_REQUEST);
                res.render(Json(r));
            }
        }
        Err(e) => render_api_error(res, status_for_plugin_error(&e), e.to_string()),
    }
    Ok(())
}

#[handler]
pub(crate) async fn node_plugin_config_versions(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = scx_cfg(depot)?;
    let Some((node_id, name)) = path_node_plugin(req, res) else {
        return Ok(());
    };
    let message_type = cfg.read().await.message_type;
    let result = if node_id == scx.node.id() {
        local_list_plugin_versions(&scx, &name).map(|v| json!(v))
    } else {
        remote_json(&scx, node_id, message_type, Message::ListPluginConfigVersions { name: &name }).await
    };
    match result {
        Ok(v) => res.render(Json(v)),
        Err(e) => render_api_error(res, status_for_plugin_error(&e), e.to_string()),
    }
    Ok(())
}

#[handler]
pub(crate) async fn node_plugin_config_rollback(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (scx, cfg) = scx_cfg(depot)?;
    let Some((node_id, name)) = path_node_plugin(req, res) else {
        return Ok(());
    };
    let version = match req.param::<String>("version") {
        Some(v) => v,
        None => {
            render_not_found(res, "version not found");
            return Ok(());
        }
    };
    let apply = parse_apply(req).unwrap_or(true);
    let keep = history_keep(&*cfg.read().await);
    let message_type = cfg.read().await.message_type;
    let result = if node_id == scx.node.id() {
        local_rollback_plugin(&scx, &name, &version, apply, keep).await
    } else {
        remote_json(
            &scx,
            node_id,
            message_type,
            Message::RollbackPluginConfig { name: &name, version: &version, apply },
        )
        .await
        .and_then(|v| serde_json::from_value(v).map_err(|e| anyhow!(e)))
    };
    match result {
        Ok(r) => {
            audit::record(
                req,
                depot,
                "plugin_config_rollback",
                Some(format!("{node_id}/{name}#{version}")),
                r.ok && r.written,
                Some(json!({"effective": r.effective, "applied": r.applied, "backup": r.backup})),
            )
            .await;
            res.render(Json(r));
        }
        Err(e) => {
            audit::record(
                req,
                depot,
                "plugin_config_rollback",
                Some(format!("{node_id}/{name}#{version}")),
                false,
                Some(json!({"error": e.to_string()})),
            )
            .await;
            let status = if e.to_string().contains("version not found") {
                StatusCode::NOT_FOUND
            } else {
                status_for_plugin_error(&e)
            };
            render_api_error(res, status, e.to_string());
        }
    }
    Ok(())
}

fn deny_reveal_if_needed(req: &Request, depot: &Depot, res: &mut Response) -> bool {
    if !wants_reveal(req) {
        return false;
    }
    match identity_from_depot(depot) {
        Some(id) if id.can_admin() => false,
        Some(id) => {
            render_api_error_with(
                res,
                StatusCode::FORBIDDEN,
                "forbidden",
                Some(json!({
                    "required_role": "admin",
                    "role": id.role.as_str(),
                    "reason": "reveal=1 requires admin",
                })),
            );
            true
        }
        None => {
            render_api_error(res, StatusCode::UNAUTHORIZED, "unauthorized");
            true
        }
    }
}

#[handler]
pub(crate) async fn broker_config_get(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    if deny_reveal_if_needed(req, depot, res) {
        return Ok(());
    }
    let reveal = wants_reveal(req);
    let (_scx, cfg) = scx_cfg(depot)?;
    let cfg = cfg.read().await;
    let Some(path) = resolve_broker_file(cfg.broker_config_file.as_deref()) else {
        render_not_found(res, "broker config file not found (set broker_config_file or FERROMQ_CONFIG)");
        return Ok(());
    };
    if !path.is_file() {
        render_not_found(res, format!("broker config file not found: {}", path.display()));
        return Ok(());
    }
    match broker_overview(&path, reveal) {
        Ok(v) => res.render(Json(v)),
        Err(e) => render_api_error(res, StatusCode::BAD_REQUEST, e.to_string()),
    }
    Ok(())
}

#[handler]
pub(crate) async fn broker_config_section_get(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    if deny_reveal_if_needed(req, depot, res) {
        return Ok(());
    }
    let reveal = wants_reveal(req);
    let section = match req.param::<String>("section") {
        Some(s) => s,
        None => {
            render_not_found(res, "section not found");
            return Ok(());
        }
    };
    let (_scx, cfg) = scx_cfg(depot)?;
    let cfg = cfg.read().await;
    let Some(path) = resolve_broker_file(cfg.broker_config_file.as_deref()) else {
        render_not_found(res, "broker config file not found (set broker_config_file or FERROMQ_CONFIG)");
        return Ok(());
    };
    if !path.is_file() {
        render_not_found(res, format!("broker config file not found: {}", path.display()));
        return Ok(());
    }
    match read_broker_doc(&path).and_then(|doc| {
        let key = normalize_section(&section)?;
        let mut body = json!({
            "file": path.display().to_string(),
            "section": key,
            "effective": EffectiveMode::RestartRequired.as_str(),
            "writable": true,
            "note": "Writing this section updates ferromq.toml only; restart ferromqd to apply.",
            "config": section_of(&doc, key)?.clone(),
        });
        if !reveal {
            body = redact_secrets(body);
        }
        Ok(body)
    }) {
        Ok(v) => res.render(Json(v)),
        Err(e) => render_api_error(res, StatusCode::BAD_REQUEST, e.to_string()),
    }
    Ok(())
}

#[handler]
pub(crate) async fn broker_config_section_put(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let section = match req.param::<String>("section") {
        Some(s) => s,
        None => {
            render_not_found(res, "section not found");
            return Ok(());
        }
    };
    let (_scx, cfg) = scx_cfg(depot)?;
    let keep = history_keep(&*cfg.read().await);
    let file = cfg.read().await.broker_config_file.clone();
    let Some(path) = resolve_broker_file(file.as_deref()) else {
        render_not_found(res, "broker config file not found (set broker_config_file or FERROMQ_CONFIG)");
        return Ok(());
    };
    if !path.is_file() {
        render_not_found(res, format!("broker config file not found: {}", path.display()));
        return Ok(());
    }
    let (bytes, ct) = match read_body(req).await {
        Ok(v) => v,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let (json, _) = match parse_config_body(&bytes, ct.as_deref()) {
        Ok(v) => v,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    match write_broker_section(&path, &section, &json, keep) {
        Ok(r) => {
            audit::record(
                req,
                depot,
                "broker_config_update",
                Some(section.clone()),
                true,
                Some(json!({"effective": r.effective, "diff": r.diff, "file": path.display().to_string()})),
            )
            .await;
            res.render(Json(r));
        }
        Err(e) => {
            audit::record(
                req,
                depot,
                "broker_config_update",
                Some(section),
                false,
                Some(json!({"error": e.to_string()})),
            )
            .await;
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
        }
    }
    Ok(())
}

#[handler]
pub(crate) async fn broker_config_section_validate(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let section = match req.param::<String>("section") {
        Some(s) => s,
        None => {
            render_not_found(res, "section not found");
            return Ok(());
        }
    };
    let (_scx, cfg) = scx_cfg(depot)?;
    let file = cfg.read().await.broker_config_file.clone();
    let Some(path) = resolve_broker_file(file.as_deref()) else {
        render_not_found(res, "broker config file not found (set broker_config_file or FERROMQ_CONFIG)");
        return Ok(());
    };
    let (bytes, ct) = match read_body(req).await {
        Ok(v) => v,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    let (json, _) = match parse_config_body(&bytes, ct.as_deref()) {
        Ok(v) => v,
        Err(e) => {
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
            return Ok(());
        }
    };
    match (|| -> Result<ValidateResult> {
        let key = normalize_section(&section)?;
        validate_broker_section(key, &json)?;
        let old = if path.is_file() {
            let doc = read_broker_doc(&path)?;
            doc.get(key).cloned().unwrap_or(json!({}))
        } else {
            json!({})
        };
        let new_sec = merge_object(old.clone(), json);
        validate_broker_section(key, &new_sec)?;
        Ok(ValidateResult {
            ok: true,
            valid: true,
            effective: EffectiveMode::RestartRequired,
            diff: diff_json(&old, &new_sec),
            errors: vec![],
            plugin: None,
            node: None,
            section: Some(key.into()),
            note: Some("Dry-run only. A write would still require a ferromqd restart to apply.".into()),
        })
    })() {
        Ok(r) => res.render(Json(r)),
        Err(e) => {
            res.status_code(StatusCode::BAD_REQUEST);
            res.render(Json(ValidateResult {
                ok: false,
                valid: false,
                effective: EffectiveMode::RestartRequired,
                diff: ConfigDiff::default(),
                errors: vec![e.to_string()],
                plugin: None,
                node: None,
                section: Some(section),
                note: None,
            }));
        }
    }
    Ok(())
}

#[handler]
pub(crate) async fn broker_config_versions(
    _req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let (_scx, cfg) = scx_cfg(depot)?;
    let file = cfg.read().await.broker_config_file.clone();
    let Some(path) = resolve_broker_file(file.as_deref()) else {
        render_not_found(res, "broker config file not found");
        return Ok(());
    };
    let hist = history_dir(
        path.parent().and_then(|p| p.to_str()).unwrap_or("."),
        path.file_stem().and_then(|s| s.to_str()).unwrap_or("ferromq"),
    );
    match list_history(&hist) {
        Ok(v) => res.render(Json(v)),
        Err(e) => render_api_error(res, StatusCode::BAD_REQUEST, e.to_string()),
    }
    Ok(())
}

#[handler]
pub(crate) async fn broker_config_rollback(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
) -> std::result::Result<(), salvo::Error> {
    let version = match req.param::<String>("version") {
        Some(v) => v,
        None => {
            render_not_found(res, "version not found");
            return Ok(());
        }
    };
    if version.contains('/')
        || version.contains("..")
        || !version.chars().all(|c| c.is_ascii_digit() || c == '-')
    {
        render_api_error(res, StatusCode::BAD_REQUEST, "invalid version id");
        return Ok(());
    }
    let (_scx, cfg) = scx_cfg(depot)?;
    let keep = history_keep(&*cfg.read().await);
    let file = cfg.read().await.broker_config_file.clone();
    let Some(path) = resolve_broker_file(file.as_deref()) else {
        render_not_found(res, "broker config file not found");
        return Ok(());
    };
    let hist = history_dir(
        path.parent().and_then(|p| p.to_str()).unwrap_or("."),
        path.file_stem().and_then(|s| s.to_str()).unwrap_or("ferromq"),
    );
    let src = hist.join(format!("{version}.toml"));
    if !src.is_file() {
        render_not_found(res, format!("version not found: {version}"));
        return Ok(());
    }
    match (|| -> Result<WriteResult> {
        let toml_text = fs::read_to_string(&src)?;
        let new_doc = toml_to_json(&toml_text)?;
        let old_doc = if path.is_file() { read_broker_doc(&path)? } else { json!({}) };
        let backup = backup_current(&path, &hist, keep)?;
        atomic_write(&path, &toml_text)?;
        Ok(WriteResult {
            ok: true,
            written: true,
            applied: false,
            effective: EffectiveMode::RestartRequired,
            diff: diff_json(&old_doc, &new_doc),
            backup,
            apply_error: None,
            plugin: None,
            node: None,
            section: None,
            note: Some(format!(
                "Rolled ferromq.toml back to {version}. Restart ferromqd to apply; no hot restart."
            )),
        })
    })() {
        Ok(r) => {
            audit::record(
                req,
                depot,
                "broker_config_rollback",
                Some(version),
                true,
                Some(json!({"effective": r.effective, "backup": r.backup})),
            )
            .await;
            res.render(Json(r));
        }
        Err(e) => {
            audit::record(
                req,
                depot,
                "broker_config_rollback",
                Some(version),
                false,
                Some(json!({"error": e.to_string()})),
            )
            .await;
            render_api_error(res, StatusCode::BAD_REQUEST, e.to_string());
        }
    }
    Ok(())
}

/// Used by gRPC hook handler on the receiving node.
pub(crate) async fn grpc_write_plugin(
    scx: &ServerContext,
    name: &str,
    toml_text: &str,
    apply: bool,
) -> Result<String> {
    let json = toml_to_json(toml_text)?;
    let r = local_write_plugin(scx, name, &json, toml_text, apply, DEFAULT_HISTORY_KEEP).await?;
    Ok(serde_json::to_string(&r)?)
}

pub(crate) fn grpc_validate_plugin(scx: &ServerContext, name: &str, toml_text: &str) -> Result<String> {
    let json = toml_to_json(toml_text)?;
    let r = local_validate_plugin(scx, name, &json, true)?;
    Ok(serde_json::to_string(&r)?)
}

pub(crate) fn grpc_list_versions(scx: &ServerContext, name: &str) -> Result<String> {
    Ok(serde_json::to_string(&local_list_plugin_versions(scx, name)?)?)
}

pub(crate) async fn grpc_rollback_plugin(
    scx: &ServerContext,
    name: &str,
    version: &str,
    apply: bool,
) -> Result<String> {
    let r = local_rollback_plugin(scx, name, version, apply, DEFAULT_HISTORY_KEEP).await?;
    Ok(serde_json::to_string(&r)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_keys_match_password_token_private_key_secret_jwt() {
        for k in [
            "password",
            "dashboard_admin_password",
            "http_bearer_token",
            "token",
            "private_key",
            "tls_private_key",
            "secret",
            "api_secret",
            "jwt",
            "jwt_secret",
            "jwt_key",
            "auth_jwt_secret",
        ] {
            assert!(is_secret_key(k), "{k} should be secret");
        }
        for k in ["http_laddr", "max_row_limit", "level", "dir", "max_sessions"] {
            assert!(!is_secret_key(k), "{k} should not be secret");
        }
    }

    #[test]
    fn redact_nested_secrets() {
        let v = json!({
            "http_laddr": "0.0.0.0:6060",
            "http_bearer_token": "super-secret",
            "nested": {"password": "p", "ok": 1},
            "empty_token": "",
            "null_secret": null
        });
        let r = redact_secrets(v);
        assert_eq!(r["http_laddr"], "0.0.0.0:6060");
        assert_eq!(r["http_bearer_token"], "***");
        assert_eq!(r["nested"]["password"], "***");
        assert_eq!(r["nested"]["ok"], 1);
        assert_eq!(r["empty_token"], "");
        assert!(r["null_secret"].is_null());
    }

    #[test]
    fn parse_json_and_toml_bodies() {
        let (j, t) = parse_config_body(br#"{"max_row_limit": 10}"#, Some("application/json")).unwrap();
        assert_eq!(j["max_row_limit"], 10);
        assert!(t.contains("max_row_limit"));

        let (j2, _) = parse_config_body(b"max_row_limit = 11\n", Some("application/toml")).unwrap();
        assert_eq!(j2["max_row_limit"], 11);

        let (j3, _) =
            parse_config_body(br#"{"toml":"max_row_limit = 12\n"}"#, Some("application/json")).unwrap();
        assert_eq!(j3["max_row_limit"], 12);
    }

    #[test]
    fn parse_rejects_empty_and_array() {
        assert!(parse_config_body(b"   ", Some("application/json")).is_err());
        assert!(parse_config_body(b"[1,2]", Some("application/json")).is_err());
    }

    #[test]
    fn diff_detects_added_removed_changed() {
        let old = json!({"a": 1, "b": 2});
        let new = json!({"a": 3, "c": 4});
        let d = diff_json(&old, &new);
        assert_eq!(d.changed, vec!["a".to_string()]);
        assert_eq!(d.removed, vec!["b".to_string()]);
        assert_eq!(d.added, vec!["c".to_string()]);
        assert!(!d.is_empty());
    }

    #[test]
    fn mqtt_section_rejects_unknown_keys() {
        assert!(validate_broker_section("mqtt", &json!({"max_sessions": 1})).is_ok());
        assert!(validate_broker_section("mqtt", &json!({"nope": 1})).is_err());
        assert!(validate_broker_section("log", &json!({"level": "info", "to": "console"})).is_ok());
        assert!(validate_broker_section("log", &json!({"to": "somewhere"})).is_err());
    }

    #[test]
    fn version_history_roundtrip() {
        let root =
            std::env::temp_dir().join(format!("ferromq-p4-hist-{}-{}", std::process::id(), now_version()));
        let file = root.join("demo.toml");
        let hist = root.join(".config-history/demo");
        fs::create_dir_all(&root).unwrap();
        atomic_write(&file, "a = 1\n").unwrap();
        let v1 = backup_current(&file, &hist, 2).unwrap();
        assert!(v1.is_some());
        atomic_write(&file, "a = 2\n").unwrap();
        let _v2 = backup_current(&file, &hist, 2).unwrap();
        atomic_write(&file, "a = 3\n").unwrap();
        let _v3 = backup_current(&file, &hist, 2).unwrap();
        let list = list_history(&hist).unwrap();
        assert!(list.len() <= 2, "keep last N: {:?}", list);
        let _ = fs::remove_dir_all(&root);
    }
}
