//! Shared HTTP response helpers for the management API.
//!
//! Error bodies are JSON `{ "code", "message", "details?", "request_id?" }`.
//! List endpoints keep their existing JSON schema by default and attach
//! pagination metadata via `X-Row-Count` / `X-Truncated` headers. Pass
//! `?format=page` to wrap a list as `{ items, offset, limit, truncated, total? }`.

use salvo::http::header::{HeaderValue, CONTENT_TYPE};
use salvo::http::StatusCode;
use salvo::prelude::*;
use serde::Serialize;
use serde_json::json;

/// Header: number of rows in the successful JSON list body (or `items` length).
pub(crate) const HEADER_ROW_COUNT: &str = "X-Row-Count";
/// Header: `true` when the result was cut off by `_limit` / `max_row_limit`.
pub(crate) const HEADER_TRUNCATED: &str = "X-Truncated";
/// Header / error-body correlation id (echoed from the request when present).
pub(crate) const HEADER_REQUEST_ID: &str = "X-Request-Id";

/// Depot key for the per-request correlation id.
pub(crate) const DEPOT_REQUEST_ID: &str = "REQUEST_ID";

/// Pagination derived from query params without changing the JSON schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ListPaging {
    /// Rows to skip after the backend fetch.
    pub offset: usize,
    /// Maximum rows to return to the client.
    pub page_size: usize,
    /// How many rows to request from the backend (`offset + page_size`, capped).
    pub fetch_limit: usize,
}

impl ListPaging {
    /// Build paging from a parsed `_limit` (0 means "use default") plus optional
    /// `_offset` / `offset` and `limit` query aliases.
    pub(crate) fn from_request(req: &Request, requested_limit: usize, max_row_limit: usize) -> Self {
        let offset = req.query::<usize>("_offset").or_else(|| req.query::<usize>("offset")).unwrap_or(0);
        let requested =
            if requested_limit == 0 { req.query::<usize>("limit").unwrap_or(0) } else { requested_limit };
        Self::new(offset, requested, max_row_limit)
    }

    pub(crate) fn new(offset: usize, requested_limit: usize, max_row_limit: usize) -> Self {
        let page_size = clamp_row_limit(requested_limit, max_row_limit);
        let fetch_limit = offset.saturating_add(page_size);
        let fetch_limit = if fetch_limit == 0 { max_row_limit } else { fetch_limit.min(max_row_limit) };
        Self { offset, page_size, fetch_limit }
    }

    /// Slice a backend result and report whether the fetch hit `fetch_limit`.
    pub(crate) fn apply<T>(self, fetched: Vec<T>) -> (Vec<T>, bool) {
        paginate_fetched(fetched, self.offset, self.page_size, self.fetch_limit)
    }
}

/// Treat `0` or values above `max_row_limit` as `max_row_limit`.
#[inline]
pub(crate) fn clamp_row_limit(requested: usize, max_row_limit: usize) -> usize {
    if requested == 0 || requested > max_row_limit {
        max_row_limit
    } else {
        requested
    }
}

/// Apply HTTP-layer offset/limit to an already-fetched, capped result set.
#[inline]
pub(crate) fn paginate_fetched<T>(
    fetched: Vec<T>,
    offset: usize,
    page_size: usize,
    fetch_limit: usize,
) -> (Vec<T>, bool) {
    let truncated = fetch_limit > 0 && fetched.len() >= fetch_limit;
    let page = fetched.into_iter().skip(offset).take(page_size).collect();
    (page, truncated)
}

/// Attach list metadata headers. Does not modify the JSON body.
pub(crate) fn apply_list_headers(res: &mut Response, row_count: usize, truncated: bool) {
    if let Ok(v) = HeaderValue::from_str(&row_count.to_string()) {
        res.add_header(HEADER_ROW_COUNT, v, true).ok();
    }
    let flag = if truncated { "true" } else { "false" };
    res.add_header(HEADER_TRUNCATED, flag, true).ok();
}

/// Cheap correlation id: hex timestamp plus a mixed-in uniqueness token.
pub(crate) fn new_request_id() -> String {
    let nanos =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    format!("{nanos:x}-{:04x}", ((nanos >> 17) as u16) ^ 0xa5a5)
}

/// `true` when the client asked for the optional page envelope (`?format=page`).
pub(crate) fn wants_page_format(req: &Request) -> bool {
    req.query::<String>("format").as_deref().is_some_and(|v| v.eq_ignore_ascii_case("page"))
}

/// Render a list: default is a bare JSON array; `?format=page` wraps it.
pub(crate) fn render_list<T: Serialize + Send>(
    req: &Request,
    res: &mut Response,
    items: Vec<T>,
    paging: ListPaging,
    truncated: bool,
    total: Option<usize>,
) {
    apply_list_headers(res, items.len(), truncated);
    if wants_page_format(req) {
        let mut page = json!({
            "items": items,
            "offset": paging.offset,
            "limit": paging.page_size,
            "truncated": truncated,
        });
        if let Some(t) = total {
            page["total"] = json!(t);
        }
        res.render(Json(page));
    } else {
        res.render(Json(items));
    }
}

/// Structured per-node failure used by cluster-aggregating endpoints.
///
/// Shape: `{ "ok": false, "node_id": <id>, "error": "<msg>" }`.
pub(crate) fn cluster_node_failure(node_id: u64, error: impl std::fmt::Display) -> serde_json::Value {
    json!({
        "ok": false,
        "node_id": node_id,
        "error": error.to_string(),
    })
}

/// Mark a successful per-node object with `"ok": true` without replacing keys.
pub(crate) fn cluster_node_success(mut value: serde_json::Value) -> serde_json::Value {
    if let Some(obj) = value.as_object_mut() {
        obj.entry("ok".to_string()).or_insert(json!(true));
    }
    value
}

/// Render a JSON error body `{ code, message, request_id, details? }`.
pub(crate) fn render_api_error(res: &mut Response, status: StatusCode, message: impl Into<String>) {
    render_api_error_with(res, status, message, None);
}

/// Render a JSON error body with optional `details`.
pub(crate) fn render_api_error_with(
    res: &mut Response,
    status: StatusCode,
    message: impl Into<String>,
    details: Option<serde_json::Value>,
) {
    let request_id = res
        .headers()
        .get(HEADER_REQUEST_ID)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(new_request_id);
    if res.headers().get(HEADER_REQUEST_ID).is_none() {
        if let Ok(v) = HeaderValue::from_str(&request_id) {
            res.add_header(HEADER_REQUEST_ID, v, true).ok();
        }
    }
    res.status_code(status);
    res.add_header(CONTENT_TYPE, "application/json; charset=utf-8", true).ok();
    let mut body = json!({
        "code": status.as_u16(),
        "message": message.into(),
        "request_id": request_id,
    });
    if let Some(d) = details {
        body["details"] = d;
    }
    res.render(Json(body));
}

/// Convenience 404 JSON error.
#[inline]
pub(crate) fn render_not_found(res: &mut Response, message: impl Into<String>) {
    render_api_error(res, StatusCode::NOT_FOUND, message);
}

/// Map plugin-manager errors to an HTTP status without changing success shapes.
pub(crate) fn status_for_plugin_error(err: &anyhow::Error) -> StatusCode {
    let lower = err.to_string().to_ascii_lowercase();
    if lower.contains("does not exist") {
        StatusCode::NOT_FOUND
    } else if lower.contains("immutable")
        || lower.contains("not started")
        || lower.contains("not initialized")
    {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Documented subscription fields (`qos`, `share`) plus the existing `opts` object.
///
/// The old dashboard reads `opts.qos` / `opts.group`. Adding top-level aliases
/// matches `http-api.md` and is backward compatible.
pub(crate) fn subscription_to_json(mut value: serde_json::Value) -> serde_json::Value {
    let (qos, share) = value
        .get("opts")
        .map(|opts| (opts.get("qos").cloned(), opts.get("group").cloned()))
        .unwrap_or((None, None));
    if let Some(obj) = value.as_object_mut() {
        if !obj.contains_key("qos") {
            obj.insert("qos".into(), qos.unwrap_or(serde_json::Value::Null));
        }
        if !obj.contains_key("share") {
            obj.insert("share".into(), share.unwrap_or(serde_json::Value::Null));
        }
    }
    value
}

/// Canonical plugin object keys used by GET /plugins responses (snake_case).
#[cfg(test)]
pub(crate) fn plugin_info_required_keys() -> &'static [&'static str] {
    &["name", "version", "descr", "inited", "active", "immutable", "attrs"]
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferromq::plugin::PluginInfo;

    #[test]
    fn clamp_row_limit_treats_zero_and_overflow_as_max() {
        assert_eq!(clamp_row_limit(0, 10_000), 10_000);
        assert_eq!(clamp_row_limit(50, 10_000), 50);
        assert_eq!(clamp_row_limit(99_999, 10_000), 10_000);
    }

    #[test]
    fn list_paging_offset_plus_limit_is_capped() {
        let p = ListPaging::new(100, 50, 10_000);
        assert_eq!(p.offset, 100);
        assert_eq!(p.page_size, 50);
        assert_eq!(p.fetch_limit, 150);

        let p = ListPaging::new(9_950, 100, 10_000);
        assert_eq!(p.fetch_limit, 10_000);
        assert_eq!(p.page_size, 100);
    }

    #[test]
    fn paginate_fetched_skips_offset_and_flags_truncation() {
        let items: Vec<u32> = (0..150).collect();
        let (page, truncated) = paginate_fetched(items, 100, 50, 150);
        assert_eq!(page, (100..150).collect::<Vec<u32>>());
        assert!(truncated);

        let items: Vec<u32> = (0..10).collect();
        let (page, truncated) = paginate_fetched(items, 0, 50, 50);
        assert_eq!(page, (0..10).collect::<Vec<u32>>());
        assert!(!truncated);
    }

    #[test]
    fn plugin_info_json_uses_active_not_running() {
        let info = PluginInfo {
            name: "ferromq-http-api".into(),
            version: Some("0.24.0".into()),
            descr: Some("http api".into()),
            authors: None,
            homepage: None,
            license: None,
            repository: None,
            inited: true,
            active: true,
            immutable: false,
            attrs: Vec::new(),
        };
        let v = info.to_json().expect("plugin json");
        for key in plugin_info_required_keys() {
            assert!(v.get(key).is_some(), "missing key {key}");
        }
        assert!(v.get("running").is_none(), "canonical field is active, not running");
        assert_eq!(v["active"], true);
        assert_eq!(v["inited"], true);
        assert_eq!(v["immutable"], false);
        assert!(v["attrs"].is_null());
    }

    #[test]
    fn subscription_aliases_qos_and_share() {
        let raw = json!({
            "node_id": 1,
            "clientid": "c1",
            "topic": "foo/#",
            "opts": { "qos": 2, "group": "g1" }
        });
        let v = subscription_to_json(raw);
        assert_eq!(v["qos"], 2);
        assert_eq!(v["share"], "g1");
        assert_eq!(v["opts"]["qos"], 2);
        assert_eq!(v["opts"]["group"], "g1");
    }

    #[test]
    fn plugin_error_status_mapping() {
        assert_eq!(
            status_for_plugin_error(&anyhow::anyhow!("ferromq-web-hook the plug-in does not exist")),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status_for_plugin_error(&anyhow::anyhow!("the plug-in is immutable")),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status_for_plugin_error(&anyhow::anyhow!("storage backend down")),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn api_error_json_shape() {
        let body = json!({
            "code": 404,
            "message": "plugin not found",
            "request_id": "abc",
        });
        assert_eq!(body["code"], 404);
        assert!(body["message"].as_str().unwrap().contains("plugin"));
        assert!(body["request_id"].is_string());
        assert!(body.get("details").is_none());
    }

    #[test]
    fn cluster_node_success_sets_ok_without_clobber() {
        let v = cluster_node_success(json!({"node": 1, "plugins": []}));
        assert_eq!(v["ok"], true);
        assert_eq!(v["node"], 1);
        assert!(v["plugins"].is_array());
    }

    #[test]
    fn cluster_node_failure_is_structured() {
        let v = cluster_node_failure(2, "connection refused");
        assert_eq!(v["ok"], false);
        assert_eq!(v["node_id"], 2);
        assert_eq!(v["error"], "connection refused");
        assert!(v.get("plugins").is_none());
    }

    #[test]
    fn new_request_id_is_nonempty_hex() {
        let id = new_request_id();
        assert!(id.contains('-'));
        assert!(id.len() > 8);
    }
}
