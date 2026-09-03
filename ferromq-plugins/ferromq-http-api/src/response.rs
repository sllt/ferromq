//! Shared HTTP response helpers for the management API.
//!
//! Error bodies are JSON `{ "code": <http status>, "message": "..." }`.
//! List endpoints keep their existing JSON schema and attach pagination
//! metadata via `X-Row-Count` / `X-Truncated` headers.

use salvo::http::header::{HeaderValue, CONTENT_TYPE};
use salvo::http::StatusCode;
use salvo::prelude::*;
use serde_json::json;

/// Header: number of rows in the successful JSON list body (or `items` length).
pub(crate) const HEADER_ROW_COUNT: &str = "X-Row-Count";
/// Header: `true` when the result was cut off by `_limit` / `max_row_limit`.
pub(crate) const HEADER_TRUNCATED: &str = "X-Truncated";

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

/// Render a JSON error body `{ code, message }` and set the HTTP status.
pub(crate) fn render_api_error(res: &mut Response, status: StatusCode, message: impl Into<String>) {
    res.status_code(status);
    res.add_header(CONTENT_TYPE, "application/json; charset=utf-8", true).ok();
    res.render(Json(json!({
        "code": status.as_u16(),
        "message": message.into(),
    })));
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
        let body = json!({"code": 404, "message": "plugin not found"});
        assert_eq!(body["code"], 404);
        assert!(body["message"].as_str().unwrap().contains("plugin"));
    }
}
