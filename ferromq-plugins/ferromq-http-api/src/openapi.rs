//! Serve the OpenAPI 3 description of `/api/v1` and a cheap HTML docs page.
//!
//! `GET /api/v1/openapi.json` returns the static document (embedded at compile
//! time). `GET /api/v1/docs` is a Swagger UI shell that loads that JSON.

use salvo::http::header::{HeaderValue, CONTENT_TYPE};
use salvo::prelude::*;

/// Embedded OpenAPI 3.0.3 document for the dashboard-used HTTP API surface.
pub const OPENAPI_JSON: &str = include_str!("../openapi/openapi.json");

/// Minimal Swagger UI page. The spec is always available as JSON even if the
/// CDN scripts cannot be loaded.
const DOCS_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>FerroMQ HTTP API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css"/>
  <style>body{margin:0} .fallback{font:14px/1.4 system-ui,sans-serif;padding:24px}</style>
</head>
<body>
  <div id="swagger-ui"></div>
  <noscript class="fallback">JavaScript is required to render Swagger UI. The OpenAPI document is at <a href="/api/v1/openapi.json">/api/v1/openapi.json</a>.</noscript>
  <script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
  <script>
    if (window.SwaggerUIBundle) {
      window.ui = SwaggerUIBundle({
        url: "/api/v1/openapi.json",
        dom_id: "#swagger-ui"
      });
    } else {
      document.getElementById("swagger-ui").innerHTML =
        '<p class="fallback">Swagger UI failed to load. Open <a href="/api/v1/openapi.json">/api/v1/openapi.json</a>.</p>';
    }
  </script>
</body>
</html>
"##;

/// `GET /api/v1/openapi.json`
#[handler]
pub(crate) async fn get_openapi(res: &mut Response) {
    res.add_header(CONTENT_TYPE, "application/json; charset=utf-8", true).ok();
    res.write_body(OPENAPI_JSON.as_bytes()).ok();
}

/// `GET /api/v1/docs`
#[handler]
pub(crate) async fn get_docs(res: &mut Response) {
    res.add_header(CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"), true).ok();
    res.write_body(DOCS_HTML.as_bytes()).ok();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_openapi_parses_as_openapi3() {
        let spec: serde_json::Value = serde_json::from_str(OPENAPI_JSON).expect("openapi.json is JSON");
        let ver = spec["openapi"].as_str().expect("openapi version");
        assert!(ver.starts_with("3."), "expected OpenAPI 3.x, got {ver}");
        assert_eq!(spec["info"]["title"], "FerroMQ HTTP API");
        let paths = spec["paths"].as_object().expect("paths");
        for p in [
            "/api/v1",
            "/api/v1/auth/login",
            "/api/v1/auth/logout",
            "/api/v1/auth/me",
            "/api/v1/auth/change-password",
            "/api/v1/auth/init",
            "/api/v1/openapi.json",
            "/api/v1/docs",
            "/api/v1/brokers",
            "/api/v1/nodes",
            "/api/v1/features",
            "/api/v1/health/check",
            "/api/v1/clients",
            "/api/v1/subscriptions",
            "/api/v1/routes",
            "/api/v1/retains",
            "/api/v1/mqtt/publish",
            "/api/v1/mqtt/subscribe",
            "/api/v1/mqtt/unsubscribe",
            "/api/v1/plugins",
            "/api/v1/plugins/{node}/{plugin}/config",
            "/api/v1/plugins/{node}/{plugin}/config/validate",
            "/api/v1/plugins/{node}/{plugin}/config/versions",
            "/api/v1/plugins/{node}/{plugin}/config/rollback/{version}",
            "/api/v1/broker/config",
            "/api/v1/broker/config/{section}",
            "/api/v1/acl",
            "/api/v1/acl/rules",
            "/api/v1/auth-providers",
            "/api/v1/auth-providers/{name}/test",
            "/api/v1/blacklist",
            "/api/v1/auto-subscriptions",
            "/api/v1/topic-rewrites",
            "/api/v1/webhooks",
            "/api/v1/webhooks/test",
            "/api/v1/bridges",
            "/api/v1/bridges/{plugin}",
            "/api/v1/alarms",
            "/api/v1/alarms/history",
            "/api/v1/alarms/{id}/acknowledge",
            "/api/v1/logs",
            "/api/v1/trace",
            "/api/v1/slow-subs",
            "/api/v1/topic-metrics",
            "/api/v1/cluster",
            "/api/v1/cluster/join",
            "/api/v1/cluster/leave",
            "/api/v1/stats",
            "/api/v1/metrics",
            "/api/v1/metrics/prometheus",
            "/api/v1/stats/history",
            "/api/v1/metrics/history",
        ] {
            assert!(paths.contains_key(p), "missing path {p}");
        }
        let schemas = spec["components"]["schemas"].as_object().expect("schemas");
        for s in [
            "Error",
            "Page",
            "Features",
            "FeaturesSummary",
            "FeaturesNodeResult",
            "ClusterNodeError",
            "SessionUser",
            "LoginRequest",
            "ChangePasswordRequest",
            "ChangePasswordResult",
            "InitAdminResult",
            "OkResult",
            "ConfigWriteResult",
            "ConfigValidateResult",
            "EffectiveMode",
            "BrokerConfigOverview",
            "AclRule",
            "AclOverview",
            "BlacklistGap",
            "CapabilityGap",
            "Alarm",
            "AlarmList",
            "TopicMetrics",
            "ClusterTopology",
            "ClusterWriteResult",
            "ConnectivityTest",
        ] {
            assert!(schemas.contains_key(s), "missing schema {s}");
        }
        let err = &schemas["Error"]["properties"];
        assert!(err["code"].is_object());
        assert!(err["message"].is_object());
        assert!(err["details"].is_object());
        assert!(err["request_id"].is_object());
        let page = &schemas["Page"]["required"];
        let req: Vec<&str> = page.as_array().unwrap().iter().filter_map(|v| v.as_str()).collect();
        for k in ["items", "offset", "limit", "truncated"] {
            assert!(req.contains(&k), "Page.required missing {k}");
        }
        assert_eq!(
            spec["paths"]["/api/v1/auth/logout"]["post"]["responses"]["200"]["content"]["application/json"]
                ["schema"]["$ref"],
            "#/components/schemas/OkResult"
        );
        assert_eq!(
            spec["paths"]["/api/v1/auth/init"]["post"]["responses"]["200"]["content"]["application/json"]
                ["schema"]["$ref"],
            "#/components/schemas/InitAdminResult"
        );
    }
}
