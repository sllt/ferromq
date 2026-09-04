//! Embedded Dashboard assets via `rust-embed`.
//!
//! Production assets live in crate-local `dashboard-dist/` (Vite build of
//! https://github.com/sllt/ferromq-dashboard with `base: './'` and Hash Router).
//! `dashboard-dist/` is the SINGLE default source of the Dashboard SPA.
//!
//! IMPORTANT: `dashboard-dist/` must live INSIDE this crate. A path pointing
//! outside the crate (e.g. `../../ferromq-dashboard/dist`) breaks `cargo publish`,
//! because the packaged tarball only contains files inside the crate root and
//! the verify step re-compiles under `target/package/ferromq-http-api-<ver>/`.
//!
//! `dashboard_static_dir` still overrides this folder when the path exists
//! (dev / hot-swap of a local Vite `dist/`).

use rust_embed::RustEmbed;
use salvo::compression::{Compression, CompressionLevel};
use salvo::prelude::*;
use salvo::serve_static::{static_embed, StaticDir};

/// All files under `dashboard-dist/` embedded at compile time.
/// `README.md` is documentation only and is not served.
#[derive(RustEmbed)]
#[folder = "dashboard-dist/"]
#[exclude = "README.md"]
pub(crate) struct DashboardAssets;

/// Pragmatic CSP for the admin SPA: same-origin API + Vite bundles, plus the
/// Google Fonts stylesheets referenced by the shipped `index.html`. Avoid a
/// tighter policy that would blank the console.
const DASHBOARD_CSP: &str = "default-src 'self'; \
     script-src 'self'; \
     style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; \
     font-src 'self' data: https://fonts.gstatic.com; \
     img-src 'self' data:; \
     connect-src 'self' https://fonts.googleapis.com https://fonts.gstatic.com; \
     frame-ancestors 'none'; \
     base-uri 'self'; \
     form-action 'self'";

/// Cache-Control for hashed Vite assets vs HTML entry / SPA fallback.
pub(crate) fn cache_control_for(path: &str, content_type: Option<&str>) -> &'static str {
    let ct = content_type.unwrap_or("");
    if ct.contains("text/html") {
        return "no-cache";
    }
    let file = path.trim_end_matches('/').rsplit('/').next().unwrap_or("");
    if file.is_empty() || file.eq_ignore_ascii_case("index.html") || file.ends_with(".html") {
        return "no-cache";
    }
    // Vite content-hashed bundles live under /assets/ (e.g. index-CgGsT4ub.js).
    if path.contains("/assets/") {
        return "public, max-age=31536000, immutable";
    }
    "public, max-age=3600"
}

fn content_type_of(res: &Response) -> Option<&str> {
    res.headers().get("content-type").and_then(|v| v.to_str().ok())
}

/// Security + cache headers for Dashboard static responses.
#[handler]
pub(crate) async fn dashboard_headers(
    req: &mut Request,
    depot: &mut Depot,
    res: &mut Response,
    ctrl: &mut FlowCtrl,
) {
    ctrl.call_next(req, depot, res).await;

    let _ = res.add_header("x-content-type-options", "nosniff", true);
    let _ = res.add_header("x-frame-options", "DENY", true);
    let _ = res.add_header("referrer-policy", "same-origin", true);
    let _ = res.add_header("content-security-policy", DASHBOARD_CSP, true);

    let cache = cache_control_for(req.uri().path(), content_type_of(res));
    let _ = res.add_header("cache-control", cache, true);
}

fn gzip() -> Compression {
    Compression::new().enable_gzip(CompressionLevel::Default).min_length(512)
}

/// Mount the Dashboard SPA at `/dashboard/` and `/`.
///
/// Prefers `dashboard_static_dir` when that filesystem path exists; otherwise
/// serves the rust-embed copy of `dashboard-dist/`. Middleware (gzip, cache,
/// security headers) is applied only to these static routes so `/api/v1` is
/// unchanged.
pub(crate) fn mount_dashboard(mut root: Router, dashboard_static_dir: Option<&str>) -> Router {
    let dashboard_mounted = if let Some(dir) = dashboard_static_dir {
        let path = std::path::Path::new(dir);
        if path.exists() {
            root = root.push(
                Router::with_path("dashboard/{**path}")
                    .hoop(dashboard_headers)
                    .hoop(gzip())
                    .get(StaticDir::new([dir]).defaults("index.html")),
            );
            root = root.push(
                Router::with_path("{**path}")
                    .hoop(dashboard_headers)
                    .hoop(gzip())
                    .get(StaticDir::new([dir]).defaults("index.html")),
            );
            log::info!("Dashboard SPA mounted from filesystem: {dir}, canonical: {:?}", path.canonicalize());
            true
        } else {
            log::warn!(
                "Dashboard static dir configured but not found: {dir}, falling back to embedded assets"
            );
            false
        }
    } else {
        false
    };

    if !dashboard_mounted {
        root = root.push(
            Router::with_path("dashboard/{*path}")
                .hoop(dashboard_headers)
                .hoop(gzip())
                .get(static_embed::<DashboardAssets>().fallback("index.html")),
        );
        root = root.push(
            Router::with_path("{*path}")
                .hoop(dashboard_headers)
                .hoop(gzip())
                .get(static_embed::<DashboardAssets>().fallback("index.html")),
        );
        log::info!("Dashboard SPA mounted from embedded assets (rust-embed, dashboard-dist/)");
    }

    root
}

#[cfg(test)]
mod tests {
    use super::*;
    use salvo::test::{ResponseExt, TestClient};

    #[test]
    fn embedded_dashboard_contains_index_html() {
        let file =
            DashboardAssets::get("index.html").expect("index.html must be embedded from dashboard-dist/");
        let html = std::str::from_utf8(&file.data).expect("index.html is utf-8");
        let lower = html.to_ascii_lowercase();
        assert!(lower.contains("<!doctype html"), "got: {html}");
        assert!(html.contains("id=\"root\"") || html.contains("id='root'"), "got: {html}");
        assert!(html.contains("./assets/"), "Vite base './' should produce relative asset URLs: {html}");
    }

    #[test]
    fn embedded_dashboard_contains_hashed_bundles() {
        let names: Vec<String> = DashboardAssets::iter().map(|n| n.into_owned()).collect();
        assert!(
            names.iter().any(|n| n.starts_with("assets/") && n.ends_with(".js")),
            "missing hashed JS in {names:?}"
        );
        assert!(
            names.iter().any(|n| n.starts_with("assets/") && n.ends_with(".css")),
            "missing hashed CSS in {names:?}"
        );
        assert!(DashboardAssets::get("favicon.svg").is_some());
        assert!(DashboardAssets::get("README.md").is_none(), "README.md must not be embedded");
    }

    #[test]
    fn cache_control_html_is_no_cache() {
        assert_eq!(cache_control_for("/dashboard/", Some("text/html; charset=utf-8")), "no-cache");
        assert_eq!(cache_control_for("/dashboard/index.html", None), "no-cache");
        assert_eq!(
            cache_control_for("/dashboard/missing-route", Some("text/html; charset=utf-8")),
            "no-cache"
        );
    }

    #[test]
    fn cache_control_hashed_assets_are_immutable() {
        assert_eq!(
            cache_control_for("/dashboard/assets/index-CgGsT4ub.js", Some("application/javascript")),
            "public, max-age=31536000, immutable"
        );
        assert_eq!(
            cache_control_for("/assets/index-BepUvtrM.css", Some("text/css")),
            "public, max-age=31536000, immutable"
        );
    }

    fn embedded_svc() -> Service {
        Service::new(mount_dashboard(Router::new(), None))
    }

    #[tokio::test]
    async fn serves_index_at_dashboard_slash() {
        let svc = embedded_svc();
        let mut resp = TestClient::get("http://127.0.0.1:0/dashboard/").send(&svc).await;
        assert_eq!(resp.status_code, Some(StatusCode::OK));
        assert_eq!(
            resp.headers().get("x-content-type-options").and_then(|v| v.to_str().ok()),
            Some("nosniff")
        );
        assert_eq!(resp.headers().get("x-frame-options").and_then(|v| v.to_str().ok()), Some("DENY"));
        let csp = resp.headers().get("content-security-policy").and_then(|v| v.to_str().ok()).unwrap_or("");
        assert!(csp.contains("frame-ancestors 'none'"), "{csp}");
        let cc = resp.headers().get("cache-control").and_then(|v| v.to_str().ok()).unwrap_or("");
        assert!(cc.contains("no-cache"), "html cache-control={cc}");
        let body = resp.take_string().await.unwrap();
        assert!(body.to_ascii_lowercase().contains("<!doctype html"));
        assert!(body.contains("root"));
    }

    #[tokio::test]
    async fn hashed_js_is_immutable_and_javascript() {
        let name = DashboardAssets::iter()
            .find(|n| n.starts_with("assets/") && n.ends_with(".js"))
            .expect("hashed js");
        let svc = embedded_svc();
        let url = format!("http://127.0.0.1:0/dashboard/{name}");
        let resp = TestClient::get(url).send(&svc).await;
        assert_eq!(resp.status_code, Some(StatusCode::OK));
        let cc = resp.headers().get("cache-control").and_then(|v| v.to_str().ok()).unwrap_or("");
        assert!(cc.contains("immutable"), "asset cache-control={cc}");
        assert!(cc.contains("31536000"), "{cc}");
        let ct = resp.headers().get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("");
        assert!(
            ct.contains("javascript") || ct.contains("ecmascript") || ct.contains("text/"),
            "content-type={ct}"
        );
    }

    #[tokio::test]
    async fn spa_fallback_serves_index_html() {
        let svc = embedded_svc();
        let mut resp = TestClient::get("http://127.0.0.1:0/dashboard/not-a-real-file").send(&svc).await;
        assert_eq!(resp.status_code, Some(StatusCode::OK));
        let body = resp.take_string().await.unwrap();
        assert!(body.contains("root"), "{body}");
        let cc = resp.headers().get("cache-control").and_then(|v| v.to_str().ok()).unwrap_or("");
        assert!(cc.contains("no-cache"), "fallback html cache-control={cc}");
    }

    #[tokio::test]
    async fn gzip_when_client_accepts_it() {
        let svc = embedded_svc();
        let resp = TestClient::get("http://127.0.0.1:0/dashboard/")
            .add_header("accept-encoding", "gzip", true)
            .send(&svc)
            .await;
        assert_eq!(resp.status_code, Some(StatusCode::OK));
        let enc = resp.headers().get("content-encoding").and_then(|v| v.to_str().ok());
        assert_eq!(enc, Some("gzip"), "expected gzip, headers={:?}", resp.headers());
    }

    #[tokio::test]
    async fn filesystem_override_wins_over_embed() {
        let dir = std::env::temp_dir().join(format!("ferromq-dash-override-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("index.html"), "<!doctype html><html><body>OVERRIDE-P7</body></html>")
            .unwrap();
        let svc = Service::new(mount_dashboard(Router::new(), Some(dir.to_str().unwrap())));
        let mut resp = TestClient::get("http://127.0.0.1:0/dashboard/").send(&svc).await;
        assert_eq!(resp.status_code, Some(StatusCode::OK));
        let body = resp.take_string().await.unwrap();
        assert!(body.contains("OVERRIDE-P7"), "{body}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn missing_static_dir_falls_back_to_embed() {
        let svc = Service::new(mount_dashboard(Router::new(), Some("/no/such/ferromq-dashboard-dist")));
        let mut resp = TestClient::get("http://127.0.0.1:0/dashboard/").send(&svc).await;
        assert_eq!(resp.status_code, Some(StatusCode::OK));
        let body = resp.take_string().await.unwrap();
        assert!(body.contains("root"), "should serve embedded index.html, got {body}");
    }
}
