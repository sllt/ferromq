# Embedded Dashboard assets (`dashboard-dist/`)

Production build of the React admin console from
[`sllt/ferromq-dashboard`](https://github.com/sllt/ferromq-dashboard).
`ferromq-http-api` embeds this folder via `rust-embed` and serves it at
`/dashboard/` (and `/`). The path **must stay inside this crate** so
`cargo publish` can recompile from the packaged tarball.

## Source

| Field | Value |
|-------|-------|
| Repository | https://github.com/sllt/ferromq-dashboard |
| Ref | `dev` |
| Commit | `b38456ae71200f74eb776613b49c95c597ad2cdb` |
| UI | React 19 + Vite (`base: './'`) + Hash Router (`#/overview`) |

Do **not** copy `node_modules` here. Only the Vite `dist/` output
(`index.html`, `assets/*`, `favicon.svg`) belongs in this directory.

## Rebuild

From the FerroMQ repo root (Node 20+, pnpm 9+):

```bash
FERROMQ_DASHBOARD_REF=dev ./scripts/sync-dashboard-dist.sh
```

Override source with `FERROMQ_DASHBOARD_REPO` / `FERROMQ_DASHBOARD_REF`
(default ref is `dev`).

Then `cargo build -p ferromq-http-api` so `rust-embed` picks up the new files.

## Development (do not embed Vite)

Day-to-day UI work stays in the dashboard repo (`pnpm dev` on port 5173,
proxies `/api/v1` to the broker). To preview a local production build
without recompiling FerroMQ:

```toml
# ferromq-http-api.toml
dashboard_static_dir = "/path/to/ferromq-dashboard/dist"
```

`dashboard_static_dir` wins over the embedded assets when the directory
exists. Relative paths are resolved against the process cwd.
