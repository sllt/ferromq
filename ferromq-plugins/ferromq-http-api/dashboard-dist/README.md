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
| Branch | `cursor/ferromq-p6-diagnostics-cluster-2bc6` |
| Commit | `291588afd211cd8a79e38f920811b4eea7ed6baa` |
| UI | React 19 + Vite (`base: './'`) + Hash Router (`#/overview`) |

Do **not** copy `node_modules` here. Only the Vite `dist/` output
(`index.html`, `assets/*`, `favicon.svg`) belongs in this directory.

## Rebuild

From the FerroMQ repo root (Node 20+, pnpm 9+):

```bash
./scripts/sync-dashboard-dist.sh
```

Or by hand:

```bash
git clone --depth 1 --branch cursor/ferromq-p6-diagnostics-cluster-2bc6 \
  https://github.com/sllt/ferromq-dashboard.git /tmp/ferromq-dashboard
cd /tmp/ferromq-dashboard
pnpm install && pnpm build
rsync -a --delete dist/ /path/to/ferromq/ferromq-plugins/ferromq-http-api/dashboard-dist/
# keep this README.md (rsync --delete would remove it; copy it back or exclude it)
```

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
