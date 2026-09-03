#!/usr/bin/env bash
# Rebuild ferromq-plugins/ferromq-http-api/dashboard-dist from ferromq-dashboard.
#
# Requires Node 20+ and pnpm 9+. Does not copy node_modules.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$ROOT/ferromq-plugins/ferromq-http-api/dashboard-dist"
REPO="${FERROMQ_DASHBOARD_REPO:-https://github.com/sllt/ferromq-dashboard}"
REF="${FERROMQ_DASHBOARD_REF:-cursor/ferromq-p7-release-quality-586c}"
WORKDIR="${TMPDIR:-/tmp}/ferromq-dashboard-sync-$$"

if ! command -v pnpm >/dev/null 2>&1; then
  echo "error: pnpm is required (https://pnpm.io/installation)" >&2
  exit 1
fi

node_major="$(node -p "process.versions.node.split('.')[0]" 2>/dev/null || echo 0)"
if [ "$node_major" -lt 20 ]; then
  echo "error: Node 20+ is required (found $(node --version 2>/dev/null || echo none))" >&2
  exit 1
fi

cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

echo "Cloning $REPO @ $REF ..."
git clone --depth 1 --branch "$REF" "$REPO" "$WORKDIR"
SHA="$(git -C "$WORKDIR" rev-parse HEAD)"
echo "Source commit: $SHA"

(
  cd "$WORKDIR"
  pnpm install --frozen-lockfile
  pnpm build
)

mkdir -p "$DEST"
# Replace built files but keep this README regenerated below.
find "$DEST" -mindepth 1 -maxdepth 1 ! -name README.md -exec rm -rf {} +
cp -a "$WORKDIR/dist/." "$DEST/"

cat > "$DEST/README.md" <<EOF
# Embedded Dashboard assets (\`dashboard-dist/\`)

Production build of the React admin console from
[\`sllt/ferromq-dashboard\`](https://github.com/sllt/ferromq-dashboard).
\`ferromq-http-api\` embeds this folder via \`rust-embed\` and serves it at
\`/dashboard/\` (and \`/\`). The path **must stay inside this crate** so
\`cargo publish\` can recompile from the packaged tarball.

## Source

| Field | Value |
|-------|-------|
| Repository | $REPO |
| Ref | \`$REF\` |
| Commit | \`$SHA\` |
| UI | React 19 + Vite (\`base: './'\`) + Hash Router (\`#/overview\`) |

Do **not** copy \`node_modules\` here. Only the Vite \`dist/\` output
(\`index.html\`, \`assets/*\`, \`favicon.svg\`) belongs in this directory.

## Rebuild

From the FerroMQ repo root (Node 20+, pnpm 9+):

\`\`\`bash
./scripts/sync-dashboard-dist.sh
\`\`\`

Override source with \`FERROMQ_DASHBOARD_REPO\` / \`FERROMQ_DASHBOARD_REF\`.

Then \`cargo build -p ferromq-http-api\` so \`rust-embed\` picks up the new files.

## Development (do not embed Vite)

Day-to-day UI work stays in the dashboard repo (\`pnpm dev\` on port 5173,
proxies \`/api/v1\` to the broker). To preview a local production build
without recompiling FerroMQ:

\`\`\`toml
# ferromq-http-api.toml
dashboard_static_dir = "/path/to/ferromq-dashboard/dist"
\`\`\`

\`dashboard_static_dir\` wins over the embedded assets when the directory
exists. Relative paths are resolved against the process cwd.
EOF

echo "Synced $DEST from $REPO@$SHA"
find "$DEST" -type f | wc -l | awk '{print $1 " files"}'
