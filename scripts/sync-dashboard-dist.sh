#!/usr/bin/env bash
# Rebuild ferromq-plugins/ferromq-http-api/dashboard-dist from ferromq-dashboard.
#
# Requires Node 20+ and pnpm 9+. Does not copy node_modules.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$ROOT/ferromq-plugins/ferromq-http-api/dashboard-dist"
REPO="${FERROMQ_DASHBOARD_REPO:-https://github.com/sllt/ferromq-dashboard}"
PINNED_REF=""
if [ -f "$DEST/COMMIT" ]; then
  PINNED_REF="$(tr -d '[:space:]' < "$DEST/COMMIT")"
fi
# No override means reproduce the currently embedded commit. To refresh from
# the moving development branch, pass FERROMQ_DASHBOARD_REF=dev explicitly.
REF="${FERROMQ_DASHBOARD_REF:-${PINNED_REF:-dev}}"
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

echo "Fetching $REPO @ $REF ..."
git init -q "$WORKDIR"
git -C "$WORKDIR" remote add origin "$REPO"
git -C "$WORKDIR" fetch --depth 1 origin "$REF"
git -C "$WORKDIR" checkout -q --detach FETCH_HEAD
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
cp "$WORKDIR/LICENSE" "$DEST/THIRD_PARTY_NOTICES.txt"

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
# Reproduce the embedded commit:
./scripts/sync-dashboard-dist.sh

# Explicitly refresh from the moving development branch:
FERROMQ_DASHBOARD_REF=dev ./scripts/sync-dashboard-dist.sh
\`\`\`

Override source with \`FERROMQ_DASHBOARD_REPO\` / \`FERROMQ_DASHBOARD_REF\`
(default ref is \`dev\`).

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

printf '%s\n' "$SHA" > "$DEST/COMMIT"
printf 'repo=%s\nref=%s\ncommit=%s\n' "$REPO" "$REF" "$SHA" > "$DEST/SOURCE"

echo "Synced $DEST from $REPO@$SHA"
find "$DEST" -type f | wc -l | awk '{print $1 " files"}'
