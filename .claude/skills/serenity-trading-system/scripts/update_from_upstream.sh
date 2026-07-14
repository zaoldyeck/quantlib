#!/usr/bin/env bash
# Sync vendored Serenity reference data from upstream
# https://github.com/yan-labs/serenity-aleabitoreddit (regenerated from the
# live @aleabitoreddit feed roughly every 30 minutes).
#
# Refreshes ONLY the vendored files:
#   references/{methodology,theses,track-record,articles,maintenance}.md
#   references/ticker-stats.txt
#   analysis/*.md
# Never touches the local playbooks (valuation-playbook.md,
# entry-exit-sizing.md, replication-guardrails.md, info-sources.md) or SKILL.md.
#
# Usage:
#   ./update_from_upstream.sh                 # refresh distilled references
#   ./update_from_upstream.sh --with-archive  # also fetch the full 7.8MB tweet
#                                             # archive into research/external/
#                                             # (self-gitignored) for event studies
set -euo pipefail

SKILL_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PROJECT_ROOT="$(cd "$SKILL_DIR/../../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

git clone --quiet --depth 1 https://github.com/yan-labs/serenity-aleabitoreddit.git "$TMP/repo"

for f in methodology theses track-record articles maintenance; do
  cp "$TMP/repo/serenity-aleabitoreddit/references/$f.md" "$SKILL_DIR/references/$f.md"
done
cp "$TMP/repo/serenity-aleabitoreddit/analysis/"*.md "$SKILL_DIR/analysis/"
cp "$TMP/repo/data/ticker_stats.txt" "$SKILL_DIR/references/ticker-stats.txt"

echo "Vendored references refreshed from upstream ($(date '+%Y-%m-%d %H:%M'))."

if [[ "${1:-}" == "--with-archive" ]]; then
  ARCHIVE_DIR="$PROJECT_ROOT/research/external/serenity-archive"
  mkdir -p "$ARCHIVE_DIR"
  echo '*' > "$PROJECT_ROOT/research/external/.gitignore"
  cp "$TMP/repo/data/aleabitoreddit_tweets.json" "$ARCHIVE_DIR/"
  cp "$TMP/repo/data/aleabitoreddit_tweets.csv" "$ARCHIVE_DIR/" 2>/dev/null || true
  echo "Full tweet archive copied to $ARCHIVE_DIR (gitignored)."
fi
