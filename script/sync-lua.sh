#!/usr/bin/env bash
#
# Sync vendored BullMQ Lua scripts from the third_party/bullmq submodule
# into internal/lua/scripts/. Idempotent.
#
# Usage:
#   git submodule update --init --recursive
#   script/sync-lua.sh
#
# To update the pinned BullMQ version:
#   cd third_party/bullmq && git fetch && git checkout <new-sha> && cd -
#   script/sync-lua.sh
#   # then update internal/lua/scripts/UPSTREAM.md and commit everything.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$REPO_ROOT/third_party/bullmq/src/commands"
DST="$REPO_ROOT/internal/lua/scripts"

if [ ! -d "$SRC" ]; then
    echo "ERROR: submodule not initialized." >&2
    echo "Run: git submodule update --init --recursive" >&2
    exit 1
fi

# Sync only the files we already vendor. New entry points must be
# copied in manually the first time (along with their transitive
# includes resolved by internal/lua/preprocess.go).
shopt -s nullglob
fail=0
synced=0

for vendored in "$DST"/*.lua "$DST"/includes/*.lua; do
    rel="${vendored#$DST/}"
    src="$SRC/$rel"
    if [ ! -f "$src" ]; then
        echo "ERROR: $rel exists in vendored copy but not in submodule." >&2
        echo "  Either remove it from internal/lua/scripts/ or update UPSTREAM.md pin." >&2
        fail=1
        continue
    fi
    cp "$src" "$vendored"
    synced=$((synced + 1))
done

if [ "$fail" -ne 0 ]; then
    exit 1
fi

echo "Synced $synced lua file(s) from $SRC -> $DST"
