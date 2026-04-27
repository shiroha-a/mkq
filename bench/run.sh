#!/usr/bin/env bash
# bench/run.sh — drive both mkq and BullMQ TS through the same workload
# and pretty-print a comparison table to stdout.
#
# Usage:
#   bench/run.sh [smoke|standard|<N>]   # job count: 1k / 10k / custom
#
# Notes:
#   - Requires a running Redis on 127.0.0.1:6379 (override via REDIS=).
#   - Uses /usr/bin/time -v to capture peak RSS + user/system CPU per
#     subprocess (Linux only; on macOS install GNU coreutils' gtime).
#   - Each side uses its own keyPrefix so the same Redis can host both
#     without cross-contamination.
#   - Prints the per-mode JSON line straight from the bench programs
#     plus an OS-side summary parsed from time -v.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}/.."

JOBS="${1:-standard}"
case "$JOBS" in
  smoke) JOBS=1000 ;;
  standard) JOBS=10000 ;;
esac

REDIS="${REDIS:-127.0.0.1:6379}"
CONCURRENCY="${CONCURRENCY:-16}"

if ! command -v /usr/bin/time >/dev/null 2>&1; then
  echo "missing /usr/bin/time -v (install GNU time; on Debian: apt install time)" >&2
  exit 1
fi
TIMEBIN=/usr/bin/time

# resolve Go toolchain the rest of the repo uses (matches go.mod toolchain
# directive). Falls back to system go if GOTOOLCHAIN is unset.
export GOTOOLCHAIN="${GOTOOLCHAIN:-go1.25.0}"

GO_BIN="$(mktemp -d)/mkqbench"
STDERR_LOG=/tmp/mkqbench-stderr.log
: > "$STDERR_LOG" # truncate per-run so累積しない
trap 'rm -rf "$(dirname "$GO_BIN")"' EXIT
echo "==> compiling mkq bench" >&2
go build -o "$GO_BIN" ./bench/go

NODE_BIN=node
if ! command -v "$NODE_BIN" >/dev/null 2>&1; then
  echo "missing node executable" >&2
  exit 1
fi

if [[ ! -d bench/node/node_modules ]]; then
  echo "==> installing BullMQ deps" >&2
  (cd bench/node && npm install --silent)
fi

# run_one mode side-tag bin args...
# Captures stdout (the JSON one-liner) + /usr/bin/time -v stderr.
# Emits a single composite JSON to stdout, prefixed with mode + side.
run_one() {
  local mode="$1"; shift
  local side="$1"; shift
  local bin="$1"; shift

  local timefile
  timefile="$(mktemp)"
  local jsonfile
  jsonfile="$(mktemp)"

  set +e
  "$TIMEBIN" -v -o "$timefile" "$bin" "$@" >"$jsonfile" 2>>"$STDERR_LOG"
  local rc=$?
  set -e
  if [[ $rc -ne 0 ]]; then
    echo "==> FAILED: $side $mode (rc=$rc)" >&2
    cat "$jsonfile" >&2
    cat "$timefile" >&2
    return $rc
  fi

  local rss user sys elapsed
  rss=$(awk '/Maximum resident set size/ { print $NF }' "$timefile")
  user=$(awk '/User time \(seconds\)/ { print $NF }' "$timefile")
  sys=$(awk '/System time \(seconds\)/ { print $NF }' "$timefile")
  elapsed=$(awk -F': ' '/Elapsed \(wall clock\) time/ { print $2 }' "$timefile")

  local json
  json=$(cat "$jsonfile")
  rm -f "$timefile" "$jsonfile"

  printf '%s | rssKB=%s userS=%s sysS=%s wall=%s\n' \
    "$json" "$rss" "$user" "$sys" "$elapsed"
}

# Each mode is run twice: once mkq, once BullMQ TS, with distinct prefixes
# so they don't collide. Producer and consumer use the same prefix per
# side so the consumer reads what the producer wrote.

echo "==> Job count: $JOBS, worker concurrency: $CONCURRENCY"
echo

clear_prefixes() {
  redis-cli -h "${REDIS%:*}" -p "${REDIS##*:}" --scan --pattern "$1:*" \
    | xargs -r redis-cli -h "${REDIS%:*}" -p "${REDIS##*:}" del >/dev/null
}

for prefix in mkqbench bullbench; do
  clear_prefixes "$prefix" || true
done

echo "## produce"
run_one produce mkq        "$GO_BIN" -mode=produce -jobs="$JOBS" -prefix=mkqbench -redis="$REDIS"
run_one produce bullmq-ts  "$NODE_BIN" bench/node/produce.mjs --jobs="$JOBS" --prefix=bullbench --redis="$REDIS"
echo

echo "## consume (uses jobs queued by the produce step above)"
run_one consume mkq        "$GO_BIN" -mode=consume -jobs="$JOBS" -concurrency="$CONCURRENCY" -prefix=mkqbench -redis="$REDIS"
run_one consume bullmq-ts  "$NODE_BIN" bench/node/consume.mjs --jobs="$JOBS" --concurrency="$CONCURRENCY" --prefix=bullbench --redis="$REDIS"
echo

# Latency mode wipes the prefixes again so the per-job timestamps don't
# race against any leftover state.
for prefix in mkqbench bullbench; do
  clear_prefixes "$prefix" || true
done

echo "## latency (Add → Process e2e)"
run_one latency mkq        "$GO_BIN" -mode=latency -jobs="$JOBS" -concurrency="$CONCURRENCY" -prefix=mkqbench -redis="$REDIS"
run_one latency bullmq-ts  "$NODE_BIN" bench/node/latency.mjs --jobs="$JOBS" --concurrency="$CONCURRENCY" --prefix=bullbench --redis="$REDIS"
echo

for prefix in mkqbench bullbench; do
  clear_prefixes "$prefix" || true
done

echo "==> done"
