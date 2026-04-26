# Upstream provenance

Lua scripts in this directory are vendored verbatim from BullMQ.

- Upstream: https://github.com/taskforcesh/bullmq
- Source path: `src/commands/`
- Pinned commit: see `.gitmodules` and the checked-out SHA of
  `third_party/bullmq` (recorded redundantly here:
  `0866f39123010c9664fe32c1066f5e075faa4e23`, 2026-04-25)
- License: MIT (see `THIRD_PARTY_NOTICES.md` at the repo root)

## Why both submodule and vendored copies?

Go modules don't pull submodule contents on `go get`, so the Lua
files must live inside the module tree for `go:embed` to work for
downstream consumers. The submodule at `third_party/bullmq` is
test/dev/CI tooling only — never required for `go get
github.com/shiroha-a/mkq` to succeed.

## Files vendored

Entry points:

- `addStandardJob-9.lua`
- `addDelayedJob-6.lua`
- `addPrioritizedJob-9.lua`
- `moveToActive-11.lua`
- `moveToFinished-14.lua`
- `extendLock-2.lua`
- `releaseLock-1.lua`
- `retryJob-11.lua`
- `moveToDelayed-12.lua`
- `moveStalledJobsToWait-8.lua`

`includes/` — every script transitively reachable from the entry points
above via `--- @include "..."` directives.

The `-N` suffix on entry-point scripts is the BullMQ convention for the
expected `KEYS` count and is preserved verbatim.

## Re-vendoring

```
git submodule update --init --recursive
cd third_party/bullmq
git fetch
git checkout <new-sha>
cd -
script/sync-lua.sh
# Update the SHA + date noted at the top of this file, then commit
# everything (.gitmodules submodule pointer + sync'd lua + this file).
```

The `lua sync verify` GitHub Actions workflow runs `script/sync-lua.sh`
on every PR and fails if the result diverges from this directory,
catching both "submodule pin advanced but lua not refreshed" and
"vendored lua hand-edited without updating the submodule".

To add a NEW entry-point script (one that's not yet listed above), copy
it manually the first time along with any new transitive includes
referenced by its `--- @include "..."` directives. The sync script only
mirrors files that already exist locally.

Do not hand-edit vendored files. mkq's `@include` preprocessor (in the
parent `internal/lua` package) is responsible for resolving directives at
load time, mirroring BullMQ's `script-loader.ts`.
