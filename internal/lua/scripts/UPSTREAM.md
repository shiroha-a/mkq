# Upstream provenance

Lua scripts in this directory are vendored verbatim from BullMQ.

- Upstream: https://github.com/taskforcesh/bullmq
- Source path: `src/commands/`
- Pinned commit: `0866f39123010c9664fe32c1066f5e075faa4e23` (2026-04-25)
- License: MIT (see `THIRD_PARTY_NOTICES.md` at the repo root)

## Files vendored

Entry points:

- `addStandardJob-9.lua`
- `addDelayedJob-6.lua`
- `addPrioritizedJob-9.lua`
- `moveToActive-11.lua`
- `moveToFinished-14.lua`
- `extendLock-2.lua`
- `releaseLock-1.lua`

`includes/` — every script transitively reachable from the entry points
above via `--- @include "..."` directives.

The `-N` suffix on entry-point scripts is the BullMQ convention for the
expected `KEYS` count and is preserved verbatim.

## Re-vendoring

Update the pinned commit above and re-run the fetch script (see the PR that
introduced this directory). After re-vendoring, run the integration tests to
detect any drift in BullMQ's wire format.

Do not hand-edit vendored files. mkq's `@include` preprocessor (in the
parent `internal/lua` package) is responsible for resolving directives at
load time, mirroring BullMQ's `script-loader.ts`.
