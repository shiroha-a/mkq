# mkq <-> BullMQ TS interop tests

Cross-language smoke tests that exercise both directions of mkq <-> BullMQ
wire compatibility against a real Redis. These are not a substitute for
the unit / integration tests in the rest of the repo; they specifically
pin "mkq's vendored Lua + Go logic produces the same Redis state that
BullMQ TS expects to consume, and vice versa".

## Scenarios

- **mkq Add -> BullMQ TS Process**: mkq enqueues a job; the BullMQ TS
  worker (running in a node subprocess) handles it and writes
  `returnvalue`. The Go test reads the resulting HASH and checks the
  worker actually ran on the BullMQ side.
- **BullMQ TS Add -> mkq Process**: the node subprocess enqueues via
  `bullmq.Queue.add`; the mkq `Process` worker handles it. Both the
  Go-side handler invocation and the Redis-side completed state get
  asserted.
- **bull-board reads mkq queue state**: an Express server with the
  BullMQ adapter mounted at `/admin` is started after mkq writes
  jobs in completed/delayed states; the test hits
  `/admin/api/queues` and asserts bull-board lists our queue with
  the expected counts. If mkq's wire shape were off, the adapter
  would either error or report wrong numbers.

## Running locally

```sh
# one-time, ~3s
cd tests/interop/node && npm install && cd -

# every run
go test -tags=interop ./tests/interop/...
```

The default Redis address is `127.0.0.1:6379`. Override via
`MKQ_TEST_REDIS_ADDR`.

The `interop` build tag keeps these tests out of the default
`go test ./...` run since they need node + npm + a running Redis.

## Why npm-published bullmq instead of the submodule?

`tests/interop/node/package.json` pins `bullmq@5.76.2` — the exact
version checked into `third_party/bullmq` at the current submodule
SHA. Using the npm-published package skips the build step that would
otherwise be required to link the submodule's TypeScript into a node
runtime, and the Lua-versus-implementation alignment is preserved
because both came from the same upstream tag.

When the submodule pin moves, bump `bullmq` in
`tests/interop/node/package.json` to match.

## CI

Lives in `.github/workflows/interop.yml`. Runs on every push and PR
against `develop` / `main`. Has its own job (separate from the main
`ci.yml` test suite) so a wire-compat regression is visible
independently of unit-test signal.
