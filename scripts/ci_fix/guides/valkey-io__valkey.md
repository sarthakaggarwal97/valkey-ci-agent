# Fixing a failing Valkey test

Distilled from Valkey's own guidance (`tests/README.md`,
`.github/instructions/integration-tests.instructions.md`, `AGENTS.md`,
`DEVELOPMENT_GUIDE.md`) and from how maintainers have deflaked tests.
Read the files that exist in your checkout; they win over this summary.

## First decide what failed

- Product bug: the server misbehaved. Fix `src/`, not the test. Never make a
  test pass by weakening what it checks.
- Test bug: the test assumes a timing, an ordering, or a state the server does
  not guarantee. Fix the test.
- Harness or environment: a port clash, an out-of-disk runner, a missing tool.
  Refuse; that is not a code fix.

A test that fails once in many runs is still a bug. Find the race in the log
(`--dump-logs` prints each server's log after a failure) before editing.

## How Valkey tests wait

Never wait with a fixed `after N` and then assert. Poll for the condition:

```tcl
wait_for_condition 50 100 {
    [s master_link_status] eq {up}
} else {
    fail "replica did not connect"
}
```

Helpers to use instead of hand-written loops (`tests/support/util.tcl`,
`tests/support/test.tcl`, `tests/test_helper.tcl`,
`tests/support/cluster_util.tcl`):

- replication: `wait_for_sync`, `wait_replica_online`, `wait_for_ofs_sync`,
  `wait_replica_acked_ofs`, `verify_replica_online`
- server state: `wait_done_loading`, `wait_lazyfree_done`,
  `wait_for_blocked_client`, `wait_for_blocked_clients_count`,
  `wait_for_watched_clients_count`
- logs: `wait_for_log_messages`, `verify_log_message`, `count_log_message`
- processes: `pause_process`, `resume_process`, `wait_process_paused`
- cluster: `wait_for_cluster_propagation`, `wait_for_cluster_state`,
  `wait_for_cluster_size`
- replication stream: `attach_to_replication_stream`,
  `assert_replication_stream`, `close_replication_stream`
- `run_solo` for a test that cannot share the machine with others

Helpers and runtest options differ between branches: Valkey 7.2 has no
`wait_for_watched_clients_count`, `wait_replica_acked_ofs`,
`wait_process_paused`, or `--fastfail`. On a release branch, use only what
that branch's `tests/` defines.

Valgrind and sanitizer builds are many times slower. Scale retries rather
than hard-coding them: `[expr {$::valgrind ? 12000 : 2400}]`.

## Common deflake patterns that maintainers accepted

- Replace `after N` + `assert` with `wait_for_condition` on the real signal.
- Wrap a one-shot `assert` or `count_log_message` that races the server in
  `wait_for_condition`.
- Give a slow path more retries or a larger delay, especially under valgrind.
- Remove the race with configuration: e.g. `repl-timeout`,
  `cluster-replica-validity-factor 0`, client-output-buffer limits, or a
  `debug` knob that makes the order deterministic.
- Assert what the server guarantees, not an incidental exact value (an exact
  slowlog length, an exact key count after eviction).
- Drain deferred clients' replies, or use `CLIENT REPLY OFF`, so a reply
  cannot arrive late.
- Skip a test only where it cannot work, with a tag: `valgrind:skip`,
  `tls:skip`, `external:skip`, `cluster:skip`, `needs:debug`, `needs:repl`,
  `needs:save`. A tag is not a fix for a race. `large-memory`, `network`,
  `needs:other-server` and `compatible-redis` select whole files and are only
  allowed at a file's top level; adding one to a test is an error.

## Scope

- Keep the change minimal and easy to backport: release branches take the
  same change.
- Do not reorder or rename tests; other tests in a file may depend on state
  that earlier ones leave behind.
- New cluster tests belong in `tests/unit/cluster/`, not the legacy
  `tests/cluster/` framework.
- Do not change `.github/`.

## Running a test

```sh
make -j"$(nproc)"
./runtest --single unit/type/incr --only 'exact test name' --loops 20 --fastfail --dump-logs
```

`--only` is an exact name match (a leading `/` makes it a regexp). With
several clients (the default) loop copies run in parallel, and a `run_solo`
block runs inline, so loop a file with `run_solo` with `--clients 1`. Add `--accurate`
for what scheduled CI runs, `--valgrind --clients 1` after `make valgrind`,
`--tls` after `make BUILD_TLS=yes && ./utils/gen-test-certs.sh`.

A C unit test (`src/unit/test_*.cpp`) is a Google Test, not a Tcl test:

```sh
make -j"$(nproc)" all-with-unit-tests
./src/unit/valkey-unit-gtests --gtest_filter='Suite.Test' --gtest_repeat=20
```
