# Fuzzer walk

Updated 2026-11-08T22:11+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 997 | 1510 | #2658 (2025-09-28) | 212 | 142.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 655 | 14 | f8106e72e984 (2024-11-27) | 150 | 93.6 |

## Last runs

### valkey-io/valkey

[2026-11-08T19:07:20+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42499012726): 20 merge(s) tested, 11 without a code change.

- [valkey-io/valkey#2498](https://github.com/valkey-io/valkey/pull/2498) ([tests](tests/valkey/2498-af45feea1/)): Remove the outupdated unknown key/value pairs comment in CLUSTER SYNCSLOTS ESTABLISH (#2498): declined: The change only edits a code comment in clusterCommandSyncSlotsEstablish (src/cluster_migrateslots.c), removing an outdated note about skipping unknown key/value pairs. No executable code, replies, parsing, or configuration are modified, so there is no behaviour to exercise or regress.
- [valkey-io/valkey#2734](https://github.com/valkey-io/valkey/pull/2734) ([tests](tests/valkey/2734-54da8344c/)): Fix double MOVED reply on unblock at failover (#2734): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2721](https://github.com/valkey-io/valkey/pull/2721) ([tests](tests/valkey/2721-28e5dcce2/)): Fix crash that occurs sometimes when aborting a slot migration while child snapshot is active (#2721): 3 of 7 test(s) reached the change, coverage 80%
- [valkey-io/valkey#2729](https://github.com/valkey-io/valkey/pull/2729) ([tests](tests/valkey/2729-181ca483b/)): Bump CLUSTER SHARDS command update version to 9.1.0 (#2729): 2 of 2 test(s) reached the change
- [valkey-io/valkey#747](https://github.com/valkey-io/valkey/pull/747) ([tests](tests/valkey/747-0d500c2cc/)): Update the misleading zdiff() comment on empty first key handling. (#747): declined: The change adds only a code comment above zdiff() in src/t_zset.c explaining the existing empty-first-key no-op behaviour; no executable code, reply, config, or protocol behaviour changes, so there is nothing a scenario could exercise or compare.
- [valkey-io/valkey#2716](https://github.com/valkey-io/valkey/pull/2716) ([tests](tests/valkey/2716-8182f4a0b/)): HSETEX with FXX should not create an object if it does not exist (#2716): 4 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2710](https://github.com/valkey-io/valkey/pull/2710) ([tests](tests/valkey/2710-f7c95277d/)): Add invalid RDB signature to log statement (#2710): declined: The change only appends the first 9 bytes of the rejected RDB header to an existing LL_WARNING log line in rdbLoadRioWithLoadingCtx (src/rdb.c:3111). The buffer is NUL-terminated at buf[9] and the format is bounded with %.9s, so there is no memory-safety risk; the function still returns C_
- [valkey-io/valkey#2362](https://github.com/valkey-io/valkey/pull/2362) ([tests](tests/valkey/2362-155b0bb82/)): Fix memory leak with CLIENT LIST/KILL duplicate filters (#2362): 0 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2568](https://github.com/valkey-io/valkey/pull/2568) ([tests](tests/valkey/2568-5bbbc6bd9/)): Add shard id field to CLUSTER SHARDS response (#2568): 2 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1673](https://github.com/valkey-io/valkey/pull/1673) ([tests](tests/valkey/1673-3390b1e60/)): Allow TCL 9.0 for tests (#1673): declined: The change is purely to the project's own test infrastructure and CI: runtest launcher scripts now accept Tcl 9.0, 'package require Tcl 8.5' is removed from test-support TCL files, the test harness switches fconfigure from -encoding binary to -translation binary, and a Fedora-specific CI a
- [valkey-io/valkey#2688](https://github.com/valkey-io/valkey/pull/2688) ([tests](tests/valkey/2688-a4317e911/)): Introduce SYNCSLOTS CAPA for forwards compatibility (#2688): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2635](https://github.com/valkey-io/valkey/pull/2635) ([tests](tests/valkey/2635-383ffe487/)): Prevent exposure of importing keys on replicas during atomic slot migration (#2635): 9 of 11 test(s) reached the change, coverage 81%
- [valkey-io/valkey#2679](https://github.com/valkey-io/valkey/pull/2679) ([tests](tests/valkey/2679-064674945/)): Fix format issues with CVE fix (#2679): declined: The change is purely cosmetic: it reformats whitespace and switch-case indentation in luaSetTableProtectionForBasicTypes in src/lua/script_lua.c with byte-for-byte identical semantics (same pushed values, same loop, same calls), and adds an external:skip tag to one TCL test. No server beha
- [valkey-io/valkey@6dd003e88fea](https://github.com/valkey-io/valkey/commit/6dd003e88feace83e55491f32376f6927896e31e) ([tests](tests/valkey/6dd003e88/)): Merge commit from fork: 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2639](https://github.com/valkey-io/valkey/pull/2639) ([tests](tests/valkey/2639-d5bb986fd/)): Add slot migration client flags and module context flags (#2639): 6 of 6 test(s) reached the change, coverage 84%; crash, introduced and since fixed (recorded)
- [valkey-io/valkey#2656](https://github.com/valkey-io/valkey/pull/2656) ([tests](tests/valkey/2656-307397904/)): Defrag if slab 1/8 full to fix defrag didn't stop issue (#2656): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2646](https://github.com/valkey-io/valkey/pull/2646) ([tests](tests/valkey/2646-a9a65abc8/)): Implement a lolwut for version 9 (#2646): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2661](https://github.com/valkey-io/valkey/pull/2661) ([tests](tests/valkey/2661-f39a80971/)): Fix module key memory usage accounting (#2661): declined: no proposed test validated
- [valkey-io/valkey#2636](https://github.com/valkey-io/valkey/pull/2636) ([tests](tests/valkey/2636-e99380f75/)): Fix atomic slot migration snapshot never proceeding with hz 1 (#2636): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2329](https://github.com/valkey-io/valkey/pull/2329) ([tests](tests/valkey/2329-ae58b3d5b/)): Redirect blocked clients after failover (#2329): 6 of 8 test(s) reached the change, coverage 89%

### valkey-io/valkey-bloom

[2026-11-08T19:05:06+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42513162238): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-08T22:11:32+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42528933793): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-08T21:37:16+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42520396419): 14 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-search#68](https://github.com/valkey-io/valkey-search/pull/68): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#77](https://github.com/valkey-io/valkey-search/pull/77): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#76](https://github.com/valkey-io/valkey-search/pull/76): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#73](https://github.com/valkey-io/valkey-search/pull/73): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#60](https://github.com/valkey-io/valkey-search/pull/60): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#72](https://github.com/valkey-io/valkey-search/pull/72): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#71](https://github.com/valkey-io/valkey-search/pull/71): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#70](https://github.com/valkey-io/valkey-search/pull/70): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): Root directory: /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge Checking if configure is required...[32;1mno[0m Checking for cma
- [valkey-io/valkey-search#49](https://github.com/valkey-io/valkey-search/pull/49): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge/.build-release/.deps/install/lib/libgrpc.a(syste
- [valkey-io/valkey-search@1d8e30e25ae6](https://github.com/valkey-io/valkey-search/commit/1d8e30e25ae640522f809f839ed2a81fb191e5c1): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey-search#65](https://github.com/valkey-io/valkey-search/pull/65): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey-search#64](https://github.com/valkey-io/valkey-search/pull/64): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey-search#63](https://github.com/valkey-io/valkey-search/pull/63): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey-search#57](https://github.com/valkey-io/valkey-search/pull/57): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey

## Totals

- Merges whose tests reached the change: 610 of 887 tested (69%)
- Changed-line coverage: median 100% of 454 merge(s) measured, 36 under 50%
- Recall: 167 parents flagged of 247 fix merges
- Claude: $1207.57 in the last 24 hours, $6431.22 in the last 7 days, $6431.22 in all
- Issues filed: 56
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/100
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/101
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/102
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/103
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/104
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/105
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/40
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/41
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/42
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/43
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/44
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/45
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/47
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/48
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/49
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/50
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/53
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/54
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/55
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/56
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/57
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/58
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/59
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/60
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/61
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/62
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/63
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/64
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/65
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/66
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/67
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/68
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/70
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/72
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/73
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/75
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/76
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/78
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/79
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/80
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/81
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/82
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/83
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/84
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/85
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/86
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/87
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/89
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/90
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/91
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/92
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/93
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/94
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/95
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/96
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/98

## Retries

- [valkey-io/valkey-search#68](https://github.com/valkey-io/valkey-search/pull/68): attempt 1 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#77](https://github.com/valkey-io/valkey-search/pull/77): attempt 1 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#76](https://github.com/valkey-io/valkey-search/pull/76): attempt 1 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#73](https://github.com/valkey-io/valkey-search/pull/73): attempt 1 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#60](https://github.com/valkey-io/valkey-search/pull/60): attempt 1 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#72](https://github.com/valkey-io/valkey-search/pull/72): attempt 1 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#71](https://github.com/valkey-io/valkey-search/pull/71): attempt 1 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#70](https://github.com/valkey-io/valkey-search/pull/70): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): Root directory: /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge Checking if configure is required...[32;1mno[0m Checking for cma
- [valkey-io/valkey-search#49](https://github.com/valkey-io/valkey-search/pull/49): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge/.build-release/.deps/install/lib/libgrpc.a(syste
- [valkey-io/valkey-search@1d8e30e25ae6](https://github.com/valkey-io/valkey-search/commit/1d8e30e25ae640522f809f839ed2a81fb191e5c1): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey-search#65](https://github.com/valkey-io/valkey-search/pull/65): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey-search#64](https://github.com/valkey-io/valkey-search/pull/64): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey-search#63](https://github.com/valkey-io/valkey-search/pull/63): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey-search#57](https://github.com/valkey-io/valkey-search/pull/57): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
