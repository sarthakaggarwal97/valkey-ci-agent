# Fuzzer walk

Updated 2026-11-10T16:33+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1531 | 990 | #1489 (2025-01-02) | 299 | 218.7 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 63 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.0 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 3 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-10T15:06:05+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42809508889): 20 merge(s) tested, 6 without a code change.

- [valkey-io/valkey#5085](https://github.com/valkey-io/valkey/pull/5085) ([tests](tests/valkey/5085-7ec927fd1/)): Disconnect default user subscribers to revoked channels on ACL LOAD (#5085): 1 of 3 test(s) reached the change, coverage 100%; [check, introduced by #5085](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/115)
- [valkey-io/valkey#1541](https://github.com/valkey-io/valkey/pull/1541) ([tests](tests/valkey/1541-d6bdd9e7d/)): Fix module LatencyAddSample still work when latency-monitor-threshold is 0 (#1541): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1535](https://github.com/valkey-io/valkey/pull/1535): to retry, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#1536](https://github.com/valkey-io/valkey/pull/1536) ([tests](tests/valkey/1536-d99457c09/)): Free the passed in lua context instead of the global (#1536): 0 of 3 test(s) reached the change, coverage 100%; crash, fixed by #1536 (recorded)
- [valkey-io/valkey#1530](https://github.com/valkey-io/valkey/pull/1530) ([tests](tests/valkey/1530-80c35402b/)): Remove legacy SERVER_TEST compiler flag from cmake. (#1530): declined: The change only deletes the obsolete SERVER_TEST=1 compile definition from the unit-test CMakeLists.txt. It is a build-system cleanup affecting only how unit tests are compiled; no server, command, replication, persistence or tool code changes, so the runtime binaries the fuzzer exercises 
- [valkey-io/valkey#1501](https://github.com/valkey-io/valkey/pull/1501) ([tests](tests/valkey/1501-9f4815a22/)): Accelerate hash table iterator with prefetching (#1501): 0 of 4 test(s) reached the change, coverage 74%
- [valkey-io/valkey#1456](https://github.com/valkey-io/valkey/pull/1456) ([tests](tests/valkey/1456-418f1d059/)): Improve Typos configuration (#1456): declined: The change edits only .config/typos.toml (the typos spell-check CI tool's configuration) and a comment spelling fix in .cmake-format.yaml. No source, test, or build-output file changes; the Valkey server and tools behave identically before and after. This is a CI/lint configuration change 
- [valkey-io/valkey#1432](https://github.com/valkey-io/valkey/pull/1432) ([tests](tests/valkey/1432-9e0204941/)): valkey-cli auto-exit from subscribed mode (#1432): 2 of 2 test(s) reached the change, coverage 96%
- [valkey-io/valkey#1427](https://github.com/valkey-io/valkey/pull/1427) ([tests](tests/valkey/1427-ab627d672/)): Replace dict with new hashtable: sorted set datatype (#1427): 0 of 16 test(s) reached the change, coverage 87%
- [valkey-io/valkey#1488](https://github.com/valkey-io/valkey/pull/1488) ([tests](tests/valkey/1488-8af35a171/)): Add build folder to gitignore. (#1488): declined: The change adds a single line (build/) to .gitignore. It touches no source, build script, or runtime configuration, so no server behaviour, command reply, replication, or persistence path can differ between the two builds; there is nothing for a fuzzer scenario to exercise.
- [valkey-io/valkey#1405](https://github.com/valkey-io/valkey/pull/1405) ([tests](tests/valkey/1405-6c09eea2b/)): client struct: lazy init components and optimize struct layout (#1405): 0 of 27 test(s) reached the change, coverage 80%; crash, introduced and since fixed (recorded)
- [valkey-io/valkey#1524](https://github.com/valkey-io/valkey/pull/1524) ([tests](tests/valkey/1524-dc4628d44/)): Add `availability_zone` to the HELLO command history (#1524): declined: The change is documentation-only: it adds a history entry for HELLO in src/commands/hello.json and the generated src/commands.def, bumping the history count in the command table to match the new array length. It alters no command behaviour, reply, config handling or server logic — only the
- [valkey-io/valkey#1487](https://github.com/valkey-io/valkey/pull/1487) ([tests](tests/valkey/1487-3b52186b6/)): Add `availability_zone` to the HELLO response (#1487): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1513](https://github.com/valkey-io/valkey/pull/1513) ([tests](tests/valkey/1513-4ffd3ebde/)): Fix LUA garbage collector (CVE-2024-46981) (#1513): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1514](https://github.com/valkey-io/valkey/pull/1514) ([tests](tests/valkey/1514-7977c55ac/)): Fix Read/Write key pattern selector (CVE-2024-51741) (#1514): 4 of 4 test(s) reached the change, coverage 100%; crash, fixed by #1514 (recorded)
- [valkey-io/valkey#1061](https://github.com/valkey-io/valkey/pull/1061) ([tests](tests/valkey/1061-c0014ef15/)): Check whether to switch to fail when setting the node to pfail in cron (#1061): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1505](https://github.com/valkey-io/valkey/pull/1505) ([tests](tests/valkey/1505-33b824137/)): Explicitly check C_ERR condition to improve readability in clusterSaveConfig (#1505): declined: The change rewrites `if (retval)` as `if (retval == C_ERR)` in clusterSaveConfig's cleanup path. retval starts as C_ERR (-1, truthy) and becomes C_OK (0, falsy) only when every step succeeded, so the old truthiness test and the new explicit comparison unlink the temporary cluster config fi
- [valkey-io/valkey#1503](https://github.com/valkey-io/valkey/pull/1503) ([tests](tests/valkey/1503-b3b4bdcda/)): CMake: fail on warnings (#1503): declined: The change only adds -Werror -Wall to the CMake build and an unreachable 'return NULL' after assert(0) in valkey-cli's jsonStringOutput to silence the resulting warning. jsonStringOutput is only called with OUTPUT_JSON or OUTPUT_QUOTED_JSON, both of which return earlier, so the new stateme
- [valkey-io/valkey#1499](https://github.com/valkey-io/valkey/pull/1499) ([tests](tests/valkey/1499-26a72fa89/)): Use the correct command proc for the LOOKUP_NOTOUCH exception in lookupKey (#1499): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1495](https://github.com/valkey-io/valkey/pull/1495) ([tests](tests/valkey/1495-93b701d8d/)): Update Redis legacy keyword and link in utils/whatisdoing.sh (#1495): declined: The change only edits utils/whatisdoing.sh, a standalone poor-man's-profiler helper script: it updates comments and a documentation URL from Redis to Valkey and changes the ps|grep pattern to match the valkey-server process name. No server code, command behaviour, replication, persistence,

### valkey-io/valkey-bloom

[2026-11-10T16:21:25+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42853591257): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-10T16:21:35+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42853613143): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-10T16:33:45+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42855618720): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 750 of 1159 tested (65%)
- Changed-line coverage: median 100% of 692 merge(s) measured, 55 under 50%
- Recall: 202 parents flagged of 317 fix merges
- Claude: $1076.13 in the last 24 hours, $8444.07 in the last 7 days, $8444.07 in all
- Issues filed: 66
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/100
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/101
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/102
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/103
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/104
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/105
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/106
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/107
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/108
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/109
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/110
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/111
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/112
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/113
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/114
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/115
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

- [valkey-io/valkey#1948](https://github.com/valkey-io/valkey/pull/1948): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#1535](https://github.com/valkey-io/valkey/pull/1535): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
