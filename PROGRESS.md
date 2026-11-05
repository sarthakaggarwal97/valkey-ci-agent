# Fuzzer walk

Updated 2026-11-05T18:37+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 157 | 2334 | #4890 (2026-10-16) | 157 | 22.4 |
| valkey-io/valkey-bloom | 108 | 1 | ce516d0ef6e3 (2024-04-26) | 108 | 15.4 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 62 | 8.9 |
| valkey-io/valkey-search | 100 | 568 | #1084 (2026-09-06) | 100 | 14.3 |

## Last runs

### valkey-io/valkey

[2026-11-05T16:06:33+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42045411852): 20 merge(s) tested, 4 without a code change.

- [valkey-io/valkey#5173](https://github.com/valkey-io/valkey/pull/5173): to retry, the job left no result
- [valkey-io/valkey#5030](https://github.com/valkey-io/valkey/pull/5030) ([tests](tests/valkey/5030-b4c125082/)): Fix cluster Pub/Sub length truncation (#5030): declined: The only observable change is a new "Publish message is too large for the cluster bus" error returned by PUBLISH/SPUBLISH in cluster mode when a channel+message payload reaches the ~UINT32_MAX (2^32-byte, ~4 GB) cluster-bus limit; below that boundary the size_t refactor is byte-identical t
- [valkey-io/valkey#2250](https://github.com/valkey-io/valkey/pull/2250) ([tests](tests/valkey/2250-c1ed73bfc/)): Fix typos in comments and rename VALKEY_SANITAIZER_* CMake variables (#2250): declined: The change only fixes typos in comments (acl.c, db.c, replication.c, valkey-cli.c, zmalloc.c, test .tcl files) and renames the misspelled VALKEY_SANITAIZER_* CMake variables to VALKEY_SANITIZER_*, updated consistently at both the definition (ValkeySetup.cmake) and the use (src/CMakeLists.t
- [valkey-io/valkey#2238](https://github.com/valkey-io/valkey/pull/2238) ([tests](tests/valkey/2238-4a698691c/)): spelling: ; otherwise, (#2238): declined: The change (PR #2238, 'spelling: ; otherwise,') edits only comments and documentation text: C source comments in acl.c, bitops.c, cluster*.c, module.c, networking.c, replication.c and others, Tcl test-suite comments, and commented lines in valkey.conf. No code path, error message, reply, c
- [valkey-io/valkey#4945](https://github.com/valkey-io/valkey/pull/4945) ([tests](tests/valkey/4945-45ec67c30/)): Fix HSETEX and HGETEX when the key is named "fields" (#4945): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3974](https://github.com/valkey-io/valkey/pull/3974) ([tests](tests/valkey/3974-b93600171/)): CLI hints: Fix unbalanced ']' in bitops (BITFIELD_RO) command (#3974): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4690](https://github.com/valkey-io/valkey/pull/4690) ([tests](tests/valkey/4690-1074b3cad/)): PERF: remove redundant checks in HINCRBYFLOAT command (#4690): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4915](https://github.com/valkey-io/valkey/pull/4915) ([tests](tests/valkey/4915-16d3c5d24/)): Fix Valgrind fork-child IO thread buffer report (#4915): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4354](https://github.com/valkey-io/valkey/pull/4354) ([tests](tests/valkey/4354-bc5aae247/)): Cache earliest timer for O(1) usUntilEarliestTimer lookup (#4354): 0 of 6 test(s) reached the change, coverage 81%; crash, introduced and since fixed (recorded); crash, fixed by #4354 (recorded); crash, fixed by #4354 (recorded); crash, fixed by #4354 (recorded); crash, fixed by #4354 (recorded); crash, fixed by #4354 (recorded)
- [valkey-io/valkey#4920](https://github.com/valkey-io/valkey/pull/4920) ([tests](tests/valkey/4920-b9e22c271/)): Fix eviction replication and replica DB selection during atomic slot migration (#4920): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4815](https://github.com/valkey-io/valkey/pull/4815) ([tests](tests/valkey/4815-8b362e17c/)): Redacting when hide_user_data_from_log is set for after error reply (#4815): 0 of 3 test(s) reached the change, coverage 83%
- [valkey-io/valkey#4637](https://github.com/valkey-io/valkey/pull/4637) ([tests](tests/valkey/4637-bce52d39f/)): Return empty result for a negative LIMIT offset in zset range commands (#4637): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4875](https://github.com/valkey-io/valkey/pull/4875) ([tests](tests/valkey/4875-f2e5af9f1/)): Restore hex float parsing for sorted set scores (#4875): 4 of 4 test(s) reached the change, coverage 85%
- [valkey-io/valkey#4645](https://github.com/valkey-io/valkey/pull/4645) ([tests](tests/valkey/4645-e8ddf8d0b/)): Fix module zset range steps re-returning the current element on btree keys (#4645): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4054](https://github.com/valkey-io/valkey/pull/4054) ([tests](tests/valkey/4054-e401d52d7/)): Reject bare "(" and empty string in zset score ranges (#4054): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4939](https://github.com/valkey-io/valkey/pull/4939) ([tests](tests/valkey/4939-fbb5ab63b/)): Copy expired-but-unreaped fields in hashTypeDup (#4939): 3 of 3 test(s) reached the change, coverage 100%; [check, introduced by #4939](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/63)
- [valkey-io/valkey#4845](https://github.com/valkey-io/valkey/pull/4845) ([tests](tests/valkey/4845-9fdda3152/)): Fix broken link in ValkeyModule_ReplyWithAttribute docs (#4845): declined: The change edits only a comment in src/module.c, fixing a broken documentation link in the ValkeyModule_ReplyWithAttribute doc comment. No executable code, reply behaviour, or build configuration is altered, so there is nothing a scenario could exercise.
- [valkey-io/valkey#4894](https://github.com/valkey-io/valkey/pull/4894) ([tests](tests/valkey/4894-7f72724dc/)): Fix untracked expired hash fields after listpack-to-hashtable conversion (#4894): 2 of 2 test(s) reached the change, coverage 100%; crash, fixed by #4894 (recorded); crash, fixed by #4894 (recorded)
- [valkey-io/valkey#4817](https://github.com/valkey-io/valkey/pull/4817) ([tests](tests/valkey/4817-151906640/)): Drop WRITE flag from pausetest timer commands to shorten test sleeps (#4817): declined: The change modifies only Valkey's own test assets: tests/modules/pausetest.c (dropping the 'write' flag from the PAUSETEST.TIMER_* commands of the test-only pausetest module) and the pausetest.tcl test, so the pause can be set before scheduling the timer and the timer delay shortened from 
- [valkey-io/valkey#4679](https://github.com/valkey-io/valkey/pull/4679) ([tests](tests/valkey/4679-741b0159f/)): Support LBOUND, UBOUND, SATURATE, ENX, and PERSIST options for INCREX (#4679): 6 of 6 test(s) reached the change, coverage 89%

### valkey-io/valkey-bloom

[2026-11-05T18:09:08+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42081336814): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-05T18:37:25+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42085913031): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-05T17:55:09+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42040984391): 20 merge(s) tested, 4 without a code change.

- [valkey-io/valkey-search#1083](https://github.com/valkey-io/valkey-search/pull/1083) ([tests](tests/valkey-search/1083-5b8c24328/)): FT.Hybrid Initial Implementation (#1083): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1362](https://github.com/valkey-io/valkey-search/pull/1362) ([tests](tests/valkey-search/1362-c7c4579dc/)): Fix HNSW insertion failure on tombstone clusters and self-heal root (#1362): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#1383](https://github.com/valkey-io/valkey-search/pull/1383) ([tests](tests/valkey-search/1383-8b2fd84b9/)): Install gcc14 static libstdc++ for benchmark builds due to #1360 (#1383): declined: The change edits only ci/install_deps.sh, installing the gcc14-libstdc++-static package and asserting that libstdc++.a exists so CI benchmark builds can link statically. It is purely a build/CI infrastructure change with no modification to module code, commands, replies, persistence, or re
- [valkey-io/valkey-search#1204](https://github.com/valkey-io/valkey-search/pull/1204) ([tests](tests/valkey-search/1204-207aa0591/)): Remove continue in OnFlushDBEnded, to async free when recreate fails (#1204): declined: The diff only removes a `continue` inside OnFlushDBEnded's recreate-failure branch, so a schema whose coordinator-mode recreation fails during FLUSHDB is freed on the utility thread instead of inline on the main thread. That branch is unreachable from any input: recreating a just-removed s
- [valkey-io/valkey-search#1320](https://github.com/valkey-io/valkey-search/pull/1320) ([tests](tests/valkey-search/1320-feccde4ad/)): fix: guard HNSW prefetches against empty neighbor lists (#1320): declined: The change is internal memory-safety hardening in the bundled HNSW library and has no command-reachable behavior difference. Hunk 1 (searchBaseLayer) only moves two non-faulting __builtin_prefetch(visited_array+id) hints inside an if(size>0) block; the sole dereferencing prefetch there was
- [valkey-io/valkey-search#1364](https://github.com/valkey-io/valkey-search/pull/1364): to retry, `sudo apt-get install -y cmake ninja-build` failed in walk-work (exit 22): Preparing to unpack .../librhash1_1.4.6-1.1_amd64v3.deb ... Unpacking librhash1:amd64 (1.4.6-1.1) ... Selecting previously un
- [valkey-io/valkey-search#1312](https://github.com/valkey-io/valkey-search/pull/1312) ([tests](tests/valkey-search/1312-583fe14cd/)): Fix stale RDB restore counters inflating number_of_indexes metric (#1312): declined: The change fixes an inflated search_number_of_indexes INFO metric, but the inflated paths are unreachable by the fuzzer. The at-rest over-count requires the non-index RDB_SECTION_GLOBAL_METADATA section, which valkey-search writes only in coordinator mode (the Hidden(), start-up-only, argv
- [valkey-io/valkey-search#1355](https://github.com/valkey-io/valkey-search/pull/1355) ([tests](tests/valkey-search/1355-f737938b5/)): Run integration tests in parallel (reduces runtime from ~12 minutes to ~95 seconds) (#1355): declined: The change modifies only valkey-search's own build and test infrastructure: build.sh and scripts/common.rc gain a --parallel flag and dependency checks; integration/ and testing/integration/ gain pytest-xdist support, per-worker port/log/tmpdir isolation, and hardened setup/teardown cleanu
- [valkey-io/valkey-search#1217](https://github.com/valkey-io/valkey-search/pull/1217) ([tests](tests/valkey-search/1217-bf2e6fc72/)): Fix SORTBY with NOCONTENT returning unsorted results (#1215) (#1217): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1374](https://github.com/valkey-io/valkey-search/pull/1374) ([tests](tests/valkey-search/1374-ad9ea08b5/)): vmsdk: keep std symbols local in versionscript.lds required due to recent change in allocation strategy (#1374): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#1292](https://github.com/valkey-io/valkey-search/pull/1292) ([tests](tests/valkey-search/1292-a450bf9c8/)): Avoid main-thread deadlock when draining the multi/exec queue during a fork (#1292): 0 of 3 test(s) reached the change; [check, introduced by #1292](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/64)
- [valkey-io/valkey-search#1325](https://github.com/valkey-io/valkey-search/pull/1325) ([tests](tests/valkey-search/1325-df38d5f17/)): Fix VectorRegistry lifecycle management, multi-DB isolation, and server event handling (#1325): 8 of 8 test(s) reached the change; crash, fixed by #1325 (recorded)
- [valkey-io/valkey-search#1360](https://github.com/valkey-io/valkey-search/pull/1360) ([tests](tests/valkey-search/1360-024955289/)): New memory allocation machinery (#1360): 0 of 6 test(s) reached the change
- [valkey-io/valkey-search#1310](https://github.com/valkey-io/valkey-search/pull/1310) ([tests](tests/valkey-search/1310-e7e60037b/)): Reapplies indexing for zero-length keys(#1171)  (#1310): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#1368](https://github.com/valkey-io/valkey-search/pull/1368) ([tests](tests/valkey-search/1368-3e8f9f778/)): Add configurable maximum TIMEOUT for search commands (#1368): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#1357](https://github.com/valkey-io/valkey-search/pull/1357) ([tests](tests/valkey-search/1357-5da3b130b/)): Scoring: scoring-disabled dev only config (#1357): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1271](https://github.com/valkey-io/valkey-search/pull/1271) ([tests](tests/valkey-search/1271-e4880d457/)): Extended Array compatibility fixes (#1271): 6 of 6 test(s) reached the change
- [valkey-io/valkey-search#1317](https://github.com/valkey-io/valkey-search/pull/1317) ([tests](tests/valkey-search/1317-c8601e0d7/)): Follow-ups from #1268 (the PR search benchmark workflow) (#1317): declined: The change modifies only CI and build tooling: the search-benchmark-pr.yml GitHub Actions workflow (runner label change, trusted checkout of CI scripts, cleanup of the checkout directory) and a new ci/install_deps.sh script that installs build dependencies and the GCC toolchain on Amazon L
- [valkey-io/valkey-search#1345](https://github.com/valkey-io/valkey-search/pull/1345) ([tests](tests/valkey-search/1345-1e41c0a9d/)): Scoring: score all-text OR by fanning out fetchers instead of building a OR composite iterator (#1345): 0 of 4 test(s) reached the change
- [valkey-io/valkey-search#1001](https://github.com/valkey-io/valkey-search/pull/1001) ([tests](tests/valkey-search/1001-02a550668/)): Implement FLOAT16 and BFLOAT16 (#1001): 6 of 6 test(s) reached the change

## Totals

- Merges whose tests reached the change: 181 of 233 tested (78%)
- Changed-line coverage: median 100% of 83 merge(s) measured, 3 under 50%
- Recall: 49 parents flagged of 62 fix merges
- Claude: $1354.46 in the last 24 hours, $1354.46 in the last 7 days, $1354.46 in all
- Issues filed: 22
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

## Retries

- [valkey-io/valkey-bloom#110](https://github.com/valkey-io/valkey-bloom/pull/110): attempt 0 of 3, walked before every test that ran to its end hunted (23753e31): walked again
- [valkey-io/valkey-search#1364](https://github.com/valkey-io/valkey-search/pull/1364): attempt 1 of 3, `sudo apt-get install -y cmake ninja-build` failed in walk-work (exit 22): Preparing to unpack .../librhash1_1.4.6-1.1_amd64v3.deb ... Unpacking librhash1:amd64 (1.4.6-1.1) ... Selecting previously un
- [valkey-io/valkey#5173](https://github.com/valkey-io/valkey/pull/5173): attempt 0 of 3, the job left no result
- [valkey-io/valkey#5110](https://github.com/valkey-io/valkey/pull/5110): attempt 0 of 3, walked before every test that ran to its end hunted (23753e31): walked again
