# Fuzzer walk

Updated 2026-11-05T20:29+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 179 | 2312 | #3413 (2026-10-11) | 179 | 25.6 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 109 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 62 | 8.9 |
| valkey-io/valkey-search | 100 | 568 | #1084 (2026-09-06) | 100 | 14.3 |

## Last runs

### valkey-io/valkey

[2026-11-05T18:38:04+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42061353623): 20 merge(s) tested, 4 without a code change.

- [valkey-io/valkey#5173](https://github.com/valkey-io/valkey/pull/5173): to retry, the job left no result
- [valkey-io/valkey#4890](https://github.com/valkey-io/valkey/pull/4890) ([tests](tests/valkey/4890-e1b4ec845/)): Fail the RDB transcode on bytes after the source frame (#4890): 0 of 1 test(s) reached the change, coverage 33%
- [valkey-io/valkey#3496](https://github.com/valkey-io/valkey/pull/3496) ([tests](tests/valkey/3496-070440826/)): aof: write directly to server.aof_buf in feedAppendOnlyFile (#3496): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4829](https://github.com/valkey-io/valkey/pull/4829) ([tests](tests/valkey/4829-d87001320/)): Allow `vset` to track size pre-emptively (#4829): 4 of 9 test(s) reached the change, coverage 84%
- [valkey-io/valkey#4644](https://github.com/valkey-io/valkey/pull/4644) ([tests](tests/valkey/4644-035b8d61d/)): Fix btree zset lex seek, NaN score bounds and a root prefix leak (#4644): 2 of 2 test(s) reached the change, coverage 89%
- [valkey-io/valkey#4852](https://github.com/valkey-io/valkey/pull/4852) ([tests](tests/valkey/4852-d379883ae/)): Optimize B+tree ZPOP by detaching edge items (#4852): declined: no proposed test validated
- [valkey-io/valkey#4733](https://github.com/valkey-io/valkey/pull/4733) ([tests](tests/valkey/4733-4d5fdeb0c/)): Fix sds overflow assert on RESTORE payload with an overflowing string length (#4733): 1 of 2 test(s) reached the change, coverage 80%; crash, fixed by #4733 (recorded)
- [valkey-io/valkey#4251](https://github.com/valkey-io/valkey/pull/4251) ([tests](tests/valkey/4251-ce5120736/)): deflake ccov: Use _exit() when a child is killed by SIGUSR1 (#4251): declined: In non-coverage builds exitFromChild() is already _exit(), so replacing it with a direct _exit() in sigKillChildHandler is a no-op: a SIGUSR1-killed child exits with the same status and the parent's checkChildrenDone() path is untouched (only a comment changed there). The behaviour differe
- [valkey-io/valkey#4885](https://github.com/valkey-io/valkey/pull/4885) ([tests](tests/valkey/4885-b405ef898/)): Skip expired listpack hash fields in ValkeyModule_ScanKey (#4885): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4270](https://github.com/valkey-io/valkey/pull/4270) ([tests](tests/valkey/4270-cf5efa701/)): make getMonotonicUs static in libvalkeylua (#4270): declined: The change only adds `static` (and `static inline`) to getMonotonicUs, elapsedUs and elapsedMs in a fallback branch of function_lua.c that is compiled only when the engine's monotonic.h is not used. It changes symbol linkage to avoid clashes/exported symbols at link time, not any runtime b
- [valkey-io/valkey#4504](https://github.com/valkey-io/valkey/pull/4504) ([tests](tests/valkey/4504-e4e2c3dcf/)): Fix crash and wrong return values in the hash stringRef module API (#4504): 2 of 2 test(s) reached the change, coverage 100%; crash, fixed by #4504 (recorded)
- [valkey-io/valkey#4214](https://github.com/valkey-io/valkey/pull/4214): to retry, Claude Code failed (rc=1):
- [valkey-io/valkey#4694](https://github.com/valkey-io/valkey/pull/4694) ([tests](tests/valkey/4694-8f67ab136/)): Include roles in ACL DIGEST (#4694): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4772](https://github.com/valkey-io/valkey/pull/4772) ([tests](tests/valkey/4772-e4d62b2a3/)): Avoid propagating expired hash fields from HDEL, HGETDEL, HPERSIST, and HEXPIRE (#4772): 4 of 4 test(s) reached the change, coverage 94%
- [valkey-io/valkey#4880](https://github.com/valkey-io/valkey/pull/4880) ([tests](tests/valkey/4880-4319738e6/)): Reject relative expires that overflow when the base time is added (#4880): 3 of 5 test(s) reached the change, coverage 84%; [check, introduced by #4880](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/67)
- [valkey-io/valkey#4339](https://github.com/valkey-io/valkey/pull/4339) ([tests](tests/valkey/4339-7a782369f/)): Route security/auth crypto through the OpenSSL 3 provider API (#4339): 0 of 3 test(s) reached the change, coverage 81%
- [valkey-io/valkey#4819](https://github.com/valkey-io/valkey/pull/4819) ([tests](tests/valkey/4819-3db525ec4/)): Fix RDB and replication failures for certain listpack entry lengths (#4819): 4 of 4 test(s) reached the change, coverage 80%; [check, introduced by #4819](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/65); check, fixed by #4819 (recorded); [check, introduced by #4819](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/66); check, fixed by #4819 (recorded)
- [valkey-io/valkey#4190](https://github.com/valkey-io/valkey/pull/4190) ([tests](tests/valkey/4190-01857eb35/)): Reclaim dead client IDs from the tracking table (#4143) (#4190): declined: no proposed test validated
- [valkey-io/valkey#4843](https://github.com/valkey-io/valkey/pull/4843) ([tests](tests/valkey/4843-0eb4ba323/)): Fix XACKDEL KEEPREF reply for acked entry whose stream item is gone (#4843): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4700](https://github.com/valkey-io/valkey/pull/4700) ([tests](tests/valkey/4700-2c1e7d25b/)): Only fast-fail on NACK reasons a new election epoch can fix (#4700): 0 of 6 test(s) reached the change, coverage 100%

### valkey-io/valkey-bloom

[2026-11-05T20:29:25+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42086303278): 1 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-bloom#110](https://github.com/valkey-io/valkey-bloom/pull/110) ([tests](tests/valkey-bloom/110-9ab369980/)): Match bloom memory tracking by small over count of bytes after a defrag operation (#110): 0 of 2 test(s) reached the change

### valkey-io/valkey-json

[2026-11-05T20:22:39+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42102705573): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 191 of 248 tested (77%)
- Changed-line coverage: median 100% of 97 merge(s) measured, 4 under 50%
- Recall: 54 parents flagged of 68 fix merges
- Claude: $1458.89 in the last 24 hours, $1458.89 in the last 7 days, $1458.89 in all
- Issues filed: 25
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

## Retries

- [valkey-io/valkey-search#1364](https://github.com/valkey-io/valkey-search/pull/1364): attempt 1 of 3, `sudo apt-get install -y cmake ninja-build` failed in walk-work (exit 22): Preparing to unpack .../librhash1_1.4.6-1.1_amd64v3.deb ... Unpacking librhash1:amd64 (1.4.6-1.1) ... Selecting previously un
- [valkey-io/valkey#5173](https://github.com/valkey-io/valkey/pull/5173): attempt 0 of 3, the job left no result
- [valkey-io/valkey#5110](https://github.com/valkey-io/valkey/pull/5110): attempt 0 of 3, walked before every test that ran to its end hunted (23753e31): walked again
- [valkey-io/valkey#4214](https://github.com/valkey-io/valkey/pull/4214): attempt 1 of 3, Claude Code failed (rc=1):
