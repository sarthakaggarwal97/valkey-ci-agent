# Fuzzer walk

Updated 2026-11-05T17:35+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 158 | 2333 | #4890 (2026-10-16) | 158 | 22.6 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 109 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 62 | 8.9 |
| valkey-io/valkey-search | 77 | 591 | #1362 (2026-09-12) | 77 | 11.0 |

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

[2026-11-05T15:33:12+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42055759828): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-05T17:35:51+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42076075945): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-05T14:01:10+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42007906355): 20 merge(s) tested, 6 without a code change.

- [valkey-io/valkey-search#1464](https://github.com/valkey-io/valkey-search/pull/1464) ([tests](tests/valkey-search/1464-0cbca0e60/)): Fix GROUPBY 0 being rejected in FT.AGGREGATE (#1464): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1487](https://github.com/valkey-io/valkey-search/pull/1487) ([tests](tests/valkey-search/1487-c24e74581/)): perf: Restore HNSW ingestion and search throughput to 1.2 levels (#1487): declined: no proposed test validated
- [valkey-io/valkey-search#1304](https://github.com/valkey-io/valkey-search/pull/1304) ([tests](tests/valkey-search/1304-7687ca2e9/)): Skip metadata reconciliation after replica demotion (#1304): declined: The only behavioral change is a race guard added inside the asynchronous RunByMain callback in MetadataManager::HandleBroadcastedMetadata: it re-checks vmsdk::IsReplica(ctx) on the main thread after the gRPC GetGlobalMetadata fetch returns, discarding the reconciliation if the node was dem
- [valkey-io/valkey-search#1445](https://github.com/valkey-io/valkey-search/pull/1445) ([tests](tests/valkey-search/1445-0a8e615a2/)): fixed: sort key should be nil instead of `#` when sortby is missing (#1445): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1395](https://github.com/valkey-io/valkey-search/pull/1395) ([tests](tests/valkey-search/1395-20f414ee1/)): Accept NOHL and SORTABLE UNF in FT.CREATE, and report sortable/unf in FT.INFO (#1395): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#1354](https://github.com/valkey-io/valkey-search/pull/1354) ([tests](tests/valkey-search/1354-cc6cb5e7f/)): Scoring: BM25STD scoring for stemming queries (#1354): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#1394](https://github.com/valkey-io/valkey-search/pull/1394) ([tests](tests/valkey-search/1394-cad356055/)): Add WITHCURSOR to FT.AGGREGATE and FT.SEARCH, and the FT.CURSOR command (#1394): 4 of 4 test(s) reached the change; [crash, appeared after #1394](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/61); check, introduced and since fixed (recorded); crash, fixed by #1394 (recorded)
- [valkey-io/valkey-search#967](https://github.com/valkey-io/valkey-search/pull/967) ([tests](tests/valkey-search/967-d719c30c2/)): feat: Implement INKEYS parameter for FT.SEARCH (#967): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#1400](https://github.com/valkey-io/valkey-search/pull/1400) ([tests](tests/valkey-search/1400-0e1bc3303/)): Various small scoring changes (#1400): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#1083](https://github.com/valkey-io/valkey-search/pull/1083): to retry, the job left no result
- [valkey-io/valkey-search#1402](https://github.com/valkey-io/valkey-search/pull/1402) ([tests](tests/valkey-search/1402-e968b168c/)): Fixing Alpine Build (#1402): 0 of 6 test(s) reached the change
- [valkey-io/valkey-search#1406](https://github.com/valkey-io/valkey-search/pull/1406) ([tests](tests/valkey-search/1406-ef5771391/)): Fix long-running clang-format after running integration tests (#1406): declined: The change modifies only the format() function in build.sh, adding a -prune for .build* directories so clang-format skips build artifacts after integration tests. It is a developer-tooling/build-script fix with no change to any module source file, command, reply, persistence, or replicatio
- [valkey-io/valkey-search#1389](https://github.com/valkey-io/valkey-search/pull/1389) ([tests](tests/valkey-search/1389-6428b669b/)): Bug fix: only last RETURN in SEARCH should be effective (#1389): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1350](https://github.com/valkey-io/valkey-search/pull/1350) ([tests](tests/valkey-search/1350-4f6ef4b04/)): Scoring: BM25STD scoring for prefix, suffix and fuzzy queries (#1350): 7 of 7 test(s) reached the change
- [valkey-io/valkey-search#1218](https://github.com/valkey-io/valkey-search/pull/1218) ([tests](tests/valkey-search/1218-03f27d2ac/)): Support field-scoped text group @field:(a|b|c) (#1214) (#1218): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1090](https://github.com/valkey-io/valkey-search/pull/1090) ([tests](tests/valkey-search/1090-0cc7609c6/)): Fix AS parameter substitution for vector score aliases (#1090): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1279](https://github.com/valkey-io/valkey-search/pull/1279) ([tests](tests/valkey-search/1279-20757bae4/)): RFC for multi-language support in FTS (#1279): declined: The change is documentation-only: it adds rfc/multi-language-support.md (a proposal document for future multi-language FTS support) and excludes that file from the typos checker in .config/typos.toml. No source code, build configuration, or runtime behaviour of the module changes, so the c
- [valkey-io/valkey-search#984](https://github.com/valkey-io/valkey-search/pull/984) ([tests](tests/valkey-search/984-cbf0fda34/)): Implement FILTER option on FT.CREATE (#984): 5 of 5 test(s) reached the change
- [valkey-io/valkey-search#1373](https://github.com/valkey-io/valkey-search/pull/1373) ([tests](tests/valkey-search/1373-707324aa1/)): FT.AGGREGATE: SORTBY ignores a following LIMIT, breaking paging past the first page (#1373): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1366](https://github.com/valkey-io/valkey-search/pull/1366) ([tests](tests/valkey-search/1366-9db11eff5/)): Move compatibility reference to redis:latest and fix what it exposed (supersedes #1068, #1239) (#1366): 4 of 8 test(s) reached the change

## Totals

- Merges whose tests reached the change: 173 of 222 tested (78%)
- Changed-line coverage: median 100% of 84 merge(s) measured, 4 under 50%
- Recall: 46 parents flagged of 58 fix merges
- Claude: $1294.05 in the last 24 hours, $1294.05 in the last 7 days, $1294.05 in all
- Issues filed: 21
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

## Retries

- [valkey-io/valkey-search#1083](https://github.com/valkey-io/valkey-search/pull/1083): attempt 0 of 3, the job left no result
- [valkey-io/valkey#5173](https://github.com/valkey-io/valkey/pull/5173): attempt 0 of 3, the job left no result
