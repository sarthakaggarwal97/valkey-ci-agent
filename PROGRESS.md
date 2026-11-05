# Fuzzer walk

Updated 2026-11-05T15:33+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 135 | 2356 | #2250 (2026-10-21) | 135 | 19.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 109 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 62 | 8.9 |
| valkey-io/valkey-search | 77 | 591 | #1362 (2026-09-12) | 77 | 11.0 |

## Last runs

### valkey-io/valkey

[2026-11-05T14:28:48+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42025798250): 20 merge(s) tested, 3 without a code change.

- [valkey-io/valkey#5173](https://github.com/valkey-io/valkey/pull/5173): to retry, the job left no result
- [valkey-io/valkey#5030](https://github.com/valkey-io/valkey/pull/5030): to retry, the job left no result
- [valkey-io/valkey#4823](https://github.com/valkey-io/valkey/pull/4823) ([tests](tests/valkey/4823-e50b62d0b/)): [correctness] Fix LSET modifying elements for out-of-range 64-bit indexes (#4823): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4856](https://github.com/valkey-io/valkey/pull/4856) ([tests](tests/valkey/4856-e23cab135/)): Optimize B+tree ZSCAN with item references and prefetch (#4856): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2496](https://github.com/valkey-io/valkey/pull/2496) ([tests](tests/valkey/2496-de4b2c198/)): Add static specifier to the internal functions of HLL (#2496): declined: The change only adds the `static` specifier to internal functions in src/hyperloglog.c (MurmurHash64A, hllPatLen, hllDenseSet, hllCount, the merge/compress helpers, createHLLObject, isHLLObjectOrReply). It is a pure linkage/visibility refactor with no change to any function body, command r
- [valkey-io/valkey#4976](https://github.com/valkey-io/valkey/pull/4976) ([tests](tests/valkey/4976-2c11a10f4/)): Fix false sharing in IO thread active-time and job-finished counters (#4976): declined: The change only adds cache-line padding (_Alignas wrappers) around the io_jobs_finished counter and the per-IO-thread used_active_time_io_thread slots to eliminate false sharing. Every read and write keeps the same atomic operation, value, and memory order; the only change is the counters'
- [valkey-io/valkey@aaeee8d5b17d](https://github.com/valkey-io/valkey/commit/aaeee8d5b17d490a9490a83407591d746d4d4051) ([tests](tests/valkey/aaeee8d5b/)): bgiteration: remove from early-iterate on key-delete: 0 of 6 test(s) reached the change, coverage 100%; crash, fixed by aaeee8d5b17d (recorded); crash, fixed by aaeee8d5b17d (recorded); crash, introduced and since fixed (recorded); [check, predates aaeee8d5b17d](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/62)
- [valkey-io/valkey#4927](https://github.com/valkey-io/valkey/pull/4927) ([tests](tests/valkey/4927-668d875ca/)): Link macOS modules with the compiler driver (#4927): declined: The change is purely a build-system fix: three Makefiles (src/modules, src/modules/lua, tests/modules) switch the macOS-only link step for modules from raw ld with a hardcoded SDK libSystem path to linking via the compiler driver ($(CC) $(CFLAGS)). It is guarded by uname_S == Darwin, so Li
- [valkey-io/valkey#4952](https://github.com/valkey-io/valkey/pull/4952) ([tests](tests/valkey/4952-989889a9b/)): Send SELECT at the end of the slot migration snapshot (#4952): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4997](https://github.com/valkey-io/valkey/pull/4997) ([tests](tests/valkey/4997-5c6a0cdcf/)): Fix ZINTER/ZINTERSTORE score when a weighted term is 0*inf (#4997): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4782](https://github.com/valkey-io/valkey/pull/4782) ([tests](tests/valkey/4782-6902fdc96/)): Make quicklist read-only iteration non-mutating on compressed nodes (#4782): 0 of 6 test(s) reached the change, coverage 100%; crash, predates #4782, since fixed (recorded)
- [valkey-io/valkey#4968](https://github.com/valkey-io/valkey/pull/4968) ([tests](tests/valkey/4968-8b9019c95/)): Fix crash when a database's keyspace is replaced mid-stage during defrag (#4968): 0 of 3 test(s) reached the change, coverage 100%; crash, fixed by #4968 (recorded)
- [valkey-io/valkey#4911](https://github.com/valkey-io/valkey/pull/4911) ([tests](tests/valkey/4911-a4e5b695a/)): chore: update stale skiplist comments to the ordered index(#4911): declined: The diff only updates stale code comments (renaming 'skiplist' references to 'ordered index' in fbtree.c, module.c, rdb.c and t_zset.c). No executable code, data layout, config, reply, or RDB format changes; the compiled binary is behaviourally identical to the parent, so no scenario can d
- [valkey-io/valkey#4489](https://github.com/valkey-io/valkey/pull/4489) ([tests](tests/valkey/4489-e283addef/)): Hold no descriptor on nodes.conf when rename() publishes it (#4489): 0 of 8 test(s) reached the change, coverage 64%
- [valkey-io/valkey#2241](https://github.com/valkey-io/valkey/pull/2241) ([tests](tests/valkey/2241-5f38f9e89/)): Spelling 4 (#2241): declined: The change (Spelling 4, #2241) only fixes typos in comments across allocator_defrag.c, hyperloglog.c, lolwut5.c, rdb.c and sparkline.c, and renames a local variable binind to bin_index in allocatorShouldDefrag with identical expressions and control flow. No command behaviour, reply, persis
- [valkey-io/valkey#2252](https://github.com/valkey-io/valkey/pull/2252) ([tests](tests/valkey/2252-9a029ba83/)): Fix typos in comments and rename TimedOut/Sparkline functions and tests (#2252): declined: The change only renames internal C functions (migrateCloseTimedoutSockets, latencyCommandGenSparkeline, scriptIsTimedout, sentinelKillTimedoutScripts) with all call sites updated, fixes typos in comments and TCL test names, and changes a reply string solely in the hello* example modules, w
- [valkey-io/valkey#2242](https://github.com/valkey-io/valkey/pull/2242) ([tests](tests/valkey/2242-6b8dfd83c/)): spelling: cannot (#2242): declined: The change is a spelling fix ('can not' -> 'cannot') confined to code comments, serverLog message strings that are only written to the server log (bio.c, debug.c, io_threads.c, rdma.c), test module comments, and Tcl test names/comments. No client-visible reply, error message, config option
- [valkey-io/valkey#4417](https://github.com/valkey-io/valkey/pull/4417) ([tests](tests/valkey/4417-32e3e81fa/)): Speed up HGETALL, HKEYS and HVALS with exact-length headers and fused bulk replies (#4417): 0 of 8 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4107](https://github.com/valkey-io/valkey/pull/4107) ([tests](tests/valkey/4107-8b73b9734/)): Fix false sharing in per-thread memory usage counters (#4107): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3728](https://github.com/valkey-io/valkey/pull/3728) ([tests](tests/valkey/3728-bc7ceb79a/)): Fix grammatical typo "This functions" in code comments (#3728): declined: The change is purely cosmetic: it fixes the grammatical typo "This functions" in code comments across db.c, hashtable.c, kvstore.c, object.c and rand.c. No executable code, command behaviour, reply, replication, persistence or configuration path is modified, so no scenario can distinguish 

### valkey-io/valkey-bloom

[2026-11-05T15:33:12+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42055759828): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-05T15:32:06+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42046797966): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 163 of 208 tested (78%)
- Changed-line coverage: median 100% of 70 merge(s) measured, 4 under 50%
- Recall: 41 parents flagged of 52 fix merges
- Claude: $1231.03 in the last 24 hours, $1231.03 in the last 7 days, $1231.03 in all
- Issues filed: 20
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

## Retries

- [valkey-io/valkey-search#1083](https://github.com/valkey-io/valkey-search/pull/1083): attempt 0 of 3, the job left no result
- [valkey-io/valkey#5173](https://github.com/valkey-io/valkey/pull/5173): attempt 0 of 3, the job left no result
- [valkey-io/valkey#5030](https://github.com/valkey-io/valkey/pull/5030): attempt 0 of 3, the job left no result
