# Fuzzer walk

Updated 2026-11-05T21:29+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 179 | 2312 | #3413 (2026-10-11) | 179 | 25.6 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 109 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 62 | 8.9 |
| valkey-io/valkey-search | 125 | 543 | #946 (2026-08-19) | 125 | 17.9 |

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

[2026-11-05T21:00:44+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42108552108): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-05T21:24:15+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42112209418): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-05T21:29:24+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42079311522): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey-search#1364](https://github.com/valkey-io/valkey-search/pull/1364) ([tests](tests/valkey-search/1364-bd6d2f238/)): fixing redis search divergence #4: sort key prefix always use #; strings should use $ (#1364): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1084](https://github.com/valkey-io/valkey-search/pull/1084) ([tests](tests/valkey-search/1084-9e8fec7e0/)): Implement Renaming in LOAD clause of FT.AGGREGATE (COMPATIBILTY) PLUS FIX JSON on LOAD clause (#1084): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1196](https://github.com/valkey-io/valkey-search/pull/1196) ([tests](tests/valkey-search/1196-6e38de5d7/)): Allow indexing the same field as both TEXT and TAG (#1195) (#1196): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1331](https://github.com/valkey-io/valkey-search/pull/1331) ([tests](tests/valkey-search/1331-0190919f1/)): Fix ThreadPool suspend/resume race with concurrent Resize (#1331): 0 of 5 test(s) reached the change
- [valkey-io/valkey-search#1352](https://github.com/valkey-io/valkey-search/pull/1352) ([tests](tests/valkey-search/1352-bc55ab18c/)): fix breaking changes introduced in #1008 (#1352): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#949](https://github.com/valkey-io/valkey-search/pull/949) ([tests](tests/valkey-search/949-7296a5a1b/)): feat: add Random Sample reducer to FT.Aggregate groupby stage (#949): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#1341](https://github.com/valkey-io/valkey-search/pull/1341) ([tests](tests/valkey-search/1341-7f04b7d6e/)): Implicitly load fields referenced by a pipeline stage (#919) (#1341): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1349](https://github.com/valkey-io/valkey-search/pull/1349) ([tests](tests/valkey-search/1349-e8f05bbd9/)): Fix HNSW load crash: remove incorrect max_level <= element_count check (#1349): 0 of 3 test(s) reached the change
- [valkey-io/valkey-search#759](https://github.com/valkey-io/valkey-search/pull/759) ([tests](tests/valkey-search/759-3fe91fb61/)): Handle ipv6 in coordinator (#759): declined: The change only affects gRPC target-address formatting in the valkey-search coordinator, and no fuzzer scenario can reach it. (1) All three changed call sites run only when the coordinator is enabled: ValkeySearch::Startup creates the MetadataManager, client pool and fanout paths only unde
- [valkey-io/valkey-search#1343](https://github.com/valkey-io/valkey-search/pull/1343) ([tests](tests/valkey-search/1343-b747be659/)): Fix aarch64 RDB compatibility integration tests (#1343): declined: The change only fixes the project's own integration-test harness and build scripts: it adds an aarch64 prebuilt 1.0.0 module fixture to a test zip and makes the Python RDB-compatibility test pick the fixture per architecture, moves AI-assistant skill docs, and corrects an inverted sanitize
- [valkey-io/valkey-search#1075](https://github.com/valkey-io/valkey-search/pull/1075) ([tests](tests/valkey-search/1075-293b0f27a/)): Fix TAG indexing of non UTF-8 data (#1075): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1287](https://github.com/valkey-io/valkey-search/pull/1287) ([tests](tests/valkey-search/1287-74037e9df/)): Scoring: BM25STD for text, numeric and tag (#1287): 4 of 4 test(s) reached the change; [regressed-fix, the fix of #1287 regressed](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/68)
- [valkey-io/valkey-search#1273](https://github.com/valkey-io/valkey-search/pull/1273) ([tests](tests/valkey-search/1273-6a5644a23/)): Fix FT.AGGREGATE alias re-use corrupting record state (#1251) (#1273): declined: no proposed test validated
- [valkey-io/valkey-search#1316](https://github.com/valkey-io/valkey-search/pull/1316) ([tests](tests/valkey-search/1316-7005c00c8/)): introduce VectorRegistry for memory sharing (#1316): 6 of 6 test(s) reached the change; crash, fixed by #1316 (recorded)
- [valkey-io/valkey-search#1232](https://github.com/valkey-io/valkey-search/pull/1232) ([tests](tests/valkey-search/1232-5662dc10c/)): Changing HNSWLib to store raw vectors (#1232): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1168](https://github.com/valkey-io/valkey-search/pull/1168) ([tests](tests/valkey-search/1168-0f3c267cd/)): Hnswlib templatized vectors (#1168): declined: no proposed test validated
- [valkey-io/valkey-search#971](https://github.com/valkey-io/valkey-search/pull/971) ([tests](tests/valkey-search/971-cbad9d689/)): change ft.aggregate to generate the Reducer name the same as Redisearch (#971): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#1267](https://github.com/valkey-io/valkey-search/pull/1267) ([tests](tests/valkey-search/1267-66d1c822f/)): Allow SORTBY on vector score in ascending order (#1267): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search@cae4fd2dbb14](https://github.com/valkey-io/valkey-search/commit/cae4fd2dbb14189dcd7326366e9ff203d58d1ae7) ([tests](tests/valkey-search/cae4fd2db/)): Revert "Fix indexing for zero-length keys (#1171)": 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#1264](https://github.com/valkey-io/valkey-search/pull/1264) ([tests](tests/valkey-search/1264-e415fe131/)): Fix numeric field precision loss in content materialization (#1264): 2 of 2 test(s) reached the change

## Totals

- Merges whose tests reached the change: 205 of 264 tested (78%)
- Changed-line coverage: median 100% of 97 merge(s) measured, 4 under 50%
- Recall: 60 parents flagged of 76 fix merges
- Claude: $1578.94 in the last 24 hours, $1578.94 in the last 7 days, $1578.94 in all
- Issues filed: 26
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

## Retries

- [valkey-io/valkey#5173](https://github.com/valkey-io/valkey/pull/5173): attempt 0 of 3, the job left no result
- [valkey-io/valkey#5110](https://github.com/valkey-io/valkey/pull/5110): attempt 0 of 3, walked before every test that ran to its end hunted (23753e31): walked again
- [valkey-io/valkey#4214](https://github.com/valkey-io/valkey/pull/4214): attempt 1 of 3, Claude Code failed (rc=1):
