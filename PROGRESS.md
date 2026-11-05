# Fuzzer walk

Updated 2026-11-05T22:02+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 204 | 2288 | #4778 (2026-09-25) | 204 | 29.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 109 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 62 | 8.9 |
| valkey-io/valkey-search | 125 | 543 | #946 (2026-08-19) | 125 | 17.9 |

## Last runs

### valkey-io/valkey

[2026-11-05T21:33:12+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42086130846): 20 merge(s) tested, 6 without a code change.

- [valkey-io/valkey#4983](https://github.com/valkey-io/valkey/pull/4983) ([tests](tests/valkey/4983-7836c5afe/)): Fix for XINFO GROUPS reports too-high lag after XTRIM removes unread … (#4983): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5173](https://github.com/valkey-io/valkey/pull/5173) ([tests](tests/valkey/5173-b14acea0a/)): Reject a non-positive --hotkeys-count in valkey-cli (#5173): 1 of 1 test(s) reached the change, coverage 100%; crash, fixed by #5173 (recorded)
- [valkey-io/valkey#5110](https://github.com/valkey-io/valkey/pull/5110) ([tests](tests/valkey/5110-130b36084/)): Stop zset ops from rewriting keyspace set members during lookups (#5110): 1 of 3 test(s) reached the change, coverage 60%; crash, fixed by #5110 (recorded); regressed-fix, not reproduced (recorded)
- [valkey-io/valkey#4214](https://github.com/valkey-io/valkey/pull/4214) ([tests](tests/valkey/4214-035a9ee25/)): fix(acl): prevent NULL pointer dereference on malformed selector in ACL file (#4214): declined: The fix (src/acl.c:3244) adds a missing `sdsfreesplitres(argv, argc); continue;` so that when ACLMergeSelectorArguments returns NULL on a user line with an unmatched parenthesis in a selector, the loader stops instead of falling through to `for (j=0; j<merged_argc; j++) acl_args[j]=...` wi
- [valkey-io/valkey#3413](https://github.com/valkey-io/valkey/pull/3413) ([tests](tests/valkey/3413-b3a135637/)): Optimize infoCommand with SDS pre-allocation (#3413): declined: The change adds a single sdsMakeRoomFor(info, 4096) call to pre-allocate the SDS buffer used to build the INFO reply. sdsMakeRoomFor only grows allocated capacity and leaves the string length and contents untouched, so the INFO output and every reply are byte-identical to the parent build;
- [valkey-io/valkey#4707](https://github.com/valkey-io/valkey/pull/4707) ([tests](tests/valkey/4707-2fce936b3/)): Skip raw TSC when the kernel clocksource is not tsc (#4707): 0 of 2 test(s) reached the change, coverage 75%
- [valkey-io/valkey#4787](https://github.com/valkey-io/valkey/pull/4787) ([tests](tests/valkey/4787-1db8edcbe/)): geohash code cleanup (#4787): declined: The change only deletes the unused wrapper function geohashDecodeWGS84 (which merely forwarded to geohashDecodeType) and its header declaration. No call sites are modified, so no code path, command reply, encoding, or data format changes; it is a pure dead-code refactor with no observable 
- [valkey-io/valkey#4765](https://github.com/valkey-io/valkey/pull/4765) ([tests](tests/valkey/4765-ff9481cfb/)): Restore replica flow control with I/O threads enabled (#4765): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4657](https://github.com/valkey-io/valkey/pull/4657) ([tests](tests/valkey/4657-f213a6750/)): Build: make the Make / CMake gtest build link on macOS (#4657): declined: The change only modifies the gtest unit-test build machinery (src/unit CMakeLists.txt and Makefile, plus the symbol-wrapping Python scripts) so the unit-test binary links on macOS. It compiles no code into valkey-server or any shipped tool and changes no runtime behaviour, replies, persist
- [valkey-io/valkey#4800](https://github.com/valkey-io/valkey/pull/4800) ([tests](tests/valkey/4800-642196f63/)):  bracketed IPv6 cluster CLI address parsing (#4800): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3645](https://github.com/valkey-io/valkey/pull/3645): to retry, 3 of 3 run(s) could not start: the run could not start: environment acquisition failed
- [valkey-io/valkey#4424](https://github.com/valkey-io/valkey/pull/4424) ([tests](tests/valkey/4424-2a823011b/)): Resolve slot from key for module and arbitrary-key commands (#4424): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4783](https://github.com/valkey-io/valkey/pull/4783) ([tests](tests/valkey/4783-28ecc5142/)): Fix HEXPIRE family command summaries (#4783): 2 of 2 test(s) reached the change
- [valkey-io/valkey#4710](https://github.com/valkey-io/valkey/pull/4710) ([tests](tests/valkey/4710-278384274/)): Fix IO thread shutdown cleanup leak (#4710): 0 of 10 test(s) reached the change, coverage 40%
- [valkey-io/valkey#4775](https://github.com/valkey-io/valkey/pull/4775) ([tests](tests/valkey/4775-9b270b6d8/)): Bound the time spent evicting keys from the tracking table (#4775): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4567](https://github.com/valkey-io/valkey/pull/4567) ([tests](tests/valkey/4567-329309165/)): Fix valkey-benchmark spin on would-block writes (#4567): declined: no proposed test validated
- [valkey-io/valkey#4754](https://github.com/valkey-io/valkey/pull/4754) ([tests](tests/valkey/4754-51b3f0c87/)): Support streaming-compressed RDB preambles in AOF (#4754): 2 of 19 test(s) reached the change, coverage 75%
- [valkey-io/valkey#4632](https://github.com/valkey-io/valkey/pull/4632) ([tests](tests/valkey/4632-722305525/)): bgiteration hardening (#4632): 0 of 5 test(s) reached the change, coverage 100%; crash, introduced and since fixed (recorded); crash, fixed by #4632 (recorded)
- [valkey-io/valkey#4759](https://github.com/valkey-io/valkey/pull/4759) ([tests](tests/valkey/4759-026d29167/)): Fix inline protocol parser stalling on an embedded '\0' (#4759): 0 of 4 test(s) reached the change, coverage 0%
- [valkey-io/valkey#4749](https://github.com/valkey-io/valkey/pull/4749) ([tests](tests/valkey/4749-7fe7ca45f/)): Decouple `rdbcompression` (disk format) from `repl-compression` (wire format) (#4749): 2 of 24 test(s) reached the change, coverage 79%

### valkey-io/valkey-bloom

[2026-11-05T22:02:15+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42117888034): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-05T21:54:55+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42116821515): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 213 of 278 tested (77%)
- Changed-line coverage: median 100% of 110 merge(s) measured, 6 under 50%
- Recall: 62 parents flagged of 80 fix merges
- Claude: $1691.20 in the last 24 hours, $1691.20 in the last 7 days, $1691.20 in all
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

- [valkey-io/valkey#3645](https://github.com/valkey-io/valkey/pull/3645): attempt 1 of 3, 3 of 3 run(s) could not start: the run could not start: environment acquisition failed
