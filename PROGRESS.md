# Fuzzer walk

Updated 2026-11-05T14:07+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 114 | 2377 | #5030 (2026-10-26) | 114 | 16.3 |
| valkey-io/valkey-bloom | 107 | 2 | ce516d0ef6e3 (2024-04-26) | 107 | 15.3 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 62 | 8.9 |
| valkey-io/valkey-search | 77 | 591 | #1362 (2026-09-12) | 77 | 11.0 |

## Last runs

### valkey-io/valkey

[2026-11-05T12:16:47+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42003770568): 20 merge(s) tested, 10 without a code change.

- [valkey-io/valkey#5186](https://github.com/valkey-io/valkey/pull/5186) ([tests](tests/valkey/5186-a4ffea938/)): Skip RESP3 attributes when reading replies in valkey-cli (#5186): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5175](https://github.com/valkey-io/valkey/pull/5175) ([tests](tests/valkey/5175-663fff858/)): Stop valkey-benchmark function script generation when the buffer is full (#5175): 3 of 3 test(s) reached the change, coverage 100%; crash, fixed by #5175 (recorded)
- [valkey-io/valkey#5181](https://github.com/valkey-io/valkey/pull/5181) ([tests](tests/valkey/5181-9c4c8739d/)): Apply the library load timeout to FUNCTION RESTORE (#5181): 0 of 10 test(s) reached the change, coverage 37%
- [valkey-io/valkey#5173](https://github.com/valkey-io/valkey/pull/5173): to retry, the job left no result
- [valkey-io/valkey#5187](https://github.com/valkey-io/valkey/pull/5187) ([tests](tests/valkey/5187-eef69cb6d/)): Defer freeing the client that runs a replication role change (#5187): 0 of 4 test(s) reached the change, coverage 81%; crash, fixed by #5187 (recorded); crash, fixed by #5187 (recorded); [check, introduced by #5187](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/59)
- [valkey-io/valkey#5098](https://github.com/valkey-io/valkey/pull/5098) ([tests](tests/valkey/5098-45cd91459/)): Skip QoS event draining when AE_FILE_EVENTS is not set (#5098): declined: The change guards aeProcessQoSEvents in aeProcessEvents behind the AE_FILE_EVENTS flag, but every production caller already passes that flag: aeMain uses AE_ALL_EVENTS and processEventsWhileBlocked uses AE_FILE_EVENTS|AE_DONT_WAIT, and these are the only two call sites outside unit tests. 
- [valkey-io/valkey#5141](https://github.com/valkey-io/valkey/pull/5141) ([tests](tests/valkey/5141-6e3eddf49/)): Don't block CLUSTER SETSLOT on replica acks when the client cannot block (#5141): 2 of 4 test(s) reached the change, coverage 100%; crash, fixed by #5141 (recorded)
- [valkey-io/valkey#5148](https://github.com/valkey-io/valkey/pull/5148) ([tests](tests/valkey/5148-3350565b3/)): Allow user to pass testing option via make file & use that to dump logs in coverage job (#5148): declined: The change is purely build/CI plumbing: it adds a TEST_OPTIONS variable to the Makefile's test, test-modules, test-sentinel, test-cluster and lcov targets so extra flags can be passed to the runtest scripts, and uses it in the codecov workflow to pass --dump-logs. No Valkey server, command
- [valkey-io/valkey#5109](https://github.com/valkey-io/valkey/pull/5109) ([tests](tests/valkey/5109-13d7ed2aa/)): Free Lua function name and description with ValkeyModule_FreeString (#5109): 1 of 1 test(s) reached the change
- [valkey-io/valkey#5111](https://github.com/valkey-io/valkey/pull/5111) ([tests](tests/valkey/5111-88089c54e/)): Fix clang-format in the VM_CallArgv doc comment (#5111): declined: The change edits a single line of a documentation comment above VM_CallArgv in src/module.c, correcting indentation for clang-format. No executable code, API behaviour, reply, or build configuration is altered, so there is nothing a scenario could exercise.
- [valkey-io/valkey#3911](https://github.com/valkey-io/valkey/pull/3911) ([tests](tests/valkey/3911-20f640930/)): Make VM_CallArgv() API public (#3911): 4 of 6 test(s) reached the change, coverage 74%; [check, introduced by #3911](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/60)
- [valkey-io/valkey#5091](https://github.com/valkey-io/valkey/pull/5091) ([tests](tests/valkey/5091-5a70cde9e/)): Fix VM_Call crash on script-mode validation errors (#5091): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5092](https://github.com/valkey-io/valkey/pull/5092) ([tests](tests/valkey/5092-3aa4cbce4/)): Count expired but unreaped hash fields in objectComputeSize (#5092): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5046](https://github.com/valkey-io/valkey/pull/5046) ([tests](tests/valkey/5046-f2ca837ed/)): Fix memory leak in GetCommandKeysWithFlags (#5046): declined: The fix adds getKeysFreeResult() to VM_GetCommandKeysWithFlags, a module-only API that leaks the heap-allocated getKeysResult.keys array only when a command produces more than 256 keys (MAX_KEYS_BUFFER). It is reachable exclusively through RM_GetCommandKeys/WithFlags, which among all test 
- [valkey-io/valkey#4877](https://github.com/valkey-io/valkey/pull/4877) ([tests](tests/valkey/4877-3192cf694/)): Block DEBUG RELOAD and DEBUG LOADAOF in loading and async-loading (#4877): 2 of 8 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4995](https://github.com/valkey-io/valkey/pull/4995) ([tests](tests/valkey/4995-faec379ec/)): Fix GEOSEARCH repeated BYPOLYGON memory leak (#4995): declined: Pure memory-leak fix: repeated GEOSEARCH/GEOSEARCHSTORE BYPOLYGON now frees the earlier polygon-vertex allocation before re-allocating. Both builds already accept repeated BYPOLYGON and use the last polygon, so all replies, errors, and stored data are identical before and after the fix; th
- [valkey-io/valkey#4863](https://github.com/valkey-io/valkey/pull/4863) ([tests](tests/valkey/4863-255018c00/)): Fix module blocking of RESP3 Pub/Sub clients (#4863): 0 of 1 test(s) reached the change, coverage 100%; [check, introduced by #4863](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/58)
- [valkey-io/valkey#4655](https://github.com/valkey-io/valkey/pull/4655) ([tests](tests/valkey/4655-ee247dbd4/)): [Bug Fix] Fix fast-fail double-counting a NACK voter that later fails (#4655): 0 of 5 test(s) reached the change, coverage 95%, 1 skipped (required capability 'host' is unsupported)
- [valkey-io/valkey#4954](https://github.com/valkey-io/valkey/pull/4954) ([tests](tests/valkey/4954-25d3b7d9e/)): Match listpack for lex range commands on mixed-score btree zsets (#4954): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4886](https://github.com/valkey-io/valkey/pull/4886) ([tests](tests/valkey/4886-938007fd9/)): Only warn about a command line password when -a or -u set it (#4886): 1 of 1 test(s) reached the change, coverage 100%

### valkey-io/valkey-bloom

[2026-11-05T14:04:19+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42035755074): 9 merge(s) tested, 2 without a code change.

- [valkey-io/valkey-bloom@b11970ee2c0a](https://github.com/valkey-io/valkey-bloom/commit/b11970ee2c0a78cc2bf5e17a0ccb5af9a2888761) ([tests](tests/valkey-bloom/b11970ee2/)): Add support for BF.MEXISTS: 3 of 3 test(s) reached the change
- [valkey-io/valkey-bloom@778c219cf655](https://github.com/valkey-io/valkey-bloom/commit/778c219cf655e084091911757a2ad79e9040988c) ([tests](tests/valkey-bloom/778c219cf/)): Add support for BF.INFO: 5 of 5 test(s) reached the change; check, introduced and since fixed (recorded)
- [valkey-io/valkey-bloom@2db0e4b437e3](https://github.com/valkey-io/valkey-bloom/commit/2db0e4b437e3914a114b2379708f273ef4a864bb) ([tests](tests/valkey-bloom/2db0e4b43/)): Add support for BF.RESERVE and expansion config: 2 of 5 test(s) reached the change; crash, introduced and since fixed (recorded); check, predates 2db0e4b437e3, since fixed (recorded)
- [valkey-io/valkey-bloom@be80d9d4bab8](https://github.com/valkey-io/valkey-bloom/commit/be80d9d4bab85a82fbded5c455f0dd23e76e6079) ([tests](tests/valkey-bloom/be80d9d4b/)): Create Module Datatype and support RDB load, save, free: 4 of 4 test(s) reached the change; crash, fixed by be80d9d4bab8 (recorded)
- [valkey-io/valkey-bloom@17295202f3bd](https://github.com/valkey-io/valkey-bloom/commit/17295202f3bd26e1d1c7e0015a165ec76cb23952) ([tests](tests/valkey-bloom/17295202f/)): Add Dev Profile in Cargo.toml for debugging: declined: The change only adds a [profile.dev] section to Cargo.toml (opt-level 0, debug 2, debug-assertions true), which merely restates Cargo's default dev-profile settings to aid debugging. No source code, command behaviour, data format, replication or persistence path of the valkey-bloom module 
- [valkey-io/valkey-bloom@8557b557c393](https://github.com/valkey-io/valkey-bloom/commit/8557b557c3936d49b061949265ac23cdcbceb7ee) ([tests](tests/valkey-bloom/8557b557c/)): Remove older Bloom APIs using prev serialization apporach: 2 of 4 test(s) reached the change; crash, introduced and since fixed (recorded); crash, fixed by 8557b557c393 (recorded); crash, fixed by 8557b557c393 (recorded); crash, fixed by 8557b557c393 (recorded); crash, predates 8557b557c393, since fixed (recorded)
- [valkey-io/valkey-bloom@5ebc1cbe42d9](https://github.com/valkey-io/valkey-bloom/commit/5ebc1cbe42d95dc08069f1accda719136ca1cb89): to retry, Claude did not return a scenarios list
- [valkey-io/valkey-bloom@a96c08acc6bf](https://github.com/valkey-io/valkey-bloom/commit/a96c08acc6bfba9c95e7a7921d9e068472c05019) ([tests](tests/valkey-bloom/a96c08acc/)): Add support for BF.ADD, BF.EXISTS, BF.CARD: 4 of 5 test(s) reached the change; crash, introduced and since fixed (recorded); crash, introduced and since fixed (recorded); crash, fixed by a96c08acc6bf (recorded); crash, introduced and since fixed (recorded); crash, introduced and since fixed (recorded)
- [valkey-io/valkey-bloom@5237a1008041](https://github.com/valkey-io/valkey-bloom/commit/5237a10080413fc9f1082e7083f167c4aaadcb1f): to retry, `if [ ! -f Cargo.lock ]; then` failed in parent (exit 101): error: could not find `Cargo.toml` in `/home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/parent` or any parent directory

### valkey-io/valkey-json

[2026-11-05T14:07:21+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42041854646): 0 merge(s) tested, 0 without a code change.


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

- Merges whose tests reached the change: 159 of 197 tested (81%)
- Changed-line coverage: median 100% of 60 merge(s) measured, 4 under 50%
- Recall: 38 parents flagged of 48 fix merges
- Claude: $1167.74 in the last 24 hours, $1167.74 in the last 7 days, $1167.74 in all
- Issues filed: 19
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

## Retries

- [valkey-io/valkey-bloom@5ebc1cbe42d9](https://github.com/valkey-io/valkey-bloom/commit/5ebc1cbe42d95dc08069f1accda719136ca1cb89): attempt 1 of 3, Claude did not return a scenarios list
- [valkey-io/valkey-bloom@5237a1008041](https://github.com/valkey-io/valkey-bloom/commit/5237a10080413fc9f1082e7083f167c4aaadcb1f): attempt 1 of 3, `if [ ! -f Cargo.lock ]; then` failed in parent (exit 101): error: could not find `Cargo.toml` in `/home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/parent` or any parent directory
- [valkey-io/valkey-search#1083](https://github.com/valkey-io/valkey-search/pull/1083): attempt 0 of 3, the job left no result
- [valkey-io/valkey#5173](https://github.com/valkey-io/valkey/pull/5173): attempt 0 of 3, the job left no result
