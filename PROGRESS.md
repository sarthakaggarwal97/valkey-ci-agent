# Fuzzer walk

Updated 2026-11-07T06:16+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 542 | 1963 | #3683 (2026-05-18) | 273 | 77.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 372 | 297 | #607 (2026-01-29) | 202 | 53.1 |

## Last runs

### valkey-io/valkey

[2026-11-07T04:58:35+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42331067903): 20 merge(s) tested, 12 without a code change.

- [valkey-io/valkey#3374](https://github.com/valkey-io/valkey/pull/3374): to retry, 1 of 1 run(s) could not start: the run could not start: environment acquisition failed
- [valkey-io/valkey#3658](https://github.com/valkey-io/valkey/pull/3658) ([tests](tests/valkey/3658-0ff817e8c/)): CMake: Plain and keyword can not be mixed (#3658): declined: The change is purely a build-system fix: it adds the PRIVATE keyword to target_link_libraries calls in the CMake files so plain and keyword signatures are not mixed, which CMake requires. The set and order of linked libraries is unchanged, so the produced binaries behave identically; no co
- [valkey-io/valkey#3846](https://github.com/valkey-io/valkey/pull/3846) ([tests](tests/valkey/3846-f25ec5e40/)): Fix use-after-free in VM_RegisterClusterMessageReceiver (#3846): 1 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3872](https://github.com/valkey-io/valkey/pull/3872) ([tests](tests/valkey/3872-2035fb30c/)): Redacting customer information when hide_user_data_from_log is true in rdb.c, networking.c, debug.c and t_hash  (#3872): declined: Every hunk only alters the text of a server log message (serverLog/serverLogHexDump/rdbReportCorruptRDB) based on hide-user-data-from-log, redacting the user's key name, consumer-group name, query-buffer bytes, or listpack hexdump. The surrounding control flow is identical on both branches
- [valkey-io/valkey#3721](https://github.com/valkey-io/valkey/pull/3721) ([tests](tests/valkey/3721-e4fdae45a/)): Force listpack validation on load to prevent deferred assertions (#3721): 10 of 14 test(s) reached the change, coverage 87%; crash, fixed by #3721 (recorded)
- [valkey-io/valkey#3676](https://github.com/valkey-io/valkey/pull/3676) ([tests](tests/valkey/3676-8ee3fd704/)): Trigger cluster state update immediately when cluster-require-full-coverage changes (#3676): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3827](https://github.com/valkey-io/valkey/pull/3827) ([tests](tests/valkey/3827-1c49ca05c/)): Trigger failover check ASAP when cluster-replica-no-failover is disabled (#3827): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3407](https://github.com/valkey-io/valkey/pull/3407) ([tests](tests/valkey/3407-42765b2ff/)): Fix integer overflow in WAIT and WAITAOF numreplicas parameter (#3407): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3791](https://github.com/valkey-io/valkey/pull/3791) ([tests](tests/valkey/3791-cce8f49b6/)): Restrict zmadvise_dontneed_range() to jemalloc builds (#3791): declined: The change only narrows a preprocessor condition so madvise(MADV_DONTNEED) in zmadvise_dontneed_range() compiles on jemalloc Linux builds instead of all Linux builds. Its sole caller, dismissRehashedBucketsIfNeeded() in hashtable.c, releases pages of already-rehashed, dead hashtable bucket
- [valkey-io/valkey#3808](https://github.com/valkey-io/valkey/pull/3808) ([tests](tests/valkey/3808-c59b24713/)): Document database-level ACL rules in valkey.conf and acl.c (#3808): declined: The change edits only comments: doc comments in src/acl.c describing the db=/alldbs/resetdbs ACL rules and the reset action, and the matching commented documentation text in valkey.conf. No executable code, parsing, or reply behaviour changes, so there is nothing a scenario could exercise.
- [valkey-io/valkey#3804](https://github.com/valkey-io/valkey/pull/3804) ([tests](tests/valkey/3804-355d30642/)): Remove MIGRATE command dead code in ACLSelectorCheckCmd (#3804): declined: The change deletes an unreachable branch: the removed `cmd->proc == migrateCommand` case is inside the `if (cmd->get_dbid_args)` block in ACLSelectorCheckCmd, and MIGRATE does not define get_dbid_args in migrate.json (only SELECT, MOVE, SWAPDB and COPY do), so the branch could never execut
- [valkey-io/valkey#3796](https://github.com/valkey-io/valkey/pull/3796) ([tests](tests/valkey/3796-604dcdb00/)): hashtable: disallow insert during safe iteration (fixes #2302) (#3796): declined: The change adds an internal invariant assertion (assert(ht->safe_iterators == NULL) in hashtable.c's insert()), a doc rewrite, and a rewritten unit test. It forbids inserting into a hashtable during safe iteration; delete/replace are unchanged. No command-reachable path in this build viola
- [valkey-io/valkey#3787](https://github.com/valkey-io/valkey/pull/3787) ([tests](tests/valkey/3787-8d3e6c306/)): Fix "time_t' different  typedef  on 32-bit /64-bit systems (#3787): declined: The change only corrects printf format specifiers for time_t/long on 32-bit builds (valkey-check-aof messages, INFO lru_clock cast, internal fuzzer generator) and adds a 32-bit Alpine CI job. On the 64-bit platform the fuzzer runs on, time_t is a 64-bit long, so %ld and %jd with an intmax_
- [valkey-io/valkey#3723](https://github.com/valkey-io/valkey/pull/3723) ([tests](tests/valkey/3723-d778b3771/)): Fix double-finish and RESP reply violation in cluster slot migration (#3723): 4 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): to retry, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3756](https://github.com/valkey-io/valkey/pull/3756) ([tests](tests/valkey/3756-f2fe917bb/)): Revert "IO-Threads redesign cleanup work (#3544)" (#3756): 0 of 12 test(s) reached the change, coverage 76%
- [valkey-io/valkey#3678](https://github.com/valkey-io/valkey/pull/3678) ([tests](tests/valkey/3678-294680700/)): Restore ERR prefix and error-code preservation in Lua scripting (#3678): 2 of 2 test(s) reached the change
- [valkey-io/valkey#3743](https://github.com/valkey-io/valkey/pull/3743) ([tests](tests/valkey/3743-bfeb3685e/)): Fix buffered_reply assert in HFE commands with module keyspace notifications (#3743): declined: The change adds initDeferredReplyBuffer/commitDeferredReplyBuffer around the reply-building in HGETDEL, the hexpireGenericCommand family, and HPERSIST (src/t_hash.c) to defer the array reply when a module blocks the client inside a keyspace-notification callback. Both helpers are strict no
- [valkey-io/valkey#3505](https://github.com/valkey-io/valkey/pull/3505) ([tests](tests/valkey/3505-69bd02b2e/)): Extend CAPA REDIRECT to cover keyless commands on cluster replicas (#3505): 3 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#3654](https://github.com/valkey-io/valkey/pull/3654) ([tests](tests/valkey/3654-5a604b3ef/)): Use full hash-seed bytes when deriving SipHash seed (#3654): 0 of 4 test(s) reached the change, coverage 100%

### valkey-io/valkey-bloom

[2026-11-07T06:16:54+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42346258293): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T05:47:34+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42344152322): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-07T05:20:38+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42324778733): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey-search#741](https://github.com/valkey-io/valkey-search/pull/741): to retry, `deps=/opt/valkey-search-deps` failed in parent (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/command
- [valkey-io/valkey-search#735](https://github.com/valkey-io/valkey-search/pull/735): to retry, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#734](https://github.com/valkey-io/valkey-search/pull/734): to retry, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [137/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#621](https://github.com/valkey-io/valkey-search/pull/621): to retry, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [137/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#707](https://github.com/valkey-io/valkey-search/pull/707) ([tests](tests/valkey-search/707-aa8261ef0/)): Implement WEIGHT text field option, require a 1.0 value (#707): 4 of 4 test(s) reached the change; crash, predates #707, since fixed (recorded)
- [valkey-io/valkey-search#622](https://github.com/valkey-io/valkey-search/pull/622) ([tests](tests/valkey-search/622-69658d902/)): Introduce ShardedAtomic for zero-contention, high-concurrency counters (#622): declined: no proposed test validated
- [valkey-io/valkey-search#701](https://github.com/valkey-io/valkey-search/pull/701) ([tests](tests/valkey-search/701-300fe700c/)): Stem search terms regardless of stemsize - Bug fix (#701): 2 of 2 test(s) reached the change; crash, fixed by #701 (recorded); crash, fixed by #701 (recorded)
- [valkey-io/valkey-search#699](https://github.com/valkey-io/valkey-search/pull/699) ([tests](tests/valkey-search/699-014309dd7/)): Fix error message to match parsing logic (#699): 2 of 2 test(s) reached the change; crash, predates #699, since fixed (recorded)
- [valkey-io/valkey-search#688](https://github.com/valkey-io/valkey-search/pull/688) ([tests](tests/valkey-search/688-fe6c99b31/)): Rax Integration (#688): 0 of 4 test(s) reached the change; crash, predates #688, since fixed (recorded)
- [valkey-io/valkey-search#668](https://github.com/valkey-io/valkey-search/pull/668) ([tests](tests/valkey-search/668-e73481fd9/)): Add Memory Tracking for Index Schema Attributes (#668): 4 of 4 test(s) reached the change; crash, predates #668, since fixed (recorded)
- [valkey-io/valkey-search#679](https://github.com/valkey-io/valkey-search/pull/679) ([tests](tests/valkey-search/679-24d9b083e/)): Refactor Dockerfile to Reduce Image Layers and Size (#679): declined: The change modifies only .devcontainer/Dockerfile, merging apt install steps into fewer layers and cleaning caches to shrink the dev-container image. It is purely a build/CI environment change: no module source, command handling, indexing, replication or persistence code changes, so the lo
- [valkey-io/valkey-search#670](https://github.com/valkey-io/valkey-search/pull/670) ([tests](tests/valkey-search/670-a1b376933/)): Fix misleading error message when query string is error or not support. (#670): 2 of 2 test(s) reached the change; crash, predates #670, since fixed (recorded)
- [valkey-io/valkey-search#608](https://github.com/valkey-io/valkey-search/pull/608) ([tests](tests/valkey-search/608-d300acff7/)): Endurance failover (#608): declined: The change modifies only valkey-search's own integration test harness (testing/integration/stability_runner.py, stability_test.py, utils.py), adding failover coordination to its Python stability tests. No module source code changes, so the built module's behaviour is identical at the chang
- [valkey-io/valkey-search#669](https://github.com/valkey-io/valkey-search/pull/669) ([tests](tests/valkey-search/669-13bea40e9/)): Solving text completely in entries fetcher (#669): 4 of 4 test(s) reached the change; crash, predates #669, since fixed (recorded)
- [valkey-io/valkey-search#630](https://github.com/valkey-io/valkey-search/pull/630) ([tests](tests/valkey-search/630-06ea5f2a1/)): Add fulltext metrics to info (#630): declined: no proposed test validated
- [valkey-io/valkey-search#677](https://github.com/valkey-io/valkey-search/pull/677) ([tests](tests/valkey-search/677-1772b56d5/)): Revert "Rax Integration (#605)" (#677): 0 of 4 test(s) reached the change; crash, predates #677, since fixed (recorded)
- [valkey-io/valkey-search#605](https://github.com/valkey-io/valkey-search/pull/605) ([tests](tests/valkey-search/605-ab8da9c75/)): Rax Integration (#605): 0 of 4 test(s) reached the change; crash, predates #605, since fixed (recorded); crash, introduced and since fixed (recorded)
- [valkey-io/valkey-search#614](https://github.com/valkey-io/valkey-search/pull/614) ([tests](tests/valkey-search/614-ae0f8f095/)): Stemming Ingestion and Search  (resolves #651) (#614): 4 of 4 test(s) reached the change; crash, predates #614, since fixed (recorded)
- [valkey-io/valkey-search#647](https://github.com/valkey-io/valkey-search/pull/647) ([tests](tests/valkey-search/647-e9571671c/)): Move MINSTEMSIZE clause from per text field to per schema (#647): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#641](https://github.com/valkey-io/valkey-search/pull/641) ([tests](tests/valkey-search/641-2fc6219c2/)): Revert "Update Vector HNSW output field format (#312)" (#641): 2 of 2 test(s) reached the change; crash, predates #641, since fixed (recorded)

## Totals

- Merges whose tests reached the change: 398 of 564 tested (71%)
- Changed-line coverage: median 100% of 254 merge(s) measured, 16 under 50%
- Recall: 118 parents flagged of 165 fix merges
- Claude: $1737.27 in the last 24 hours, $3934.23 in the last 7 days, $3934.23 in all
- Issues filed: 41
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

## Retries

- [valkey-io/valkey-search#741](https://github.com/valkey-io/valkey-search/pull/741): attempt 2 of 3, `deps=/opt/valkey-search-deps` failed in parent (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/command
- [valkey-io/valkey-search#735](https://github.com/valkey-io/valkey-search/pull/735): attempt 2 of 3, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#734](https://github.com/valkey-io/valkey-search/pull/734): attempt 2 of 3, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [137/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#621](https://github.com/valkey-io/valkey-search/pull/621): attempt 2 of 3, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [137/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey#3374](https://github.com/valkey-io/valkey/pull/3374): attempt 2 of 3, 1 of 1 run(s) could not start: the run could not start: environment acquisition failed
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
