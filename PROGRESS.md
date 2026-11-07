# Fuzzer walk

Updated 2026-11-07T05:16+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 542 | 1963 | #3683 (2026-05-18) | 273 | 77.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 351 | 318 | #707 (2026-02-05) | 181 | 50.1 |

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

[2026-11-07T05:15:26+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42341904179): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-07T05:16:38+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42341991342): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-07T01:34:15+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42306322056): 20 merge(s) tested, 1 without a code change.

- [valkey-io/valkey-search#1546](https://github.com/valkey-io/valkey-search/pull/1546) ([tests](tests/valkey-search/1546-6e403afa1/)): feat(vector-range): add epsilon-aware HNSW range traversal (#1546): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#667](https://github.com/valkey-io/valkey-search/pull/667) ([tests](tests/valkey-search/667-fa3ae057c/)): Raise limit on number of attributes. Correct misnamed max-vector-attribute (#667): 5 of 5 test(s) reached the change
- [valkey-io/valkey-search#749](https://github.com/valkey-io/valkey-search/pull/749) ([tests](tests/valkey-search/749-b383d752f/)): Make fanout async threshold configurable (#749): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#746](https://github.com/valkey-io/valkey-search/pull/746) ([tests](tests/valkey-search/746-311be1eb3/)): Use consistent + performant casefolding logic in both query/ingestion (#746): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#711](https://github.com/valkey-io/valkey-search/pull/711) ([tests](tests/valkey-search/711-da4e28f2d/)): Hybrid vector text query integration test (#711): declined: The change is test-and-CI only: it adds hybrid vector/text query cases to integration/test_fulltext.py (refactoring existing assertions into a shared HYBRID_QUERY_EXPECTED_RESULTS table and adding a KNN variant of the same queries) and amends scripts/common.rc to pip-install an extra requi
- [valkey-io/valkey-search#743](https://github.com/valkey-io/valkey-search/pull/743) ([tests](tests/valkey-search/743-a6a71b394/)):  ftdebug to list configs in search module (#743): 0 of 2 test(s) reached the change; [crash, appeared after #743](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/87)
- [valkey-io/valkey-search#674](https://github.com/valkey-io/valkey-search/pull/674) ([tests](tests/valkey-search/674-a2c814f7d/)): Add grpc coordinator threads CPU time metric to INFO command (#674): declined: The change only does anything when the module is loaded with the --use-coordinator module argument: the ThreadGroupCPUMonitor is created in ValkeySearch::Startup only under GetUseCoordinator(), and the new INFO field search_coordinator_threads_cpu_time_sec is VisibleIf(UsingCoordinator()),
- [valkey-io/valkey-search#722](https://github.com/valkey-io/valkey-search/pull/722) ([tests](tests/valkey-search/722-e015fa874/)): Implement SLOP and INORDER in FT.AGGREGATE (#722): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#741](https://github.com/valkey-io/valkey-search/pull/741): to retry, `deps=/opt/valkey-search-deps` failed in parent (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/command
- [valkey-io/valkey-search#735](https://github.com/valkey-io/valkey-search/pull/735): to retry, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#734](https://github.com/valkey-io/valkey-search/pull/734): to retry, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [137/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#621](https://github.com/valkey-io/valkey-search/pull/621): to retry, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [136/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#618](https://github.com/valkey-io/valkey-search/pull/618) ([tests](tests/valkey-search/618-9b29188d0/)): Feature/sortby implementation (#618): 5 of 5 test(s) reached the change
- [valkey-io/valkey-search#639](https://github.com/valkey-io/valkey-search/pull/639) ([tests](tests/valkey-search/639-444fb186b/)): Increase default for allowed number of indexes to 1000 (#639): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#671](https://github.com/valkey-io/valkey-search/pull/671) ([tests](tests/valkey-search/671-cff18a5a0/)): Endurance Testing - adding more search functionality testing. (#671): declined: The change modifies only valkey-search's own integration-test harness: testing/integration/stability_runner.py (new memtier test documents, a different FT.SEARCH query mix for TEXT indexes, consolidated delete/expire memtier commands, and nostem/with_suffix_trie tweaks in the test's index 
- [valkey-io/valkey-search#731](https://github.com/valkey-io/valkey-search/pull/731) ([tests](tests/valkey-search/731-0345b6268/)): Endurance test added as a GitHub workflow (#731): declined: The change is purely CI/tooling: it adds a new GitHub workflow (.github/workflows/endurance_tests.yml), a benchmark driver script (scripts/benchmark/run_endurance_test.sh), and installs jq in the devcontainer Dockerfile. No module source, command handling, indexing, replication, or persist
- [valkey-io/valkey-search#541](https://github.com/valkey-io/valkey-search/pull/541) ([tests](tests/valkey-search/541-6bdc2dde5/)): Block Search Response on Conflicting In-Flight Keys for fulltext queries (#541): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#712](https://github.com/valkey-io/valkey-search/pull/712) ([tests](tests/valkey-search/712-945d29388/)): Ensure content is always retrieved on correct DB during search (#712): declined: no proposed test validated
- [valkey-io/valkey-search#708](https://github.com/valkey-io/valkey-search/pull/708) ([tests](tests/valkey-search/708-703331414/)): Undo the renaming of an existing config (#708): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#685](https://github.com/valkey-io/valkey-search/pull/685) ([tests](tests/valkey-search/685-cd722c45b/)): Fix OOM Handling for SearchBased ReadOnly commands (#685): 2 of 2 test(s) reached the change

## Totals

- Merges whose tests reached the change: 389 of 552 tested (70%)
- Changed-line coverage: median 100% of 254 merge(s) measured, 16 under 50%
- Recall: 115 parents flagged of 162 fix merges
- Claude: $1605.16 in the last 24 hours, $3802.11 in the last 7 days, $3802.11 in all
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

- [valkey-io/valkey-search#741](https://github.com/valkey-io/valkey-search/pull/741): attempt 1 of 3, `deps=/opt/valkey-search-deps` failed in parent (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/command
- [valkey-io/valkey-search#735](https://github.com/valkey-io/valkey-search/pull/735): attempt 1 of 3, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [138/223] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#734](https://github.com/valkey-io/valkey-search/pull/734): attempt 1 of 3, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [137/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey-search#621](https://github.com/valkey-io/valkey-search/pull/621): attempt 1 of 3, `deps=/opt/valkey-search-deps` failed in merge (exit 1): [136/221] Building CXX object src/commands/CMakeFiles/commands.dir/commands.cc.o FAILED: [code=1] src/commands/CMakeFiles/commands.dir/commands
- [valkey-io/valkey#3374](https://github.com/valkey-io/valkey/pull/3374): attempt 2 of 3, 1 of 1 run(s) could not start: the run could not start: environment acquisition failed
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
