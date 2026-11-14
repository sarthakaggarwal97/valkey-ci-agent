# Fuzzer walk

Updated 2026-11-14T08:18+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 2299 | 241 | #12996 (2024-02-20) | 302 | 244.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 0.0 |
| valkey-io/valkey-json | 66 | 0 | c1423d7e4b38 (2024-11-25) | 2 | 0.6 |
| valkey-io/valkey-search | 690 | 0 | f8106e72e984 (2024-11-27) | 10 | 42.4 |

## Last runs

### valkey-io/valkey

[2026-11-14T06:53:57+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43560262144): 20 merge(s) tested, 1 without a code change.

- [valkey-io/valkey#35](https://github.com/valkey-io/valkey/pull/35): to retry, 2 of 2 run(s) could not start: the run could not start: environment acquisition failed (artifacts.parent acquire: FileNotFoundError)
- [redis/redis#13115](https://github.com/redis/redis/pull/13115) ([tests](tests/valkey/13115-5fdaa53d2/)): Fix conversion of numbers in lua args to redis args (#13115): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#13112](https://github.com/redis/redis/pull/13112) ([tests](tests/valkey/13112-4979cf02f/)): Change crc16 slot table to be fixed size character array instead of pointer to strings (#13112): declined: Pure data-layout refactor: crc16_slot_table changes from an array of 16384 string pointers to a flat array of char[4] entries, with the string initializers untouched. The table's only consumer is valkey-benchmark's cluster-mode hash-tag substitution (setClusterKeyHashTag, redis-benchmark.c
- [redis/redis#13055](https://github.com/redis/redis/pull/13055) ([tests](tests/valkey/13055-28976a900/)): Fix PONG message processing for primary-ship tracking during failovers (#13055): 0 of 10 test(s) reached the change, coverage 78%
- [redis/redis#13058](https://github.com/redis/redis/pull/13058) ([tests](tests/valkey/13058-ad1273033/)): Implement defragmentation for pubsub kvstore (#13058): 0 of 5 test(s) reached the change, coverage 88%; crash, introduced and since fixed (recorded)
- [redis/redis#13101](https://github.com/redis/redis/pull/13101) ([tests](tests/valkey/13101-33ea43258/)): Call finalizerProc when free the aeTimeEvent in ae (#13101): declined: The change makes aeDeleteEventLoop call each pending time event's finalizerProc before freeing it (src/ae.c:152-153). No code in the tree registers a finalizer: every aeCreateTimeEvent call site (serverCron in server.c:2743, evictionTimeProc in evict.c:475, module timers in module.c:9244, 
- [redis/redis#13103](https://github.com/redis/redis/pull/13103) ([tests](tests/valkey/13103-df75153d7/)): Fix reply schemas validator build issue due to new regular expression (#13103): declined: The change only modifies utils/generate-command-code.py, a build-time code generator used for the reply-schemas validator CI build. It escapes backslash and plus characters in generated C identifiers and uses json.dumps for string literals so the generated code compiles when a reply schema
- [redis/redis#13097](https://github.com/redis/redis/pull/13097) ([tests](tests/valkey/13097-a50bbcb65/)): redis-cli fixes around help hints version filtering (#13097): declined: This is a valkey-cli bug fix in the legacy help-hint version-filtering path (versionIsSupported and removeUnsupportedArgs in src/redis-cli.c), reached only when the CLI compares the connected server's reported version against the command/argument `since` strings compiled into cli_commands.
- [redis/redis#12913](https://github.com/redis/redis/pull/12913) ([tests](tests/valkey/12913-4cae99e78/)): Add overhead of all DBs and rehashing dict count to info. (#12913): 4 of 6 test(s) reached the change, coverage 100%
- [redis/redis#12898](https://github.com/redis/redis/pull/12898) ([tests](tests/valkey/12898-f17381a38/)): Fix propagation of entries_read by calling streamPropagateGroupID unconditionally (#12898): 2 of 4 test(s) reached the change, coverage 100%
- [redis/redis#13098](https://github.com/redis/redis/pull/13098) ([tests](tests/valkey/13098-cc9fbd270/)): freeDictIfNeeded when kvstoreEmpty (#13098): declined: The added freeDictIfNeeded() call in kvstoreEmpty() is unreachable with effect in this checkout: freeDictIfNeeded is a no-op unless the kvstore has KVSTORE_FREE_EMPTY_DICTS, the only kvstores ever passed to kvstoreEmpty are db->keys/db->expires (db.c:498-499) which are always created witho
- [redis/redis#13087](https://github.com/redis/redis/pull/13087) ([tests](tests/valkey/13087-a7abc2f06/)): SCRIPT FLUSH run truly async, close lua interpreter in bio (#13087): 0 of 4 test(s) reached the change, coverage 100%
- [redis/redis#13092](https://github.com/redis/redis/pull/13092) ([tests](tests/valkey/13092-763827c98/)): Fix redis-cli --count (for --scan, --bigkeys, etc) was ignored unless --pattern was also used (#13092): 0 of 4 test(s) reached the change, coverage 0%
- [redis/redis#13080](https://github.com/redis/redis/pull/13080) ([tests](tests/valkey/13080-0a12f380e/)): Optimize DEL on expired keys (#13080): declined: The change is a behaviour-preserving optimization: expireIfNeeded's 0/1 return is renamed to a KEY_VALID/KEY_EXPIRED/KEY_DELETED enum with KEY_VALID=0, and every call site (lookupKey, dbRandomKey, scanGenericCommand) keeps the exact old truthiness via '!= KEY_VALID'. The only logic change,
- [redis/redis#13068](https://github.com/redis/redis/pull/13068) ([tests](tests/valkey/13068-104b20760/)): Fix size stat in malloc(0) and cleanups around zmalloc file (#13068): declined: The only behavioural line (size = MALLOC_MIN_SIZE(size) in zmalloc's prefix-size accounting) is compiled only when HAVE_MALLOC_SIZE is undefined, i.e. no jemalloc/tcmalloc/macOS and no libc malloc_usable_size; the fuzzer's Linux builds (jemalloc or glibc) never compile that path. The rest 
- [redis/redis#13086](https://github.com/redis/redis/pull/13086) ([tests](tests/valkey/13086-bfcaa7db0/)): Fix minor memory leak in rewriteSetObject (#13086): declined: The change only frees a set iterator on the rio-write-failure path of rewriteSetObject during AOF rewrite. That path is reached solely when writing the rewrite output fails (a disk I/O error), which the fuzzer cannot induce: the only disk-full fault (valkey.bgsave-disk-full) needs root and
- [redis/redis#12971](https://github.com/redis/redis/pull/12971) ([tests](tests/valkey/12971-4a265554a/)): Expose lua os.clock() api (#12971): 2 of 2 test(s) reached the change, coverage 100%
- [redis/redis#12659](https://github.com/redis/redis/pull/12659) ([tests](tests/valkey/12659-165afc5f2/)): Determine the large limit of the quicklist node based on fill (#12659): 4 of 6 test(s) reached the change, coverage 95%
- [redis/redis#13078](https://github.com/redis/redis/pull/13078) ([tests](tests/valkey/13078-820a4e45f/)): Edit the history field of xinfo-consumers (#13078): declined: The change only rewords the 7.2.0 history string of XINFO CONSUMERS in src/commands.def and src/commands/xinfo-consumers.json (documentation metadata surfaced via COMMAND DOCS). It alters no command behaviour, reply structure, parsing, replication or persistence path, so there is nothing r
- [redis/redis#13076](https://github.com/redis/redis/pull/13076) ([tests](tests/valkey/13076-5b9fc4652/)): Add new allocator.muzzy field to memory-stats reply schema (#13076): declined: The change only edits src/commands/memory-stats.json, adding the allocator.muzzy field to the MEMORY STATS reply schema. Reply schemas are documentation metadata compiled in only with the --with-reply-schema generator flag, not in normal builds. The runtime code emitting allocator.muzzy in

### valkey-io/valkey-bloom

[2026-11-14T07:47:41+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43572694435): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-14T08:18:20+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43575126961): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-14T07:51:57+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/43573009469): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 913 of 1457 tested (63%)
- Changed-line coverage: median 100% of 946 merge(s) measured, 70 under 50%
- Recall: 237 parents flagged of 375 fix merges
- Claude: $648.77 in the last 24 hours, $6528.32 in the last 7 days, $10711.82 in all
- Issues filed: 78
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
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/117
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/122
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/125
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/126
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/127
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/128
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/129
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/145
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/146
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/147
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/153
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/156
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
- Model updates: 0 asked for, 0 with no model change, 0 rejected, 0 failed; 0 pull request(s)

## Retries

- [valkey-io/valkey#35](https://github.com/valkey-io/valkey/pull/35): attempt 0 of 3, 2 of 2 run(s) could not start: the run could not start: environment acquisition failed (artifacts.parent acquire: FileNotFoundError)
