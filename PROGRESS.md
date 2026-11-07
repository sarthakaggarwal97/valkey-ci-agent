# Fuzzer walk

Updated 2026-11-07T00:06+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 456 | 2043 | #3772 (2026-06-29) | 223 | 65.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 334 | 334 | #667 (2026-02-13) | 209 | 47.7 |

## Last runs

### valkey-io/valkey

[2026-11-06T23:25:50+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42296666154): 20 merge(s) tested, 1 without a code change.

- [valkey-io/valkey#5310](https://github.com/valkey-io/valkey/pull/5310) ([tests](tests/valkey/5310-a77ca2249/)): Correct the `since` version on SISMEMBER XX to 9.2.0 (#5310): 2 of 2 test(s) reached the change
- [valkey-io/valkey#3362](https://github.com/valkey-io/valkey/pull/3362): to retry, Claude Code failed (rc=1): timeout after 600s
- [valkey-io/valkey#3581](https://github.com/valkey-io/valkey/pull/3581) ([tests](tests/valkey/3581-8d7e713d3/)): Add libbacktrace fallback for stack traces on musl/Alpine (#3581): declined: The change only adds a libbacktrace stack-trace fallback gated behind the USE_LIBBACKTRACE compile-time macro, which defaults to 'no' in the Makefile and is set only for musl/Alpine builds the fuzzer does not produce. On the standard glibc build the fuzzer compiles, HAVE_EXECINFO is define
- [valkey-io/valkey#4098](https://github.com/valkey-io/valkey/pull/4098) ([tests](tests/valkey/4098-5bcc9f744/)): Validate node ID when decoding open slots RDB aux field (#4098): declined: The change only alters clusterDecodeOpenSlotsAuxField, which returns early unless RDBFLAGS_REPLICATION is set, so it runs solely during a live full sync and never for an RDB file, DEBUG RELOAD, or valkey-check-rdb. The aux field's only producer is clusterEncodeOpenSlotsAuxField, which alwa
- [valkey-io/valkey#4086](https://github.com/valkey-io/valkey/pull/4086) ([tests](tests/valkey/4086-4b723ba7a/)): Avoid refreshing daylight-saving info on every event-loop wakeup (#4086): declined: Pure performance change with no externally observable behaviour: the diff only changes how often server.daylight_active is refreshed via localtime_r (every event-loop wakeup before, once per second in serverCron plus whileBlockedCron after). That flag is used solely to render DST in log ti
- [valkey-io/valkey#4101](https://github.com/valkey-io/valkey/pull/4101) ([tests](tests/valkey/4101-b2a8911c7/)): Fix gossip corruption debug log to print the offending entry (#4101): declined: The change only fixes which bytes a LL_WARNING debug log prints when a gossip entry carries an invalid node ID (g+i instead of g in verifyGossipSectionNodeIds). No reply, data, cluster-state, or control-flow behaviour changes: the invalid-ID count, the link drop, and the bounds assert are 
- [valkey-io/valkey#4089](https://github.com/valkey-io/valkey/pull/4089) ([tests](tests/valkey/4089-156a01e41/)): Save cluster config when node address is updated via gossip (#4089): 0 of 3 test(s) reached the change, coverage 0%
- [valkey-io/valkey#4099](https://github.com/valkey-io/valkey/pull/4099) ([tests](tests/valkey/4099-db7193bbc/)): Fix DEBUG HELP typo and add missing subcommand entry (#4099): 2 of 2 test(s) reached the change
- [valkey-io/valkey#4053](https://github.com/valkey-io/valkey/pull/4053) ([tests](tests/valkey/4053-ca4927ec4/)): Optimize sendBulkToReplica to reduce lseek syscalls (#4053): 0 of 6 test(s) reached the change, coverage 11%
- [valkey-io/valkey#4072](https://github.com/valkey-io/valkey/pull/4072) ([tests](tests/valkey/4072-debe5c500/)): Share pubsub pattern and watched key across clients (#4072): declined: no proposed test validated
- [valkey-io/valkey#4055](https://github.com/valkey-io/valkey/pull/4055) ([tests](tests/valkey/4055-3c1233c21/)): Validate cluster-announce-ip is an IP address or hostname (#4055): declined: no proposed test validated
- [valkey-io/valkey#4044](https://github.com/valkey-io/valkey/pull/4044) ([tests](tests/valkey/4044-aa940b6f4/)): Batch RESP frame in replicationFeedReplicas (#4044): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4077](https://github.com/valkey-io/valkey/pull/4077) ([tests](tests/valkey/4077-51543c01f/)): Drop the link when gossip section contains invalid node IDs (#4077): 0 of 1 test(s) reached the change, coverage 50%
- [valkey-io/valkey#4064](https://github.com/valkey-io/valkey/pull/4064) ([tests](tests/valkey/4064-64c73aacd/)): Log certificate errors in isCertValid (OpenSSL 4.0 only) (#4064): declined: The change only alters server log messages inside isCertValid (src/tls.c): when X509_check_certificate_times() fails it now logs a specific message for an invalid notBefore/notAfter field instead of a numeric error code. The function returns false in exactly the same cases as before, so TL
- [valkey-io/valkey#3149](https://github.com/valkey-io/valkey/pull/3149) ([tests](tests/valkey/3149-8eca03b00/)): Preparations to make serverObject (robj) opaque (#3149): 0 of 21 test(s) reached the change, coverage 59%, 1 skipped (required capability 'build' is unsupported); [crash, predates #3149](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/85)
- [valkey-io/valkey#4068](https://github.com/valkey-io/valkey/pull/4068) ([tests](tests/valkey/4068-af284c5f4/)): Fix sentinel failover coordinated segfault when old leader's client is disconnected (#4068): declined: The change is a NULL-check in sentinelKillClients in src/sentinel.c, code that executes only inside a Valkey Sentinel process during a coordinated failover (SENTINEL FAILOVER ... COORDINATED) when the Sentinel's command link to the old primary is disconnected. The fuzzer cannot reach this 
- [valkey-io/valkey#4065](https://github.com/valkey-io/valkey/pull/4065) ([tests](tests/valkey/4065-58cba3be7/)): Validate node ID and shard ID from network messages before applying (#4065): 0 of 2 test(s) reached the change, coverage 45%
- [valkey-io/valkey#4020](https://github.com/valkey-io/valkey/pull/4020) ([tests](tests/valkey/4020-406fe5431/)): Fix assert with CLUSTER REPLICATE for replicas that own slots (#4020): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#4002](https://github.com/valkey-io/valkey/pull/4002) ([tests](tests/valkey/4002-14624e461/)): Optimize modules dict iteration with early return when empty (#4002): declined: The change only adds early returns when the modules dict is empty, in front of loops that iterate it. For every touched function the early return yields exactly what the zero-iteration loop already produced: NULL from the type lookups (the byID cache cannot be populated while the dict is e
- [valkey-io/valkey#4073](https://github.com/valkey-io/valkey/pull/4073) ([tests](tests/valkey/4073-eefa8927d/)): fix: Reject corrupt stream RDB with shared NACK across consumers (#4073): 1 of 2 test(s) reached the change, coverage 100%; [crash, introduced by #4073](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/84)

### valkey-io/valkey-bloom

[2026-11-07T00:06:01+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42312081997): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-06T22:58:29+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42308544055): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-06T22:39:00+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42284111779): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey-search#794](https://github.com/valkey-io/valkey-search/pull/794) ([tests](tests/valkey-search/794-c7c21976a/)): FlatPosiitonMap::ReadCounts Optimization (#794): declined: The change replaces memcpy-based little-endian count reads in FlatPositionMap::ReadCounts with an equivalent byte-wise ReadVarUint loop and deduplicates that helper out of PositionIterator into an anonymous namespace. On the little-endian platform the fuzzer runs on, the new decode compute
- [valkey-io/valkey-search#796](https://github.com/valkey-io/valkey-search/pull/796) ([tests](tests/valkey-search/796-63eeaf926/)): Remove an allocation which is not needed in text search (#796): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#790](https://github.com/valkey-io/valkey-search/pull/790) ([tests](tests/valkey-search/790-ffdec7e08/)): Bump version to module 1.2 and add check for GetMinVersion (#790): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#786](https://github.com/valkey-io/valkey-search/pull/786) ([tests](tests/valkey-search/786-a876eed5f/)): Fix a memory leak bug in search.cc (#786): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#782](https://github.com/valkey-io/valkey-search/pull/782) ([tests](tests/valkey-search/782-bf70d7327/)): Enhance overview description, correct aggregation (#782): declined: The change modifies only documentation: markdown files under docs/ and the src/commands/*.json command-specification files. The JSON specs are not compiled, embedded, or loaded by the module (src/commands/CMakeLists.txt lists only .cc/.h sources; no source or build rule references the .jso
- [valkey-io/valkey-search#728](https://github.com/valkey-io/valkey-search/pull/728) ([tests](tests/valkey-search/728-be0f584a5/)): Sanitize module logging  (#728): 6 of 6 test(s) reached the change; crash, predates #728, since fixed (recorded)
- [valkey-io/valkey-search#747](https://github.com/valkey-io/valkey-search/pull/747) ([tests](tests/valkey-search/747-aa4832d46/)): Add THIRD_PARTY_LICENSES file (#747): declined: The change adds a THIRD_PARTY_NOTICES license file and nothing else: no source, build, or configuration file is modified, so no runtime behaviour of the valkey-search module can change. There is no path for a scenario to reach.
- [valkey-io/valkey-search#764](https://github.com/valkey-io/valkey-search/pull/764) ([tests](tests/valkey-search/764-8b04556b7/)): Fix for handling trailing punctuations in query (#764): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#742](https://github.com/valkey-io/valkey-search/pull/742) ([tests](tests/valkey-search/742-154b80cfc/)): Restructure Content Fetching to match key blocking semantics (#742): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#776](https://github.com/valkey-io/valkey-search/pull/776) ([tests](tests/valkey-search/776-6c47e4276/)): Fix config name and add check for duplicate config names (#776): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#775](https://github.com/valkey-io/valkey-search/pull/775) ([tests](tests/valkey-search/775-419c5d3e5/)): Change default RDB version to 2 (#775): 1 of 1 test(s) reached the change; crash, predates #775, since fixed (recorded)
- [valkey-io/valkey-search#740](https://github.com/valkey-io/valkey-search/pull/740) ([tests](tests/valkey-search/740-21de43864/)): untrack missing fields to properly support negate queries (#740): 0 of 3 test(s) reached the change; crash, introduced and since fixed (recorded)
- [valkey-io/valkey-search#774](https://github.com/valkey-io/valkey-search/pull/774) ([tests](tests/valkey-search/774-ff60845fc/)): Quick fix for unwanted concurrency on index_key_info_ (#774): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#772](https://github.com/valkey-io/valkey-search/pull/772) ([tests](tests/valkey-search/772-f6d08509e/)): Add VERBATIM to FT AGGREGATE (#772): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#768](https://github.com/valkey-io/valkey-search/pull/768) ([tests](tests/valkey-search/768-921a95408/)): Remove incorrect evaluation stemming assertion (#768): 0 of 2 test(s) reached the change
- [valkey-io/valkey-search#754](https://github.com/valkey-io/valkey-search/pull/754) ([tests](tests/valkey-search/754-df2edfda9/)): app metric review changes (#754): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#744](https://github.com/valkey-io/valkey-search/pull/744) ([tests](tests/valkey-search/744-a906ddceb/)): Add attribute data to FT.INFO PRIMARY and CLUSTER mode (#744): 4 of 4 test(s) reached the change
- [valkey-io/valkey-search#598](https://github.com/valkey-io/valkey-search/pull/598) ([tests](tests/valkey-search/598-ff145d2e2/)): Implement single slot indexes (resolves #141) (#598): 3 of 3 test(s) reached the change
- [valkey-io/valkey-search#644](https://github.com/valkey-io/valkey-search/pull/644) ([tests](tests/valkey-search/644-05abf6048/)): [FEATURE] Implement SKIPINITIALSCAN option on FT.CREATE command (#644): 2 of 2 test(s) reached the change
- [valkey-io/valkey-search#753](https://github.com/valkey-io/valkey-search/pull/753) ([tests](tests/valkey-search/753-7328c42cd/)): Add unit test 'pagination_offset_exceeds_remaining' (#753): declined: The change adds only a new parameterized unit test case ('pagination_offset_exceeds_remaining') to testing/ft_search_test.cc, asserting the already-existing LIMIT offset/count reply behaviour of FT.SEARCH's SendReply path. No production source is touched: no command handling, reply encodin

## Totals

- Merges whose tests reached the change: 359 of 508 tested (71%)
- Changed-line coverage: median 100% of 226 merge(s) measured, 14 under 50%
- Recall: 107 parents flagged of 151 fix merges
- Claude: $1718.02 in the last 24 hours, $3483.76 in the last 7 days, $3483.76 in all
- Issues filed: 39
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

## Retries

- [valkey-io/valkey#3362](https://github.com/valkey-io/valkey/pull/3362): attempt 2 of 3, Claude Code failed (rc=1): timeout after 600s
