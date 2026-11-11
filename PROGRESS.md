# Fuzzer walk

Updated 2026-11-11T03:56+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1654 | 868 | #1009 (2024-11-11) | 258 | 236.3 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 64 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.1 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 1 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-11T02:16:57+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42929188310): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey#1298](https://github.com/valkey-io/valkey/pull/1298) ([tests](tests/valkey/1298-b486a4150/)): Preserve original fd blocking state in TLS I/O operations (#1298): declined: The change only alters TLS synchronous I/O (connTLSSyncWrite/Read/ReadLine in src/tls.c, compiled only with BUILD_TLS and exercised only on a TLS connection doing blocking I/O, such as a TLS replication handshake or MIGRATE over TLS). The fuzzer cannot open any TLS connection: builds are m
- [valkey-io/valkey#1254](https://github.com/valkey-io/valkey/pull/1254) ([tests](tests/valkey/1254-6038eda01/)): Make FUNCTION RESTORE FLUSH flush async based on lazyfree-lazy-user-flush (#1254): 0 of 4 test(s) reached the change, coverage 90%; crash, introduced and since fixed (recorded); crash, introduced and since fixed (recorded)
- [valkey-io/valkey#1332](https://github.com/valkey-io/valkey/pull/1332) ([tests](tests/valkey/1332-f553ccbda/)): Use goto to cleanup error handling in readSyncBulkPayload (#1332): 0 of 3 test(s) reached the change, coverage 33%
- [valkey-io/valkey#1185](https://github.com/valkey-io/valkey/pull/1185) ([tests](tests/valkey/1185-498631094/)): Import-mode: Avoid expiration and eviction during data syncing (#1185): 4 of 7 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1305](https://github.com/valkey-io/valkey/pull/1305) ([tests](tests/valkey/1305-ee386c92f/)): Manual failover vote is not limited by two times the node timeout (#1305): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1320](https://github.com/valkey-io/valkey/pull/1320) ([tests](tests/valkey/1320-132798b57/)): Receipt of REPLCONF VERSION reply should be triggered by event (#1320): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1310](https://github.com/valkey-io/valkey/pull/1310): to retry, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#1318](https://github.com/valkey-io/valkey/pull/1318) ([tests](tests/valkey/1318-f9d0b8762/)): Upgrade macos-12 to macos-13 in workflows (#1318): declined: The change edits only GitHub Actions workflow files (.github/workflows/daily.yml and deps/hiredis/.github/workflows/build.yml), replacing the macos-12 runner image with macos-13. It is a pure CI configuration change with no modification to Valkey server, replication, persistence, tools, or
- [valkey-io/valkey#1199](https://github.com/valkey-io/valkey/pull/1199) ([tests](tests/valkey/1199-c5012cc63/)): Optimize RDB load performance and fix cluster mode resizing on replica side (#1199): 2 of 7 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1314](https://github.com/valkey-io/valkey/pull/1314) ([tests](tests/valkey/1314-d07674fc0/)): Fix sds unittest tests to check for zmalloc_usable_size (#1314): declined: The change modifies only unit-test files (src/unit/test_sds.c and src/unit/test_zmalloc.c): it switches test assertions from s_malloc_size to s_malloc_usable_size and adds a diagnostic TEST_PRINT_INFO line. No server, replication, persistence, command, or tool code changes, so no running v
- [valkey-io/valkey#1271](https://github.com/valkey-io/valkey/pull/1271) ([tests](tests/valkey/1271-94113fde7/)): Improvements for TLS with I/O threads (#1271): declined: The change is confined to src/tls.c: it fixes main-thread/I/O-thread races on SSL objects (caching SSL_pending() in a new TLS_CONN_FLAG_HAS_PENDING flag set at each SSL_read, narrowing ERR_clear_error() to just before each SSL call, and running updatePendingData on write events too). Every
- [valkey-io/valkey#1297](https://github.com/valkey-io/valkey/pull/1297) ([tests](tests/valkey/1297-86f33ea2b/)): Unprotect rdb channel when bgsave child fails in dual channel replication (#1297): 0 of 5 test(s) reached the change, coverage 0%
- [valkey-io/valkey#1131](https://github.com/valkey-io/valkey/pull/1131) ([tests](tests/valkey/1131-92181b679/)): Fix primary crash when processing dirty slots during shutdown wait / failover wait / client pause (#1131): 2 of 4 test(s) reached the change, coverage 100%; crash, fixed by #1131 (recorded)
- [valkey-io/valkey#1227](https://github.com/valkey-io/valkey/pull/1227) ([tests](tests/valkey/1227-4e2493e5c/)): Kill diskless fork child asap when the last replica drop (#1227): 0 of 8 test(s) reached the change, coverage 88%
- [valkey-io/valkey#1247](https://github.com/valkey-io/valkey/pull/1247) ([tests](tests/valkey/1247-b9994030e/)): Log clusterbus handshake timeout failures (#1247): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1281](https://github.com/valkey-io/valkey/pull/1281) ([tests](tests/valkey/1281-32f7541fe/)): Simplify dictType callbacks and move some macros from dict.h to dict.c (#1281): declined: Pure mechanical refactor: the change removes the unused `dict *d` parameter from the dictType keyDup/keyCompare/keyDestructor/valDestructor callbacks and updates all implementations and call sites; every affected callback previously began with UNUSED(d), so none ever used the parameter. Th
- [valkey-io/valkey#1296](https://github.com/valkey-io/valkey/pull/1296) ([tests](tests/valkey/1296-863d31280/)): Fix link-time optimization to work correctly for unit tests (i.e. -flto flag) (#1290) (#1296): declined: The change is build-system only: it adds -ffat-lto-objects to the GCC LTO optimization flags in src/Makefile so unit-test binaries link correctly under -flto=auto. No server code, command behavior, replication, persistence, or reply format changes; there is no runtime path a fuzzer scenari
- [valkey-io/valkey#515](https://github.com/valkey-io/valkey/pull/515) ([tests](tests/valkey/515-4a9864206/)): Migrate quicklist unit test to new framework (#515): declined: The change migrates the quicklist unit tests from the legacy SERVER_TEST embedded framework to the new unit-test framework: it deletes #ifdef SERVER_TEST test code from quicklist.c and server.c, adds src/unit/test_quicklist.c, and updates the Makefile and CI. The only lines that newly comp
- [valkey-io/valkey#1301](https://github.com/valkey-io/valkey/pull/1301) ([tests](tests/valkey/1301-6fba747c3/)): Fix log printing always shows the role as child under daemonize (#1301): declined: The change only resets server.pid after daemonize() so that log lines from a daemonized server show role M/S instead of the stale 'child' role (and so startSaving() fires the correct module persistence subevent). The differing behaviour exists only when the server runs with 'daemonize yes'
- [valkey-io/valkey#1285](https://github.com/valkey-io/valkey/pull/1285) ([tests](tests/valkey/1285-2df56d87c/)): Fix empty primary may have dirty slots data due to bad migration (#1285): 2 of 3 test(s) reached the change, coverage 80%

### valkey-io/valkey-bloom

[2026-11-11T03:38:32+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42954038396): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-11T03:54:49+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42955748277): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-11T03:56:23+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42955900220): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 777 of 1220 tested (64%)
- Changed-line coverage: median 100% of 748 merge(s) measured, 60 under 50%
- Recall: 207 parents flagged of 327 fix merges
- Claude: $924.73 in the last 24 hours, $8914.80 in the last 7 days, $8914.80 in all
- Issues filed: 67
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

## Retries

- [valkey-io/valkey#1948](https://github.com/valkey-io/valkey/pull/1948): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#1535](https://github.com/valkey-io/valkey/pull/1535): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#1310](https://github.com/valkey-io/valkey/pull/1310): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
