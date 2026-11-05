# Fuzzer walk

Updated 2026-11-05T03:48+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 26 | 2462 | #5244 (2026-11-03) | 26 | 3.7 |
| valkey-io/valkey-bloom | 5 | 104 | #93 (2026-04-26) | 5 | 0.7 |
| valkey-io/valkey-json | 6 | 56 | #115 (2026-06-24) | 6 | 0.9 |
| valkey-io/valkey-search | 0 | 667 | #1634 (2026-11-03) | 0 | 0.0 |

## Last runs

### valkey-io/valkey

[2026-11-05T03:48:58+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41956206467): 20 merge(s) tested, 0 without a code change.

- [valkey-io/valkey#4711](https://github.com/valkey-io/valkey/pull/4711) ([tests](tests/valkey/4711-d2466b90a/)): Fix UAF in streams when XADD creates a new node (#4711): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5138](https://github.com/valkey-io/valkey/pull/5138) ([tests](tests/valkey/5138-4500d5824/)): Fix hllSparseSet aborting on SDS_TYPE_5 strings when deltalen is zero (#5138): 2 of 2 test(s) reached the change, coverage 100%; crash, fixed by #5138 (recorded)
- [valkey-io/valkey#5124](https://github.com/valkey-io/valkey/pull/5124) ([tests](tests/valkey/5124-99f5a75f4/)): Reject sparse HLLs that end in a truncated XZERO opcode (#5124): 2 of 2 test(s) reached the change, coverage 88%
- [valkey-io/valkey#5105](https://github.com/valkey-io/valkey/pull/5105) ([tests](tests/valkey/5105-d0f45c110/)): Free functions ctx synchronously when its engine state is not handed over (#5105): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5164](https://github.com/valkey-io/valkey/pull/5164) ([tests](tests/valkey/5164-d142467e9/)): Only resume a hash defrag scan from the hash scanner's own cursor (#5164): 0 of 3 test(s) reached the change, coverage 100%; crash, fixed by #5164 (recorded)
- [valkey-io/valkey#5184](https://github.com/valkey-io/valkey/pull/5184) ([tests](tests/valkey/5184-e59ba66ec/)): Treat a replica with no known primary as owning no slots (#5184): declined: The change is a NULL-pointer guard in clusterUpdateSlotsConfigWith() (cluster_legacy.c:3530), reached only via the cluster gossip bus and only when a LIVE replica's primary has been deleted from the nodes table (replicaof == NULL) while another primary claims a previously-unassigned slot. 
- [valkey-io/valkey#5115](https://github.com/valkey-io/valkey/pull/5115) ([tests](tests/valkey/5115-3cdf86c0b/)): functions: key per-engine stats by registered name, not shebang text (#5115): 1 of 1 test(s) reached the change, coverage 100%; crash, fixed by #5115 (recorded)
- [valkey-io/valkey#5169](https://github.com/valkey-io/valkey/pull/5169) ([tests](tests/valkey/5169-ff117b00a/)): Dirty every client WATCHing a key that was expired at WATCH time (#5169): 2 of 2 test(s) reached the change
- [valkey-io/valkey#5162](https://github.com/valkey-io/valkey/pull/5162) ([tests](tests/valkey/5162-c921c0a3d/)): Free the ACL log object when the ACL log is disabled (#5162): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5158](https://github.com/valkey-io/valkey/pull/5158) ([tests](tests/valkey/5158-94c716abb/)): Fail CONFIG REWRITE when the old config file reads short (#5158): declined: The change only alters behaviour when fread of the old config file returns fewer bytes than fstat reported (src/config.c rewriteConfigReadOldFile): previously a partial read proceeded with an uninitialized SDS_NOINIT buffer tail, now CONFIG REWRITE fails with errno set to EIO on a quiet sh
- [valkey-io/valkey#5155](https://github.com/valkey-io/valkey/pull/5155) ([tests](tests/valkey/5155-439d297a6/)): Reject ValkeyModule_Calloc requests whose nmemb * size overflows (#5155): 3 of 4 test(s) reached the change, coverage 75%; [crash, introduced by #5155](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/43)
- [valkey-io/valkey#5157](https://github.com/valkey-io/valkey/pull/5157) ([tests](tests/valkey/5157-88c77ea81/)): Check for an attribute reply in RM_CallReplyAttributeElement (#5157): 2 of 2 test(s) reached the change, coverage 100%; [check, introduced by #5157](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/44); [check, introduced by #5157](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/45)
- [valkey-io/valkey#5160](https://github.com/valkey-io/valkey/pull/5160) ([tests](tests/valkey/5160-2b1fc8c1f/)): Skip throttled clients in processPendingCommandAndInputBuffer (#5160): 0 of 5 test(s) reached the change, coverage 50%
- [valkey-io/valkey#5146](https://github.com/valkey-io/valkey/pull/5146): to retry, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey#5088](https://github.com/valkey-io/valkey/pull/5088) ([tests](tests/valkey/5088-7035f412c/)): Drop repeated EX/PX/EXAT when propagating HSETEX and MSETEX (#5088): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#5108](https://github.com/valkey-io/valkey/pull/5108) ([tests](tests/valkey/5108-67bf7c3d8/)): Share the Lua hook between coroutines so SCRIPT KILL reaches them all (#5108): 2 of 2 test(s) reached the change
- [valkey-io/valkey#5087](https://github.com/valkey-io/valkey/pull/5087) ([tests](tests/valkey/5087-b47c9632e/)): Treat key specs that start past the last argument as matching no keys (#5087): 4 of 4 test(s) reached the change, coverage 100%; crash, fixed by #5087 (recorded)
- [valkey-io/valkey#4929](https://github.com/valkey-io/valkey/pull/4929) ([tests](tests/valkey/4929-0a9fe22af/)): Guard NULL replica_primary_host in Sentinel INFO handling (#4929): declined: The change is a NULL guard in sentinel.c's sentinelRefreshInstanceInfo, code that executes only inside a valkey-server process running in Sentinel mode (--sentinel / valkey-sentinel) while it polls INFO from a monitored replica whose reported master_host is unset (freed by sentinelResetPri
- [valkey-io/valkey#5167](https://github.com/valkey-io/valkey/pull/5167) ([tests](tests/valkey/5167-094dcd660/)): Hash HRANDFIELD and SRANDMEMBER temporary tables case-sensitively (#5167): declined: This is a pure performance fix with no observable behaviour change. It switches the temporary SRANDMEMBER/HRANDFIELD reply hashtable (sdsReplyHashtableType) from a case-insensitive hash function (dictSdsCaseHash) to a case-sensitive one (dictSdsHash), while keeping the case-sensitive compa
- [valkey-io/valkey#5129](https://github.com/valkey-io/valkey/pull/5129) ([tests](tests/valkey/5129-557777cf7/)): Clear the script debugger input buffer when it hits the size limit (#5129): declined: The fix changes behaviour only at the Lua script debugger's buffer boundary: when the debugger input buffer ds.cbuf exceeds 1<<20 bytes it is now cleared (sdsfree + sdsempty) before the 'max client buffer reached' error returns, instead of returning with the oversized buffer still populate

### valkey-io/valkey-bloom

[2026-11-05T00:41:15+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41936041312): 3 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-bloom#110](https://github.com/valkey-io/valkey-bloom/pull/110) ([tests](tests/valkey-bloom/110-9ab369980/)): Match bloom memory tracking by small over count of bytes after a defrag operation (#110): 0 of 2 test(s) reached the change
- [valkey-io/valkey-bloom#97](https://github.com/valkey-io/valkey-bloom/pull/97) ([tests](tests/valkey-bloom/97-c340f0ec2/)): Support reusing an externally built valkey-server binary for test (#97): declined: The change modifies only build.sh and README.md of valkey-bloom's test harness: it lets the integration-test setup reuse an externally built valkey-server binary (VALKEY_SERVER_PATH) and whitelists '9.1' as a SERVER_VERSION. No module code, command handling, data format, or runtime behavio
- [valkey-io/valkey-bloom#94](https://github.com/valkey-io/valkey-bloom/pull/94) ([tests](tests/valkey-bloom/94-712a1d7ee/)): remove 'validate_scale_to' check in replica (#94): 3 of 3 test(s) reached the change

### valkey-io/valkey-json

[2026-11-05T00:15:13+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41936037415): 3 merge(s) tested, 1 without a code change.

- [valkey-io/valkey-json#65](https://github.com/valkey-io/valkey-json/pull/65) ([tests](tests/valkey-json/65-52c38bb0f/)): Correct spelling and terminology across JSON documentation and tests (#65): declined: The change is purely spelling/terminology edits: README and test-file text, renamed unit/integration test functions, comment-only edits in json.cc, keytable, and the rapidjson headers, a macro renamed with an identical string value, a string inside trace() which is an empty macro (compiles
- [valkey-io/valkey-json#117](https://github.com/valkey-io/valkey-json/pull/117) ([tests](tests/valkey-json/117-7d0acb4c9/)): Making PR's run unstable and latest only, adding a daily job for all versions (#117): declined: The change only modifies the GitHub Actions CI workflow (which server-version matrix runs on PRs versus the daily schedule) and adds a SERVER_VERSION validation check to build.sh. No module source code, command behaviour, reply format, or data path is touched, so there is no runtime behavi
- [valkey-io/valkey-json#95](https://github.com/valkey-io/valkey-json/pull/95) ([tests](tests/valkey-json/95-dcaa82c2e/)): feat: JSON.MERGE plus unit and integration tests (#95): 6 of 6 test(s) reached the change; [crash, predates #95](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/42)

### valkey-io/valkey-search

[2026-11-05T01:54:48+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41928158333): 2 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-search#1576](https://github.com/valkey-io/valkey-search/pull/1576): to retry, 'reached'
- [valkey-io/valkey-search#1542](https://github.com/valkey-io/valkey-search/pull/1542): to retry, Claude Code failed (rc=1): timeout after 3000s

## Totals

- Merges whose tests reached the change: 17 of 23 tested (74%)
- Changed-line coverage: median 100% of 13 merge(s) measured, 1 under 50%
- Recall: 3 parents flagged of 4 fix merges
- Claude: $153.42 in the last 24 hours, $153.42 in the last 7 days, $153.42 in all
- Issues filed: 6
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/40
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/41
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/42
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/43
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/44
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/45

## Retries

- [valkey-io/valkey-search#1576](https://github.com/valkey-io/valkey-search/pull/1576): attempt 1 of 3, 'reached'
- [valkey-io/valkey-search#1542](https://github.com/valkey-io/valkey-search/pull/1542): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey#5146](https://github.com/valkey-io/valkey/pull/5146): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
