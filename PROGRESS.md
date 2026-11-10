# Fuzzer walk

Updated 2026-11-10T17:35+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1558 | 963 | #1451 (2024-12-17) | 306 | 222.6 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 63 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.0 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 3 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-10T16:46:02+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42840904225): 20 merge(s) tested, 7 without a code change.

- [valkey-io/valkey#1489](https://github.com/valkey-io/valkey/pull/1489) ([tests](tests/valkey/1489-8d764f27b/)): Refactor: move all valkey modules related declarations to `module.h` (#1489): 0 of 7 test(s) reached the change, coverage 74%
- [valkey-io/valkey#1496](https://github.com/valkey-io/valkey/pull/1496) ([tests](tests/valkey/1496-ede4adde7/)): Remove releasetools folder (#1496): declined: The change only removes the utils/releasetools directory: release-engineering shell scripts (tarball creation/upload, release sanity test, hash publishing) and a changelog.tcl generator. No source, build, test, or tool code that ships in a Valkey build is modified, so there is no runtime b
- [valkey-io/valkey#1449](https://github.com/valkey-io/valkey/pull/1449) ([tests](tests/valkey/1449-35abb68b7/)): Offload reading the replication stream to IO threads (#1449): 0 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1422](https://github.com/valkey-io/valkey/pull/1422) ([tests](tests/valkey/1422-ae70c5459/)): replication: fix io-threads possible race by moving waitForClientIO (#1422): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#577](https://github.com/valkey-io/valkey/pull/577) ([tests](tests/valkey/577-0f273bb64/)): Align rejected unblocked commands to update the correct error statistic (#577): 4 of 4 test(s) reached the change, coverage 50%
- [valkey-io/valkey#1159](https://github.com/valkey-io/valkey/pull/1159) ([tests](tests/valkey/1159-a136ad9a5/)): Make global configs as static (#1159): declined: The diff only changes linkage: `dict *configs` and `configEnumGetValue` in src/config.c become `static`. No runtime behaviour, reply, config semantics, or data path is altered; any misuse would be a link-time error caught by the build, not something a fuzzer scenario could exercise.
- [valkey-io/valkey#1441](https://github.com/valkey-io/valkey/pull/1441) ([tests](tests/valkey/1441-e4179f1f3/)): Only (re-)send MEET packet once every handshake timeout period (#1441): 2 of 4 test(s) reached the change, coverage 86%; check, introduced and since fixed (recorded)
- [valkey-io/valkey#1492](https://github.com/valkey-io/valkey/pull/1492) ([tests](tests/valkey/1492-e470735d9/)): Immediately restart the defrag cycle if we still need to defrag (#1492): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1473](https://github.com/valkey-io/valkey/pull/1473) ([tests](tests/valkey/1473-8b4034129/)): Fix JSON description of SET command (#1473): 0 of 2 test(s) reached the change
- [valkey-io/valkey#1486](https://github.com/valkey-io/valkey/pull/1486) ([tests](tests/valkey/1486-bb325bde3/)): Fix restore replica output bytes stat update (#1486): 1 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1474](https://github.com/valkey-io/valkey/pull/1474) ([tests](tests/valkey/1474-da92c1d6c/)): Document all command flags near serverCommand (#1474): declined: The change only adds comments to src/server.h documenting existing command flags (CMD_MODULE, CMD_PROTECTED, CMD_MODULE_GETKEYS, etc.). No code, data structures, or behaviour is modified—compiled output is identical—so there is nothing to test.
- [valkey-io/valkey#1173](https://github.com/valkey-io/valkey/pull/1173) ([tests](tests/valkey/1173-9f4503ca5/)): Add scoped RDB loading context and immediate abort flag (#1173): 0 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1481](https://github.com/valkey-io/valkey/pull/1481) ([tests](tests/valkey/1481-2ee06e798/)): Remove readability refactor for failover auth to fix clang warning (#1481): declined: The diff only inlines the trivial static helper getMessageFromSendBlock (which returns &msgblock->data[0].msg) back into clusterRequestFailoverAuth as a direct msgblock->data[0].msg.mflags[0] access, to fix a clang warning. The two expressions access the identical union member, so the FORC
- [valkey-io/valkey#1472](https://github.com/valkey-io/valkey/pull/1472) ([tests](tests/valkey/1472-d00c85644/)): Fix switch case compilation error in the new helloscripting (#1472): declined: The diff only wraps two switch-case bodies in braces inside tests/modules/helloscripting.c to fix a compilation error (declarations after a case label need a scope block). It is a build fix in a test-only module with no behaviour change: the executed instructions (ARGS, RETURN) do exactly 
- [valkey-io/valkey#1277](https://github.com/valkey-io/valkey/pull/1277) ([tests](tests/valkey/1277-6adef8e2f/)): Adds support for scripting engines as Valkey modules (#1277): 7 of 11 test(s) reached the change, coverage 79%
- [valkey-io/valkey#1463](https://github.com/valkey-io/valkey/pull/1463) ([tests](tests/valkey/1463-1c9731751/)): Resolve bounds checks on cluster_legacy.c (#1463): 0 of 6 test(s) reached the change, coverage 88%
- [valkey-io/valkey#1184](https://github.com/valkey-io/valkey/pull/1184) ([tests](tests/valkey/1184-ca0b0c662/)): Clear outdated failure reports more accurately (#1184): 0 of 5 test(s) reached the change, coverage 75%
- [valkey-io/valkey#1392](https://github.com/valkey-io/valkey/pull/1392) ([tests](tests/valkey/1392-e9a1fe0b3/)): Support for reading from replicas in valkey-benchmark (#1392): 10 of 10 test(s) reached the change, coverage 86%
- [valkey-io/valkey#1341](https://github.com/valkey-io/valkey/pull/1341) ([tests](tests/valkey/1341-97029953a/)): Minor log fixes when failover auth denied due to slot epoch (#1341): declined: The change only edits two log message strings in cluster_legacy.c: it renames a variable label in the failover-auth-denied warning and appends the node's config epoch to the election-start notice. The logic (vote denial condition, election start, epoch handling) is unchanged; nodeEpoch(mys
- [valkey-io/valkey#1338](https://github.com/valkey-io/valkey/pull/1338) ([tests](tests/valkey/1338-8060c86d2/)): Offload TLS negotiation to I/O threads (#1338): 2 of 5 test(s) reached the change, coverage 13%

### valkey-io/valkey-bloom

[2026-11-10T17:23:22+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42864330418): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-10T17:23:39+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42864371675): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-10T17:35:50+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42866417992): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 756 of 1173 tested (64%)
- Changed-line coverage: median 100% of 705 merge(s) measured, 56 under 50%
- Recall: 203 parents flagged of 320 fix merges
- Claude: $1137.57 in the last 24 hours, $8567.69 in the last 7 days, $8567.69 in all
- Issues filed: 66
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
