# Fuzzer walk

Updated 2026-11-05T04:39+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 26 | 2462 | #5244 (2026-11-03) | 26 | 3.7 |
| valkey-io/valkey-bloom | 5 | 104 | #93 (2026-04-26) | 5 | 0.7 |
| valkey-io/valkey-json | 26 | 36 | #70 (2025-08-12) | 26 | 3.7 |
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

[2026-11-05T04:39:38+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41956209482): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey-json#115](https://github.com/valkey-io/valkey-json/pull/115) ([tests](tests/valkey-json/115-5e2f9fd31/)): Making it so ASAN failures fail the CI workflows (#115): declined: The change modifies only tst/integration/run.sh, the valkey-json CI test-runner script: it adds ASAN/LSAN option exports, pipefail, and log-scanning so ASAN errors, memory leaks, and pytest failures fail the CI job. No module source code is touched, so the built valkey-json module is behav
- [valkey-io/valkey-json#116](https://github.com/valkey-io/valkey-json/pull/116) ([tests](tests/valkey-json/116-62aec99ca/)): fixing heap-use-after-free bugs in JSON.MSET by closing key handles (ValkeyModule_CloseKey) within the operation loop (#116): 2 of 2 test(s) reached the change; crash, fixed by #116 (recorded)
- [valkey-io/valkey-json#111](https://github.com/valkey-io/valkey-json/pull/111) ([tests](tests/valkey-json/111-653d4022e/)): Fix unbounded array Reserve in legacy RDB load (#111): 0 of 2 test(s) reached the change
- [valkey-io/valkey-json#112](https://github.com/valkey-io/valkey-json/pull/112) ([tests](tests/valkey-json/112-0c320dda8/)): Fix JSON.MSET duplicate-key crash (#112): 1 of 6 test(s) reached the change; crash, predates #112, since fixed (recorded); crash, fixed by #112 (recorded)
- [valkey-io/valkey-json#108](https://github.com/valkey-io/valkey-json/pull/108) ([tests](tests/valkey-json/108-8a826757c/)): Fix for crash when key contains ~ (#108): 3 of 3 test(s) reached the change; crash, fixed by #108 (recorded)
- [valkey-io/valkey-json#109](https://github.com/valkey-io/valkey-json/pull/109) ([tests](tests/valkey-json/109-faf58c339/)): Fixing out of range crash (#109): 1 of 6 test(s) reached the change; crash, fixed by #109 (recorded)
- [valkey-io/valkey-json#103](https://github.com/valkey-io/valkey-json/pull/103) ([tests](tests/valkey-json/103-8ff1f53b7/)): Support reusing externally built valkey-server and valkeymodule.h for test (#103): declined: The change modifies only the build and test infrastructure (CMakeLists.txt, build.sh, README.md): it adds optional VALKEY_SERVER_PATH and VALKEY_MODULE_H_PATH cache variables so the integration-test build can reuse an externally built valkey-server and valkeymodule.h instead of cloning and
- [valkey-io/valkey-json#102](https://github.com/valkey-io/valkey-json/pull/102) ([tests](tests/valkey-json/102-5e98983ff/)): Support running integration tests against an external Valkey server (#102): declined: The change is a CI/test-infrastructure change only: it bumps the pinned valkey-test-framework commit in CMakeLists.txt, documents the new VALKEY_EXTERNAL_SERVER/VALKEY_HOST/VALKEY_PORT variables in README.md, and removes pkill cleanup from the integration-test runner script tst/integration
- [valkey-io/valkey-json#98](https://github.com/valkey-io/valkey-json/pull/98) ([tests](tests/valkey-json/98-8ee03af75/)): fix: gate JSON.DEBUG KEYTABLE-CORRUPT behind json.debug-mode config (#98): 2 of 2 test(s) reached the change
- [valkey-io/valkey-json#100](https://github.com/valkey-io/valkey-json/pull/100) ([tests](tests/valkey-json/100-632212f8b/)): ci: Add cpplint workflow to run linter on PRs (#100): declined: The change is purely CI tooling: it adds a .github/workflows/linter.yml workflow that runs cpplint on PRs and widens the lint filter lists in src/json/CPPLINT.cfg and tst/unit/CPPLINT.cfg. No C++ source, build script, or module behavior changes, so the compiled valkey-json module is byte-f
- [valkey-io/valkey-json#99](https://github.com/valkey-io/valkey-json/pull/99) ([tests](tests/valkey-json/99-66101ce33/)): Cast int64 literals to double in jsonutil_is_int64 tests (#99): declined: The change edits only a unit-test file (tst/unit/util_test.cc), adding explicit static_cast<double> to two int64 literals passed to jsonutil_is_int64(double). The implicit conversion already occurred at those call sites, so this only silences a compiler warning in the test build; no module
- [valkey-io/valkey-json@a932a1c38e12](https://github.com/valkey-io/valkey-json/commit/a932a1c38e121b26c88000be6c870e830c607e37) ([tests](tests/valkey-json/a932a1c38/)): Fix build for failure with GCC 16 for maybe_unused var: declined: The change only adds a [[maybe_unused]] attribute to a local variable in the vendored rapidjson header to fix a GCC 16 build failure (unused-variable diagnostic). The attribute has no runtime effect: no code path, reply, data layout or resource usage changes, so there is no behaviour to ex
- [valkey-io/valkey-json#92](https://github.com/valkey-io/valkey-json/pull/92) ([tests](tests/valkey-json/92-6f08bbf02/)): Fix build on mac by using correct format specifier (#92): declined: The change only replaces %lx/%lu with %llx/%llu in two ValkeyModule_Log(IOError) format strings to fix compilation on macOS, where uint64_t is unsigned long long. It alters no command behaviour, replies, data, or persistence format — only the text of a log line, and on the 64-bit Linux CI 
- [valkey-io/valkey-json#93](https://github.com/valkey-io/valkey-json/pull/93): to retry, `SERVER_VERSION=unstable VALKEY_MODULE_H_PATH="${VALKEY_SRC}/src/valkeymodule.h" ./build.sh` failed in parent (exit 2): compilation terminated. In file included from /home/runner/work/valkey-ci-agent/
- [valkey-io/valkey-json#73](https://github.com/valkey-io/valkey-json/pull/73): to retry, `SERVER_VERSION=unstable VALKEY_MODULE_H_PATH="${VALKEY_SRC}/src/valkeymodule.h" ./build.sh` failed in merge (exit 2): In file included from /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work
- [valkey-io/valkey-json#88](https://github.com/valkey-io/valkey-json/pull/88) ([tests](tests/valkey-json/88-0998b2380/)): Fix #86: Upgrade pytest and add setuptools to fix test suite (#88): declined: The change modifies only requirements.txt (upgrading pytest from 6 to 7.4.3 and adding setuptools) and tst/integration/README.md to fix the project's own Python test suite. No module source, build configuration, or runtime code is touched, so JSON command behavior, replies, persistence, an
- [valkey-io/valkey-json#89](https://github.com/valkey-io/valkey-json/pull/89) ([tests](tests/valkey-json/89-fd2753d8c/)): added VALKEYMODULE_OPTIONS_HANDLE_REPL_ASYNC_LOAD (#89): 2 of 2 test(s) reached the change; [crash, predates #89](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/48)
- [valkey-io/valkey-json#85](https://github.com/valkey-io/valkey-json/pull/85) ([tests](tests/valkey-json/85-211457637/)): Fixes redhat-linux-build for build failure (#85): declined: The change is a build fix: it only swaps printf format specifiers (%llx->%lx, %llu->%lu) in two ValkeyModule_Log/LogIOError calls in dom.cc and keytable.cc so the module compiles on redhat-linux. On LP64 Linux uint64_t is unsigned long, so the logged values are unchanged; the affected line
- [valkey-io/valkey-json#71](https://github.com/valkey-io/valkey-json/pull/71) ([tests](tests/valkey-json/71-fe9b04858/)): Implement SharedAPI interface that mimics JSON.GET (#71): 4 of 4 test(s) reached the change; [check, introduced by #71](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/47)
- [valkey-io/valkey-json#76](https://github.com/valkey-io/valkey-json/pull/76) ([tests](tests/valkey-json/76-af9354321/)): Spell Check Workflow for Valkey JSON (#76): declined: The change adds a spell-check CI workflow (.github/workflows/spell-check.yml, .config/typos.toml) and fixes typos that are exclusively in code comments (dom.cc, json.cc, memory.cc, selector.cc, rapidjson headers) and in src/commands/json.arrappend.json, a documentation metadata file that i

### valkey-io/valkey-search

[2026-11-05T01:54:48+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41928158333): 2 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-search#1576](https://github.com/valkey-io/valkey-search/pull/1576): to retry, 'reached'
- [valkey-io/valkey-search#1542](https://github.com/valkey-io/valkey-search/pull/1542): to retry, Claude Code failed (rc=1): timeout after 3000s

## Totals

- Merges whose tests reached the change: 24 of 31 tested (77%)
- Changed-line coverage: median 100% of 13 merge(s) measured, 1 under 50%
- Recall: 8 parents flagged of 10 fix merges
- Claude: $200.26 in the last 24 hours, $200.26 in the last 7 days, $200.26 in all
- Issues filed: 8
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/40
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/41
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/42
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/43
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/44
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/45
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/47
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/48

## Retries

- [valkey-io/valkey-json#93](https://github.com/valkey-io/valkey-json/pull/93): attempt 1 of 3, `SERVER_VERSION=unstable VALKEY_MODULE_H_PATH="${VALKEY_SRC}/src/valkeymodule.h" ./build.sh` failed in parent (exit 2): compilation terminated. In file included from /home/runner/work/valkey-ci-agent/
- [valkey-io/valkey-json#73](https://github.com/valkey-io/valkey-json/pull/73): attempt 1 of 3, `SERVER_VERSION=unstable VALKEY_MODULE_H_PATH="${VALKEY_SRC}/src/valkeymodule.h" ./build.sh` failed in merge (exit 2): In file included from /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work
- [valkey-io/valkey-search#1576](https://github.com/valkey-io/valkey-search/pull/1576): attempt 1 of 3, 'reached'
- [valkey-io/valkey-search#1542](https://github.com/valkey-io/valkey-search/pull/1542): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey#5146](https://github.com/valkey-io/valkey/pull/5146): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
