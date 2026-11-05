# Fuzzer walk

Updated 2026-11-05T05:24+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 26 | 2462 | #5244 (2026-11-03) | 26 | 3.7 |
| valkey-io/valkey-bloom | 29 | 80 | #45 (2025-03-11) | 29 | 4.1 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 62 | 8.9 |
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

[2026-11-05T04:40:06+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41956216161): 20 merge(s) tested, 5 without a code change.

- [valkey-io/valkey-bloom#93](https://github.com/valkey-io/valkey-bloom/pull/93) ([tests](tests/valkey-bloom/93-c6c1558fe/)): Moving metric increment to very end of decode (#93): 0 of 1 test(s) reached the change
- [valkey-io/valkey-bloom#91](https://github.com/valkey-io/valkey-bloom/pull/91) ([tests](tests/valkey-bloom/91-fc0655648/)): Adding validation on tightening_ratio and fp_rate for load (#91): declined: The change only adds defensive (0,1) range validation for fp_rate and tightening_ratio to the bloom module's RDB-load callback (BloomObject::load_from_rdb in src/bloom/data_type.rs); on an out-of-range value it now returns None, failing the load, where the parent accepted it. Every command
- [valkey-io/valkey-bloom#92](https://github.com/valkey-io/valkey-bloom/pull/92): to retry, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-bloom#89](https://github.com/valkey-io/valkey-bloom/pull/89) ([tests](tests/valkey-bloom/89-523e19a98/)): Prevalidate in BF.LOAD (#89): declined: Pure reordering refactor of BloomObject::decode_object (the decoder behind BF.LOAD): the capacity<=0 and num_items>capacity validation checks are unchanged context lines present in both builds — the change only splits the single validate+increment loop into a validate-all loop followed by 
- [valkey-io/valkey-bloom#86](https://github.com/valkey-io/valkey-bloom/pull/86) ([tests](tests/valkey-bloom/86-8606eb073/)): Fix replica crash on partial-success BF.MADD/BF.INSERT replication (#86): 3 of 3 test(s) reached the change; crash, fixed by #86 (recorded)
- [valkey-io/valkey-bloom#88](https://github.com/valkey-io/valkey-bloom/pull/88) ([tests](tests/valkey-bloom/88-348bd9f65/)): Add more validation in the RDB Load and BF.LOAD paths (#88): 2 of 2 test(s) reached the change; crash, fixed by #88 (recorded)
- [valkey-io/valkey-bloom#82](https://github.com/valkey-io/valkey-bloom/pull/82) ([tests](tests/valkey-bloom/82-a68614b6e/)): Set ModuleOptions to Handle IO Errors (#82): 2 of 2 test(s) reached the change; crash, fixed by #82 (recorded)
- [valkey-io/valkey-bloom#79](https://github.com/valkey-io/valkey-bloom/pull/79) ([tests](tests/valkey-bloom/79-9b774acd5/)): Fix typo in bf.info.json from 'TIGHTENTING' to 'TIGHTENING' (#79): declined: The change fixes a typo ('TIGHTENTING' -> 'TIGHTENING') in src/commands/bf.info.json, a command-metadata documentation file. These JSON files are not compiled into the module: there is no build.rs, no include_str!/include_bytes!, and no set-command-info call in the crate; commands are regi
- [valkey-io/valkey-bloom#81](https://github.com/valkey-io/valkey-bloom/pull/81) ([tests](tests/valkey-bloom/81-097c2a452/)): Fix AOF rewrite and restore BF tests (#81): declined: The change modifies only the module's own CI test harness: requirements.txt bumps pytest from 6 to 7.4.3, and tests/test_bloom_aofrewrite.py is fixed to enable appendonly before writing data and to keep appendonly=yes across the test's server restart so the AOF is actually replayed. No mod
- [valkey-io/valkey-bloom#73](https://github.com/valkey-io/valkey-bloom/pull/73) ([tests](tests/valkey-bloom/73-d9998e8ce/)): update build.sh to ensure POSIX compliance for dash shell (#73): declined: The change edits only build.sh, converting bash-specific [[ ]] conditionals to POSIX [ ] tests for dash compatibility. It is a build/CI script change with no modification to the module's source code, so the compiled module's runtime behaviour (commands, replies, persistence, replication) i
- [valkey-io/valkey-bloom#76](https://github.com/valkey-io/valkey-bloom/pull/76) ([tests](tests/valkey-bloom/76-148c86d2b/)): Update README to fix 8.0 build and update build.sh to support 9.0 (#76): declined: The change edits only README.md (documentation wording for SERVER_VERSION) and build.sh (accepting '9.0' as a SERVER_VERSION for the integration-test build script). No module source code is touched, so the compiled valkey-bloom artifact at the change is behaviorally identical to the parent
- [valkey-io/valkey-bloom#68](https://github.com/valkey-io/valkey-bloom/pull/68) ([tests](tests/valkey-bloom/68-223d780d2/)): Spelling (#68): declined: The change is a spelling-only PR: it edits CI step names, README/QUICK_START prose, code comments, Python test comments and an assertion message, and the wording of one internal log-warning string. The only code change is renaming the internal Rust constant BLOOM_DEFRAG_DEAFULT to BLOOM_DE
- [valkey-io/valkey-bloom#67](https://github.com/valkey-io/valkey-bloom/pull/67) ([tests](tests/valkey-bloom/67-bf60c40e3/)): Adding clean option for build script and fixing current clippy issues (#67): declined: The change is build/CI and documentation only: a `clean` subcommand added to build.sh, a README note for it, and re-indentation of doc comments in src/bloom/utils.rs to fix a clippy doc-formatting lint. No executable Rust code changes — the module's commands, replies, serialization, and co
- [valkey-io/valkey-bloom#57](https://github.com/valkey-io/valkey-bloom/pull/57) ([tests](tests/valkey-bloom/57-148930ed7/)): Cleaning up usage of valkey-test-framework (#57): declined: no proposed test validated
- [valkey-io/valkey-bloom#47](https://github.com/valkey-io/valkey-bloom/pull/47) ([tests](tests/valkey-bloom/47-c97e0c6da/)): Adding Bloom command JSON files (#47): declined: The change only adds static JSON command-documentation files under src/commands/ (and a trailing newline in .gitignore). Nothing consumes them: valkey-bloom has no build.rs and no include_str!/include_bytes! or other reference to src/commands/; all BF.* commands, their arity, flags and ACL
- [valkey-io/valkey-bloom#56](https://github.com/valkey-io/valkey-bloom/pull/56) ([tests](tests/valkey-bloom/56-d6b8763bc/)): Updating build script and github workflow to get test framework from github and removing it locally (#56): declined: The change modifies only build/CI/test infrastructure: ci.yml and build.sh now clone the valkey-test-framework repo from GitHub, the vendored copy under tests/valkeytests/ is deleted, .gitignore gains an entry, and one test file loses a trailing newline. No module source code (src/) is tou
- [valkey-io/valkey-bloom#55](https://github.com/valkey-io/valkey-bloom/pull/55) ([tests](tests/valkey-bloom/55-5d514e051/)): Set default unstable version and update readme.md (#55): 2 of 2 test(s) reached the change
- [valkey-io/valkey-bloom#54](https://github.com/valkey-io/valkey-bloom/pull/54) ([tests](tests/valkey-bloom/54-9d403c4c7/)): Adding a quick start guide for bloom (#54): declined: The change only adds a new documentation file (QUICK_START.md) and removes an unused Windows branch from build.sh; the build script change cannot affect a Linux build's output, and no module source, command behaviour, replies, persistence or replication code changed. There is no runtime be
- [valkey-io/valkey-bloom#52](https://github.com/valkey-io/valkey-bloom/pull/52) ([tests](tests/valkey-bloom/52-d23399a41/)): Update development script's tests use module extensions from different platforms + Define default/unstable version, stage + add placeholder release notes (#52): 2 of 2 test(s) reached the change
- [valkey-io/valkey-bloom#50](https://github.com/valkey-io/valkey-bloom/pull/50) ([tests](tests/valkey-bloom/50-e308fedf1/)): Adding tightening ratio to bf.info. Making BF.BLOOM-EXPANSION also (#50): 2 of 2 test(s) reached the change

### valkey-io/valkey-json

[2026-11-05T05:24:29+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41973488938): 6 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-json#18](https://github.com/valkey-io/valkey-json/pull/18): Fix #2 - Replace the configs with constants (#18): error: `if grep -q VALKEY_MODULE_H_PATH build.sh; then` failed in merge (exit 1): Script Directory: /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge Building valkeyJSON... Building valkeyJSON for x86_64 -- The C compiler identification is GNU 15.2.0 -- The CXX compiler identificatio
- [valkey-io/valkey-json#22](https://github.com/valkey-io/valkey-json/pull/22): Fix #7 - Make SERVER_VERSION open to all versions (#22): error: `if grep -q VALKEY_MODULE_H_PATH build.sh; then` failed in merge (exit 1): Script Directory: /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge Building valkeyJSON... Building valkeyJSON for x86_64 -- The C compiler identification is GNU 15.2.0 -- The CXX compiler identificatio
- [valkey-io/valkey-json@1e33774dd28b](https://github.com/valkey-io/valkey-json/commit/1e33774dd28bdba94bc7f807a4e453f01e1f8f7c): Merge pull request #15 from joehu21/issue12: error: `if grep -q VALKEY_MODULE_H_PATH build.sh; then` failed in merge (exit 1): Script Directory: /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge Building ValkeyJSON module... Building JSON for x86_64 -- The C compiler identification is GNU 15.2.0 -- The CXX compiler identificati
- [valkey-io/valkey-json#14](https://github.com/valkey-io/valkey-json/pull/14): Fix #11 - Clean up existing module metrics (#14): error: `if grep -q VALKEY_MODULE_H_PATH build.sh; then` failed in merge (exit 1): Script Directory: /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge Building ValkeyJSON module... Building JSON for x86_64 -- The C compiler identification is GNU 15.2.0 -- The CXX compiler identificati
- [valkey-io/valkey-json#13](https://github.com/valkey-io/valkey-json/pull/13): Update integration test script (#13): error: `if grep -q VALKEY_MODULE_H_PATH build.sh; then` failed in merge (exit 1): Script Directory: /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge Building ValkeyJSON module... Building JSON for x86_64 -- The C compiler identification is GNU 15.2.0 -- The CXX compiler identificati
- [valkey-io/valkey-json#1](https://github.com/valkey-io/valkey-json/pull/1): Contributing valkeyJSON module (#1): error: `if grep -q VALKEY_MODULE_H_PATH build.sh; then` failed in merge (exit 1): Script Directory: /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge Building ValkeyJSON module... Building JSON for x86_64 -- The C compiler identification is GNU 15.2.0 -- The CXX compiler identificati

### valkey-io/valkey-search

[2026-11-05T01:54:48+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/41928158333): 2 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-search#1576](https://github.com/valkey-io/valkey-search/pull/1576): to retry, 'reached'
- [valkey-io/valkey-search#1542](https://github.com/valkey-io/valkey-search/pull/1542): to retry, Claude Code failed (rc=1): timeout after 3000s

## Totals

- Merges whose tests reached the change: 30 of 38 tested (79%)
- Changed-line coverage: median 100% of 13 merge(s) measured, 1 under 50%
- Recall: 9 parents flagged of 11 fix merges
- Claude: $239.18 in the last 24 hours, $239.18 in the last 7 days, $239.18 in all
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

- [valkey-io/valkey-bloom#92](https://github.com/valkey-io/valkey-bloom/pull/92): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey-search#1576](https://github.com/valkey-io/valkey-search/pull/1576): attempt 1 of 3, 'reached'
- [valkey-io/valkey-search#1542](https://github.com/valkey-io/valkey-search/pull/1542): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
- [valkey-io/valkey#5146](https://github.com/valkey-io/valkey/pull/5146): attempt 1 of 3, Claude Code failed (rc=1): timeout after 3000s
