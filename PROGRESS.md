# Fuzzer walk

Updated 2026-11-08T14:38+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 941 | 1566 | #2787 (2025-10-31) | 303 | 134.4 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 662 | 7 | 965d40ad2aa1 (2025-01-03) | 227 | 94.6 |

## Last runs

### valkey-io/valkey

[2026-11-08T12:40:17+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42471378663): 20 merge(s) tested, 11 without a code change.

- [valkey-io/valkey#2508](https://github.com/valkey-io/valkey/pull/2508) ([tests](tests/valkey/2508-33bfac37b/)): Optimize zset memory usage by embedding element in skiplist (#2508): 1 of 9 test(s) reached the change, coverage 88%
- [valkey-io/valkey#2659](https://github.com/valkey-io/valkey/pull/2659) ([tests](tests/valkey/2659-a06cf15b2/)): Allow dual channel full sync in plain failover (#2659): 0 of 3 test(s) reached the change, coverage 0%
- [valkey-io/valkey#2825](https://github.com/valkey-io/valkey/pull/2825) ([tests](tests/valkey/2825-86db60921/)): Print node name on a best effort basis if light weight message is received before link stabilization (#2825): declined: The change only rewrites serverLog message text in cluster_legacy.c, factoring the existing `link->node ? name : "<unknown>"` null checks into two inline helpers and adding the human node name to a few debug/notice/warning log lines. No command reply, cluster-bus protocol behaviour, link l
- [valkey-io/valkey#2829](https://github.com/valkey-io/valkey/pull/2829) ([tests](tests/valkey/2829-331a85282/)): Change DEFAULT_WAIT_BEFORE_RDB_CLIENT_FREE from 60s to 5s (#2829): 0 of 3 test(s) reached the change
- [valkey-io/valkey#2835](https://github.com/valkey-io/valkey/pull/2835) ([tests](tests/valkey/2835-8e0b375da/)): Fix cluster slot stats for scripts with cross-slot keys (#2835): 2 of 2 test(s) reached the change, coverage 100%; check, fixed by #2835 (recorded); check, fixed by #2835 (recorded)
- [valkey-io/valkey#2581](https://github.com/valkey-io/valkey/pull/2581) ([tests](tests/valkey/2581-01a7657b8/)): Add --warmup and --duration parameters to valkey-benchmark (#2581): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2796](https://github.com/valkey-io/valkey/pull/2796) ([tests](tests/valkey/2796-1b0b5c0cf/)): New module API to perform prefix‑aware ACL permission check (#2796): 2 of 2 test(s) reached the change, coverage 86%
- [valkey-io/valkey#2817](https://github.com/valkey-io/valkey/pull/2817) ([tests](tests/valkey/2817-3c378862c/)): Cluster: Avoid usage of light weight messages to nodes with not ready bidirectional links (#2817): 0 of 3 test(s) reached the change, coverage 50%
- [valkey-io/valkey#2828](https://github.com/valkey-io/valkey/pull/2828) ([tests](tests/valkey/2828-047080a62/)): shared zadd for geoadd (#2828): 0 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2366](https://github.com/valkey-io/valkey/pull/2366) ([tests](tests/valkey/2366-2da21d9de/)): Allow partial sync after loading AOF with preamble (#2366): 1 of 5 test(s) reached the change, coverage 96%
- [valkey-io/valkey#2804](https://github.com/valkey-io/valkey/pull/2804) ([tests](tests/valkey/2804-7fbd4cb26/)): Expose SIMPLE_STRING and ARRAY_NULL reply type to the Module API (#2804): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2818](https://github.com/valkey-io/valkey/pull/2818) ([tests](tests/valkey/2818-bb8989cfd/)): Adds new module context flag `VALKEYMODULE_CTX_SCRIPT_EXECUTION` (#2818): 2 of 5 test(s) reached the change, coverage 30%; crash, fixed by #2818 (recorded)
- [valkey-io/valkey#2624](https://github.com/valkey-io/valkey/pull/2624) ([tests](tests/valkey/2624-65ab07dde/)): Leverage zfree_with_size for client reply blocks (#2624): 0 of 3 test(s) reached the change, coverage 57%
- [valkey-io/valkey#2798](https://github.com/valkey-io/valkey/pull/2798) ([tests](tests/valkey/2798-7a1d98969/)): Add "script" context to  ACL log entries (#2798): 3 of 3 test(s) reached the change, coverage 91%; check, not reproduced (recorded); check, introduced and since fixed (recorded); check, not reproduced (recorded); check, introduced and since fixed (recorded); check, not reproduced (recorded); check, introduced and since fixed (recorded)
- [valkey-io/valkey#2608](https://github.com/valkey-io/valkey/pull/2608) ([tests](tests/valkey/2608-32844b8b0/)): Configurable DB hash seed for SCAN family commands consistency (#2608): 1 of 5 test(s) reached the change, coverage 87%
- [valkey-io/valkey#2800](https://github.com/valkey-io/valkey/pull/2800) ([tests](tests/valkey/2800-c88c94e32/)): Reuse dbHasNoKeys() inside dbsHaveNoKeys() to remove duplicate logic (#2800): declined: The change replaces the inlined emptiness check in dbsHaveNoKeys() with a call to the existing dbHasNoKeys() helper. Within the loop bounds [0, server.dbnum) the helper's added range checks are always false, so it evaluates to exactly the same condition the old code tested (!server.db[i] |
- [valkey-io/valkey#2799](https://github.com/valkey-io/valkey/pull/2799) ([tests](tests/valkey/2799-a49d469f4/)): Reverts hashHashtableTypeValidate signature (#2799): declined: The change only reverts the C signature of hashHashtableTypeValidate from 'entry *' to 'void *' and adds an equivalent local cast, restoring type compatibility with the hashtableType.validateEntry callback declared in server.c. The function body and all runtime behaviour are identical; it 
- [valkey-io/valkey#2794](https://github.com/valkey-io/valkey/pull/2794) ([tests](tests/valkey/2794-a99c63632/)): Improve header comment and strengthen type checking for entry (#2794): declined: The change is a pure type-safety refactor of src/entry.c/entry.h and callers in t_hash.c: a rewritten header comment, `typedef void entry` replaced by an opaque `struct _entry` typedef, explicit casts, an `entryGetField()` accessor that returns the same pointer as before ((sds)entry), and 
- [valkey-io/valkey#2668](https://github.com/valkey-io/valkey/pull/2668) ([tests](tests/valkey/2668-4d78d36bf/)): HSETEX: Support NX/XX Flags (#2668): 3 of 4 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2780](https://github.com/valkey-io/valkey/pull/2780) ([tests](tests/valkey/2780-6cbc1a31d/)): Sentinel: fix regression requiring "+failover" ACL in failover path (#2780): declined: The change is confined to Sentinel-mode code: sentinelSendReplicaOf in src/sentinel.c now sends FAILOVER ABORT in the reconfiguration MULTI only when the monitored instance's master_failover_state shows an ongoing failover, instead of whenever the instance supports that INFO field, so a se

### valkey-io/valkey-bloom

[2026-11-08T14:24:21+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42489583289): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-08T14:38:06+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42490726994): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-08T14:23:10+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42486766206): 20 merge(s) tested, 1 without a code change.

- [valkey-io/valkey-search#29](https://github.com/valkey-io/valkey-search/pull/29): Redesign FT.SEARCH command line parsing in anticipation of merging with future FT.AGGREGATE code. (#29): error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@2cf882201edf](https://github.com/valkey-io/valkey-search/commit/2cf882201edfba2c0e985f6d3a46ef5590ba7a83): fixing clang tidy github workflow: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@1e6d1d26bccf](https://github.com/valkey-io/valkey-search/commit/1e6d1d26bccf4929a3c7080ba687c2bd3b8cb1ff): fixing github clang_tidy_format workflow to run as ubuntu:ubuntu: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@6bcb549550c2](https://github.com/valkey-io/valkey-search/commit/6bcb549550c23f0a0277ab572fd70992633c6582): adding spell checker verification and fixing spell errors: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@8ca35b577ed3](https://github.com/valkey-io/valkey-search/commit/8ca35b577ed306dd8c12f0d7046c9d0cd2d742e9): Fixing grpc crash: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@9ea9ff93e500](https://github.com/valkey-io/valkey-search/commit/9ea9ff93e500ef8ce0c2daa1cf7838011095439d): Downgrading grpc to 1.63.1.bcr.1: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@e1bc348254da](https://github.com/valkey-io/valkey-search/commit/e1bc348254daeea55b47f650a5657343a77c3440): adding dev container setup script: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@7164c6539888](https://github.com/valkey-io/valkey-search/commit/7164c6539888ace67e2690908eba33d97bbe8fcf): fixing refresh_comp_db.sh issue related to identifying the mounted directory: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@6b84fe12c9a8](https://github.com/valkey-io/valkey-search/commit/6b84fe12c9a89612895fcf3c1baa0e55834d50b5): additional github workflow chnages: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@7b8d240876b0](https://github.com/valkey-io/valkey-search/commit/7b8d240876b04a74480dac69f37f2a048929dc6d): adding support for github presubmit tests: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@df4dc899d3f4](https://github.com/valkey-io/valkey-search/commit/df4dc899d3f4757fa2bd2112889a9e3ca91d125e): clang format changes: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@271169df6159](https://github.com/valkey-io/valkey-search/commit/271169df6159cd1062cc57d79a1e9de2a2e1dd85): documenting vscode and dev-container in the Developer Guide: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@48cbd5623cd8](https://github.com/valkey-io/valkey-search/commit/48cbd5623cd8f2173613ed432891d567e4b43d85): adding support for dev-container and vscode: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@cfd25f76e3b3](https://github.com/valkey-io/valkey-search/commit/cfd25f76e3b33ba179350b4b8bde023fc3da7bd4): switching licensing from apache2 to bsd-3: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@508d9e90b4a2](https://github.com/valkey-io/valkey-search/commit/508d9e90b4a25adf300183ec8850cc4f81bf5a5a): fixing check_changes.sh to use `--cached` rather than `--cache`: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@135858139a87](https://github.com/valkey-io/valkey-search/commit/135858139a878fc90402c9158ce007c7895d0923): additional clang-tidy cleanups: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@740b42ca1196](https://github.com/valkey-io/valkey-search/commit/740b42ca1196eeb3d134666c143fee35ce367484): clang-tidy related changes: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@07e5dd955989](https://github.com/valkey-io/valkey-search/commit/07e5dd955989f6b24b98b56c160e36a0a9af5263): additional compilation cleanup: error: `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coordinator.grpc.pb.h': No such file or directory
- [valkey-io/valkey-search@b6f5460f2903](https://github.com/valkey-io/valkey-search/commit/b6f5460f2903d805de90fef40fcc568f23b423df): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coo
- [valkey-io/valkey-search@4015cc6b5efb](https://github.com/valkey-io/valkey-search/commit/4015cc6b5efb51e1c1e40256822f827da7a2ba45): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coo

## Totals

- Merges whose tests reached the change: 587 of 858 tested (68%)
- Changed-line coverage: median 100% of 431 merge(s) measured, 34 under 50%
- Recall: 160 parents flagged of 236 fix merges
- Claude: $1631.34 in the last 24 hours, $6187.05 in the last 7 days, $6187.05 in all
- Issues filed: 56
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/100
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/101
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/102
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/103
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/104
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/105
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

- [valkey-io/valkey-search@b6f5460f2903](https://github.com/valkey-io/valkey-search/commit/b6f5460f2903d805de90fef40fcc568f23b423df): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coo
- [valkey-io/valkey-search@4015cc6b5efb](https://github.com/valkey-io/valkey-search/commit/4015cc6b5efb51e1c1e40256822f827da7a2ba45): attempt 1 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 2): grep: build.sh: No such file or directory bash: line 12: ./build.sh: No such file or directory ls: cannot access '.build-release/src/coordinator/coo
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
- [valkey-io/valkey#3498](https://github.com/valkey-io/valkey/pull/3498): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
