# Fuzzer walk

Updated 2026-11-10T08:39+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1443 | 1073 | #1684 (2025-02-10) | 330 | 206.1 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 63 | 0 | c1423d7e4b38 (2024-11-25) | 1 | 9.0 |
| valkey-io/valkey-search | 672 | 0 | f8106e72e984 (2024-11-27) | 3 | 96.0 |

## Last runs

### valkey-io/valkey

[2026-11-10T07:17:09+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42758316003): 20 merge(s) tested, 2 without a code change.

- [valkey-io/valkey#1730](https://github.com/valkey-io/valkey/pull/1730) ([tests](tests/valkey/1730-c5ae37f22/)): Add spellchecker ignore section (#1730): declined: The change only adds an ignore entry ('Parth') to .config/typos.toml, the CI spellchecker configuration. It modifies no source code, command behaviour, replication, persistence, or tooling — no server behaviour can differ between the parent and the change, so no scenario is needed.
- [valkey-io/valkey#1724](https://github.com/valkey-io/valkey/pull/1724) ([tests](tests/valkey/1724-c4c9c6db4/)): Introduce Valkey release stage indication (#1724): 2 of 2 test(s) reached the change
- [valkey-io/valkey#1723](https://github.com/valkey-io/valkey/pull/1723) ([tests](tests/valkey/1723-41a4bc264/)): Introduce valkey_version_full info field which includes the release stage (#1723): 2 of 2 test(s) reached the change
- [valkey-io/valkey#1711](https://github.com/valkey-io/valkey/pull/1711) ([tests](tests/valkey/1711-eeda8ae74/)): Reset failover auth time when myself won the failover (#1711): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1715](https://github.com/valkey-io/valkey/pull/1715) ([tests](tests/valkey/1715-a019089ad/)): Prevent double writing out of responses and fix reply schema CI (#1715): 2 of 2 test(s) reached the change
- [valkey-io/valkey#1714](https://github.com/valkey-io/valkey/pull/1714) ([tests](tests/valkey/1714-63243e45f/)): Fix valkey-benchmark FCALL failure with TLS (#1714): 1 of 1 test(s) reached the change, coverage 83%
- [valkey-io/valkey#1712](https://github.com/valkey-io/valkey/pull/1712) ([tests](tests/valkey/1712-44675562b/)): Correctly detect address sanitizer with Clang (#1712): 0 of 2 test(s) reached the change
- [valkey-io/valkey#1710](https://github.com/valkey-io/valkey/pull/1710) ([tests](tests/valkey/1710-4e0149a38/)): Fix new benchmark test under the TLS (#1710): declined: The only behavioural change is in valkey-benchmark's fcall test path under --tls: the ad-hoc FUNCTION LOAD connection now performs the TLS handshake (cliSecureConnection) instead of sending plaintext to a TLS port and asserting. Reaching it requires a TLS-enabled server (tls-port plus cert
- [valkey-io/valkey#1709](https://github.com/valkey-io/valkey/pull/1709) ([tests](tests/valkey/1709-8fc37a0e0/)): Fix undefined behavior warning in UBSAN (#1709): declined: The change only replaces casts of sdsfree to void(*)(void*) with the equivalent sdsfreeVoid wrapper in evalInit and ldbInit, to silence a UBSAN incompatible-function-pointer-type warning. The wrapper simply calls sdsfree, so the list free behavior is byte-for-byte identical; no command rep
- [valkey-io/valkey#1707](https://github.com/valkey-io/valkey/pull/1707) ([tests](tests/valkey/1707-86469ab16/)): Fix: Exclude RTLD_DEEPBIND for Alpine Linux (musl-based systems) (#1707): declined: The change only adds `defined(__GLIBC__) && __has_include(<dlfcn.h>)` to the preprocessor condition that enables RTLD_DEEPBIND in moduleLoad's dlopen flags. On the glibc Linux machines the fuzzer runs on, both conditions are true, so the compiled code — and module-loading behaviour — is by
- [valkey-io/valkey#1694](https://github.com/valkey-io/valkey/pull/1694) ([tests](tests/valkey/1694-b8c471edd/)): valkey-cli: re-select previously selected database after reconnect (#1694): 0 of 1 test(s) reached the change, coverage 85%
- [valkey-io/valkey#1704](https://github.com/valkey-io/valkey/pull/1704) ([tests](tests/valkey/1704-1470ee8a6/)): Fix macro redefinition problem in `fmacros.h` (#1704): declined: The change only guards `#define _DEFAULT_SOURCE` with `#ifndef _DEFAULT_SOURCE` in src/fmacros.h to suppress a macro-redefinition compiler warning on Linux. _DEFAULT_SOURCE is defined in both builds either way, so the preprocessor environment and compiled behavior are identical; it is a bu
- [valkey-io/valkey#1703](https://github.com/valkey-io/valkey/pull/1703) ([tests](tests/valkey/1703-244c570c4/)): Use `DEEPBIND` flag when loading external modules using `dlopen` (#1703): 0 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1582](https://github.com/valkey-io/valkey/pull/1582) ([tests](tests/valkey/1582-99e2dc9e7/)): Add support for MustObeyClient Module API (#1582): 3 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1698](https://github.com/valkey-io/valkey/pull/1698) ([tests](tests/valkey/1698-96857bfeb/)): show capa in CLIENT LIST (#1698): 2 of 2 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1695](https://github.com/valkey-io/valkey/pull/1695) ([tests](tests/valkey/1695-8988a6135/)): Update availability-zone conf comment to mention hello command (#1695): declined: The change edits only a comment in valkey.conf, rewording the documentation of the availability-zone option to mention the HELLO command. No code path, config semantics, or server behaviour is altered, so no scenario can exercise it.
- [valkey-io/valkey#1696](https://github.com/valkey-io/valkey/pull/1696) ([tests](tests/valkey/1696-3828936fe/)): Fix outdated slot info comments in clusterGenNodesSlotsInfo (#1696): declined: The change edits only a code comment above clusterGenNodesSlotsInfo in src/cluster_legacy.c, correcting outdated documentation wording (slots_info struct -> slot_info_pairs array). No executable code, command behaviour, replies, replication, or configuration is modified, so there is no beh
- [valkey-io/valkey#1578](https://github.com/valkey-io/valkey/pull/1578) ([tests](tests/valkey/1578-f61b81e7d/)): Add `FUNCTION LOAD` and `FCALL` benchmark (#1578): 1 of 1 test(s) reached the change, coverage 100%
- [valkey-io/valkey#1497](https://github.com/valkey-io/valkey/pull/1497) ([tests](tests/valkey/1497-b58088e31/)): Adds support for running `EVAL` with different scripting engines (#1497): 15 of 15 test(s) reached the change, coverage 92%
- [valkey-io/valkey#1697](https://github.com/valkey-io/valkey/pull/1697) ([tests](tests/valkey/1697-83abb13a1/)): Fix memory error in networking unittest (#1697): declined: The change edits only src/unit/test_networking.c, adding `c->repl_data->ref_repl_buf_node = NULL;` to two unit tests' cleanup so the test harness does not reference freed replication-buffer blocks. It is a fix to the unit test code itself, compiled only into the unittest binary; no server,

### valkey-io/valkey-bloom

[2026-11-10T08:39:42+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42784309875): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-10T08:09:33+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42780367482): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-10T08:18:41+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42781518137): 0 merge(s) tested, 0 without a code change.


## Totals

- Merges whose tests reached the change: 727 of 1118 tested (65%)
- Changed-line coverage: median 100% of 654 merge(s) measured, 55 under 50%
- Recall: 199 parents flagged of 307 fix merges
- Claude: $1231.22 in the last 24 hours, $8133.35 in the last 7 days, $8133.35 in all
- Issues filed: 64
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
