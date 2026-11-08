# Fuzzer walk

Updated 2026-11-08T22:48+00:00.

| Repository | Done | Remaining | Backlog cursor | Last 24 hours | Last 7 days (per day) |
|---|---|---|---|---|---|
| valkey-io/valkey | 1027 | 1480 | #2566 (2025-08-30) | 242 | 146.7 |
| valkey-io/valkey-bloom | 109 | 0 | ce516d0ef6e3 (2024-04-26) | 0 | 15.6 |
| valkey-io/valkey-json | 62 | 0 | c1423d7e4b38 (2024-11-25) | 0 | 8.9 |
| valkey-io/valkey-search | 655 | 14 | f8106e72e984 (2024-11-27) | 150 | 93.6 |

## Last runs

### valkey-io/valkey

[2026-11-08T22:41:07+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42513425064): 20 merge(s) tested, 10 without a code change.

- [valkey-io/valkey#2658](https://github.com/valkey-io/valkey/pull/2658) ([tests](tests/valkey/2658-80cec0a18/)): Minor fix for dual rdb channel connection conn error log (#2658): declined: The change only corrects which connection's last-error string is interpolated into a warning log message when the dual-channel RDB connection fails to connect (connGetLastError(server.repl_rdb_transfer_s) instead of server.repl_transfer_s). The surrounding error-handling control flow (conn
- [valkey-io/valkey#2631](https://github.com/valkey-io/valkey/pull/2631) ([tests](tests/valkey/2631-d21d52985/)): Optimize skiplist random level generation logic (#2631): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2632](https://github.com/valkey-io/valkey/pull/2632) ([tests](tests/valkey/2632-298f7dae6/)): Adding unit tests for sha256 (#2632): declined: The change only adds unit tests: a new src/unit/test_sha256.c with three SHA-256 test vectors and their registration in src/unit/test_files.h. No server, command, replication, persistence, or tool code is modified — the SHA-256 implementation itself is untouched — so no runtime behavior ch
- [valkey-io/valkey#2630](https://github.com/valkey-io/valkey/pull/2630) ([tests](tests/valkey/2630-8d562d26d/)): Fix closing slot migration pipe read (#2630): 0 of 3 test(s) reached the change, coverage 50%
- [valkey-io/valkey#2533](https://github.com/valkey-io/valkey/pull/2533) ([tests](tests/valkey/2533-f6a0f8cfc/)): Separate RDB snapshotting from atomic slot migration (#2533): 8 of 18 test(s) reached the change, coverage 78%; check, not reproduced (recorded); check, not reproduced (recorded); crash, introduced and since fixed (recorded); crash, fixed by #2533 (recorded)
- [valkey-io/valkey#2615](https://github.com/valkey-io/valkey/pull/2615) ([tests](tests/valkey/2615-80bbbcf6f/)): Fix memory leak in deferred reply buffer (#2615): declined: The fix only adds a free method to the deferred reply list in initDeferredReplyBuffer(), which is created solely when a module has subscribed to keyspace notifications (moduleNotifyKeyspaceSubscribersCnt() > 0). No installed experiment loads a module, MODULE LOAD is refused in command sequ
- [valkey-io/valkey#2602](https://github.com/valkey-io/valkey/pull/2602) ([tests](tests/valkey/2602-93d7ccab0/)): Fix accounting for dual channel RDB bytes in replication stats (#2602): 2 of 5 test(s) reached the change, coverage 92%
- [valkey-io/valkey#2593](https://github.com/valkey-io/valkey/pull/2593) ([tests](tests/valkey/2593-2b7d5f776/)): Make modules opt-in to atomic slot migration and add server events (#2593): 6 of 6 test(s) reached the change, coverage 94%; [crash, predates #2593](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/87)
- [valkey-io/valkey#2596](https://github.com/valkey-io/valkey/pull/2596) ([tests](tests/valkey/2596-9b11d3d9e/)): Evict client only when limit is breached (#2596): declined: The change is a single `>=`-to-`>` edit in evictClients()'s loop guard; the old and new builds differ only when the summed NORMAL+PUBSUB client memory exactly equals the eviction limit (otherwise both evict the same clients or neither does). That boundary is measure-zero and not constructi
- [valkey-io/valkey#2595](https://github.com/valkey-io/valkey/pull/2595) ([tests](tests/valkey/2595-3213f48fd/)): Increase frequency of time check during fields active expiration (#2595): 0 of 3 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2583](https://github.com/valkey-io/valkey/pull/2583) ([tests](tests/valkey/2583-cef8822f6/)): valkey-cli: Add word-jump navigation (Alt/Option/Ctrl + ←/→) (#2583): declined: The change is confined to valkey-cli's interactive line editor (linenoise word-jump navigation and extended CSI escape parsing) plus a fix to the linenoise example program, which is not compiled into any shipped binary. The new code runs only inside linenoiseEdit, which is reached only whe
- [valkey-io/valkey#2429](https://github.com/valkey-io/valkey/pull/2429) ([tests](tests/valkey/2429-3b13a7cd1/)): Add cluster-announce-client-(port|tls-port) configs (#2429): 2 of 3 test(s) reached the change, coverage 91%; [check, appeared after #2429](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/107); check, introduced and since fixed (recorded)
- [valkey-io/valkey#2573](https://github.com/valkey-io/valkey/pull/2573) ([tests](tests/valkey/2573-d9382bd9f/)): ARM Neon SIMD optimization for hashtable findBucket() (#2573): 0 of 9 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2543](https://github.com/valkey-io/valkey/pull/2543) ([tests](tests/valkey/2543-9d10abfbd/)): Relaxed RDB check for foreign RDB formats (#2543): 6 of 8 test(s) reached the change, coverage 67%
- [valkey-io/valkey#2546](https://github.com/valkey-io/valkey/pull/2546) ([tests](tests/valkey/2546-811a64407/)): Un-deprecate commands (#2546): 2 of 2 test(s) reached the change
- [valkey-io/valkey#2571](https://github.com/valkey-io/valkey/pull/2571) ([tests](tests/valkey/2571-971cdb783/)): Don't use AVX2 instructions if the CPU don't support it (#2571): declined: The fix adds a runtime __builtin_cpu_supports("avx2") check before dispatching to popcountAVX2 in serverPopcount. It changes behaviour only on x86-64 CPUs that lack AVX2, where the parent build would execute illegal instructions and die with SIGILL on BITCOUNT of a 32+ byte string. On the 
- [valkey-io/valkey#2572](https://github.com/valkey-io/valkey/pull/2572) ([tests](tests/valkey/2572-47d2203c1/)): Store number of keys with volatile items per slot in RDB aux field and pre-size hashtables on load (#2572): 0 of 5 test(s) reached the change, coverage 100%
- [valkey-io/valkey#2215](https://github.com/valkey-io/valkey/pull/2215) ([tests](tests/valkey/2215-eebed8842/)): Remove deny_blocking check in moduleBlockClient, cleanup code and doc (#2215): 4 of 4 test(s) reached the change, coverage 100%; crash, fixed by #2215 (recorded)
- [valkey-io/valkey#2237](https://github.com/valkey-io/valkey/pull/2237) ([tests](tests/valkey/2237-b44de37b7/)): New module API event for tracking authentication attempts (#2237): 3 of 6 test(s) reached the change, coverage 60%; [check, introduced by #2237](https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/106)
- [valkey-io/valkey#2358](https://github.com/valkey-io/valkey/pull/2358) ([tests](tests/valkey/2358-c039b691c/)): Fix module context object re-usage in scripting engines (#2358): declined: no proposed test validated

### valkey-io/valkey-bloom

[2026-11-08T22:48:14+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42531998855): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-json

[2026-11-08T22:42:15+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42531494530): 0 merge(s) tested, 0 without a code change.


### valkey-io/valkey-search

[2026-11-08T22:42:04+00:00](https://github.com/sarthakaggarwal97/valkey-ci-agent/actions/runs/42526127195): 14 merge(s) tested, 0 without a code change.

- [valkey-io/valkey-search#68](https://github.com/valkey-io/valkey-search/pull/68): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#77](https://github.com/valkey-io/valkey-search/pull/77): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#76](https://github.com/valkey-io/valkey-search/pull/76): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#73](https://github.com/valkey-io/valkey-search/pull/73): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#60](https://github.com/valkey-io/valkey-search/pull/60): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#72](https://github.com/valkey-io/valkey-search/pull/72): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#71](https://github.com/valkey-io/valkey-search/pull/71): to retry, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#70](https://github.com/valkey-io/valkey-search/pull/70): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): Checking for gcc...[32;1mok[0m Checking for ninja...[32;1mok[0m [35;1mBuilding[0m [1/26] Linking CXX executable tests/ft_search_test FAILED: [
- [valkey-io/valkey-search#49](https://github.com/valkey-io/valkey-search/pull/49): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge/.build-release/.deps/install/lib/libgrpc.a(syste
- [valkey-io/valkey-search@1d8e30e25ae6](https://github.com/valkey-io/valkey-search/commit/1d8e30e25ae640522f809f839ed2a81fb191e5c1): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey-search#65](https://github.com/valkey-io/valkey-search/pull/65): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey-search#64](https://github.com/valkey-io/valkey-search/pull/64): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey-search#63](https://github.com/valkey-io/valkey-search/pull/63): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey-search#57](https://github.com/valkey-io/valkey-search/pull/57): to retry, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey

## Totals

- Merges whose tests reached the change: 618 of 900 tested (69%)
- Changed-line coverage: median 100% of 466 merge(s) measured, 36 under 50%
- Recall: 168 parents flagged of 249 fix merges
- Claude: $1333.87 in the last 24 hours, $6557.52 in the last 7 days, $6557.52 in all
- Issues filed: 58
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/100
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/101
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/102
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/103
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/104
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/105
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/106
  - https://github.com/sarthakaggarwal97/valkey-fuzzer/issues/107
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

- [valkey-io/valkey-search#68](https://github.com/valkey-io/valkey-search/pull/68): attempt 2 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#77](https://github.com/valkey-io/valkey-search/pull/77): attempt 2 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#76](https://github.com/valkey-io/valkey-search/pull/76): attempt 2 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#73](https://github.com/valkey-io/valkey-search/pull/73): attempt 2 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#60](https://github.com/valkey-io/valkey-search/pull/60): attempt 2 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#72](https://github.com/valkey-io/valkey-search/pull/72): attempt 2 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#71](https://github.com/valkey-io/valkey-search/pull/71): attempt 2 of 3, building valkey-search left no .build-release/libsearch.so
- [valkey-io/valkey-search#70](https://github.com/valkey-io/valkey-search/pull/70): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): Checking for gcc...[32;1mok[0m Checking for ninja...[32;1mok[0m [35;1mBuilding[0m [1/26] Linking CXX executable tests/ft_search_test FAILED: [
- [valkey-io/valkey-search#49](https://github.com/valkey-io/valkey-search/pull/49): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey-ci-agent/walk-work/merge/.build-release/.deps/install/lib/libgrpc.a(syste
- [valkey-io/valkey-search@1d8e30e25ae6](https://github.com/valkey-io/valkey-search/commit/1d8e30e25ae640522f809f839ed2a81fb191e5c1): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey-search#65](https://github.com/valkey-io/valkey-search/pull/65): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey-search#64](https://github.com/valkey-io/valkey-search/pull/64): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey-search#63](https://github.com/valkey-io/valkey-search/pull/63): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey-search#57](https://github.com/valkey-io/valkey-search/pull/57): attempt 2 of 3, `export NINJA_TOOL=ninja` failed in merge (exit 1): systemd_utils.cc:(.text+0x6f): undefined reference to `sd_is_socket_unix' /usr/bin/x86_64-linux-gnu-ld.bfd: /home/runner/work/valkey-ci-agent/valkey
- [valkey-io/valkey#3800](https://github.com/valkey-io/valkey/pull/3800): attempt 0 of 3, Claude Code failed (rc=1): API Error: Claude Code is unable to respond to this request, which appears to violate our Usage Policy (https://www.anthropic.com/legal/aup). This request triggered restrict
