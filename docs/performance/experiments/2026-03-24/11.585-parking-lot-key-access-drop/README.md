# 11.585 parking_lot key-access lock

Status: dropped

Goal:
- take a smaller follow-up cut after the rejected client-handle redesign
- reduce the remaining hot `record_key_access(...)` lock overhead without changing data layout
- swap only the per-shard key-access metadata locks from `std::sync::Mutex` to `parking_lot::Mutex`

Candidate:
- added `parking_lot` as a direct `garnet-server` dependency
- changed `DbCatalogSideState.key_lru_access_millis` and `.key_lfu_frequency` to `PerShard<parking_lot::Mutex<...>>`
- updated the `record_key_access`, `clear_key_access`, `key_lru_millis`, `set_key_idle_seconds`, `set_key_frequency`, `key_frequency`, and `SWAPDB` metadata swap call sites to use the non-poisoning guard API

Validation while the candidate was live:
- `cargo test -p garnet-server db_scoped_object_access_metadata_does_not_leak_between_databases -- --nocapture`
- `cargo test -p garnet-server swapdb_zero_and_nonzero_preserves_object_access_metadata -- --nocapture`
- `cargo test -p garnet-server object_idletime_returns_integer_for_existing_key -- --nocapture`

Benchmark evidence:
- run 1: [comparison-run1.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.585-parking-lot-key-access-drop/comparison-run1.txt)
- run 2: [comparison-run2.txt](/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet/docs/performance/experiments/2026-03-24/11.585-parking-lot-key-access-drop/comparison-run2.txt)

Decision:
- drop

Reason:
- run 1 was clearly bad on the read path: `SET -2.36%`, `GET -0.18%`, `GET p99 +12.70%`
- the rerun recovered `SET` but still kept `GET` negative (`SET +3.81%`, `GET -0.75%`, p99 flat)
- because `GET` stayed below baseline in both runs, the lock-primitive swap is not an evidence-backed win for this workload
