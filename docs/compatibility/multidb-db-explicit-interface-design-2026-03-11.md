# MultiDB DB-Explicit Interface Design

Date: 2026-03-11
Status: In progress
Scope: `garnet-rs` request-lifecycle / string-store / object-store MultiDB paths

## Problem

MultiDB bug fixes have repeatedly exposed the same structural issue:

- command and storage helpers can still discover the active DB indirectly
- some helpers branch on `current_request_selected_db()` or `current_auxiliary_db_name()`
- that makes DB0 fallback possible even when the caller logically meant `DbName(9)` or another nonzero DB

This is not a one-off bug. It is an interface problem.

## Hard rules

- Memory-state access must be DB-explicit.
- No internal API may default to DB0 by construction.
- Selected-DB peeking helpers are not an acceptable long-term interface.
- `(DbName, key-bytes)` is a domain value and should be carried as such.

## Domain types

This slice introduces the first explicit key type pair:

- `DbName`
- `KeyBytes = [u8]`
- `DbKeyRef<'a> { db: DbName, key: &'a KeyBytes }`

Owned keys already exist as `DbScopedKey`.

Design intent:

- borrowed paths use `DbKeyRef`
- owned paths use `DbScopedKey`
- low-level read/write/object helpers take one of those two instead of inferring DB from ambient state

## Inventory

Snapshot taken on 2026-03-11 after the first mechanical slice:

- `current_request_selected_db(...)`: `161` call sites
- `current_auxiliary_db_name(...)`: `29` call sites

Snapshot taken on 2026-03-11 after the second mechanical slice:

- `current_request_selected_db(...)`: `205` call sites
- `current_auxiliary_db_name(...)`: `24` call sites

Snapshot taken on 2026-03-11 after the third mechanical slice:

- `current_request_selected_db(...)`: `288` call sites
- `current_auxiliary_db_name(...)`: `22` call sites

Snapshot taken on 2026-03-11 after the fourth mechanical slice:

- `current_request_selected_db(...)`: `314` call sites
- `current_auxiliary_db_name(...)`: `20` call sites

Snapshot taken on 2026-03-11 after the fifth mechanical slice:

- `current_request_selected_db(...)`: `358` call sites
- `current_auxiliary_db_name(...)`: `13` call sites

Snapshot taken on 2026-03-11 after the sixth mechanical slice:

- `current_request_selected_db(...)`: `368` call sites
- `current_auxiliary_db_name(...)`: `0` call sites

Snapshot taken on 2026-03-11 after the seventh mechanical slice:

- `current_request_selected_db(...)`: `368` call sites
- `current_auxiliary_db_name(...)`: `0` call sites

Snapshot taken on 2026-03-11 after the eighth mechanical slice:

- `current_request_selected_db(...)`: `400` call sites
- `current_auxiliary_db_name(...)`: `0` call sites

Interpretation:

- `current_auxiliary_db_name(...)` is the storage-helper peeking metric we want to drive down.
- `current_request_selected_db(...)` can temporarily increase while DB lookup is being pushed outward from helpers to command boundaries.
- The goal is not a raw grep minimum by itself; the goal is to eliminate hidden DB rediscovery in memory-state helpers.

High-value buckets:

1. Command-layer read call sites that already know the logical DB but still fetch it indirectly.
2. String/object write and delete helpers that still branch on ambient selected DB.
3. Migration/admin paths that use `with_selected_db(...)` only to let a nested helper rediscover the DB.
4. Blocking / reprocessing / background maintenance paths that operate on keys without carrying DB in the key type.

## What landed in this slice

First mechanical slice:

- `read_string_value`, `key_exists`, `object_key_exists`, `key_exists_any`, `key_exists_any_without_expiring` now require `DbKeyRef`
- `object_read_raw`, `has_set_hot_entry`, `set_hot_payload_for_object_read`, `object_read` now require `DbKeyRef`
- call sites were migrated to pass explicit `(DbName, key)` pairs
- obvious `with_selected_db(... current_request_selected_db())` cases were removed in favor of directly passing the known `DbName`
- regression added to prove explicit `DbKeyRef` reads do not fall back to DB0

Second mechanical slice:

- `write_string_value`, `delete_string_value`, `upsert_string_value_for_migration`, `delete_string_key_for_migration`, `expiration_unix_millis`, and `rewrite_existing_value_expiration` now require `DbKeyRef`
- mutation / TTL call sites in string commands, migration, restore/debug-reload, blocking wait readiness, and replication rewrite helpers were migrated
- regression added to prove explicit `DbKeyRef` mutation and TTL paths do not fall back to DB0

Third mechanical slice:

- `object_upsert_raw`, `object_delete_raw`, `take_set_hot_entry`, `upsert_set_hot_entry`, `with_set_hot_entry`, `mark_set_hot_entry_dirty`, `object_upsert`, and `object_delete` now require `DbKeyRef`
- object mutation / delete call sites in hash/list/set/zset/geo/stream/string commands, migration, restore/debug-reload, and admin paths were migrated
- regression added to prove explicit `DbKeyRef` object mutation and deletion do not fall back to DB0

Fourth mechanical slice:

- `set_string_expiration_metadata_in_shard`, `set_string_expiration_deadline_in_shard`, `set_string_expiration_deadline`, `remove_string_key_metadata_in_shard`, `remove_string_key_metadata`, `string_expiration_deadline_in_shard`, and `string_expiration_deadline` now require `DbKeyRef`
- string expiration metadata call sites in GETEX/PERSIST/EXPIRETIME, eviction/admin, object deletes, migration, and helper tests were migrated
- regression added to prove explicit `DbKeyRef` expiration metadata paths do not fall back to DB0

Fifth mechanical slice:

- `set_hash_field_expiration_unix_millis_in_shard`, `set_hash_field_expiration_unix_millis`, `hash_field_expiration_unix_millis`, `has_hash_field_expirations_for_key`, `snapshot_hash_field_expirations_for_key`, `restore_hash_field_expirations_for_key`, `clear_hash_field_expirations_for_key_in_shard`, `remove_expired_hash_fields_for_access`, and `remove_all_expired_hash_fields_for_key` now require `DbKeyRef`
- hash-field expiration call sites in hash commands, admin snapshot/reload, object deletes, and helper tests were migrated
- regression added to prove explicit `DbKeyRef` hash-field expiration metadata paths do not fall back to DB0

Sixth mechanical slice:

- deleted `current_auxiliary_db_name()` entirely instead of preserving it as a wrapper
- migrated the remaining 13 helper/command branches in `migration.rs`, `server_commands.rs`, `string_commands.rs`, and `string_store.rs` to explicit boundary-local `selected_db` checks
- storage/helper ambient-DB peeking metric is now zero; the remaining debt is boundary code that still calls `current_request_selected_db()` inside internal execution paths

Seventh mechanical slice:

- removed the default-DB production invoke surface by replacing `RequestProcessor::execute(...)` with explicit `execute_in_db(...)`
- deleted the non-DB `execute_with_client_context(...)`, `execute_with_client_no_touch(...)`, and `execute_with_client_no_touch_in_transaction(...)` wrappers
- updated AOF replay to carry `selected_db` explicitly across replayed frames and to persist `SELECT` across operations instead of re-entering DB0 for every command
- added explicit-DB harness helpers for DB-explicit and auxiliary-db regressions so test coverage no longer relies on ambient selected-db state

Eighth mechanical slice:

- changed queued/background helper surfaces to accept explicit DB-scoped inputs:
  - lazy-expire replication queue now enqueues `DbKeyRef` / `DbScopedKey`
  - script replication effects now take explicit `selected_db`
  - `string_value_len_for_replication` and `key_type_snapshot_for_setkey_overwrite` now require `DbKeyRef`
- keyed debug/encoding side state by `DbScopedKey` instead of bare key bytes:
  - `forced_list_quicklist_keys`
  - `forced_raw_string_keys`
  - `forced_set_encoding_floors`
  - `set_debug_ht_state`
- fixed active-expire / auxiliary lazy-expire paths to propagate object delete, watch invalidation, and replication with explicit DBs instead of ambient request DB
- added exact regressions for auxiliary lazy-expire queue DB, script replication selected DB, and DB-scoped encoding-state isolation

Important interpretation:

- the raw `current_request_selected_db(...)` grep count rose in this slice because several direct command call sites now construct `DbKeyRef::new(current_request_selected_db(), ...)` at the boundary instead of letting deeper helpers rediscover DB implicitly
- that is an acceptable intermediate step; the bad pattern is hidden DB lookup inside shared helpers and queued/background paths
- the next slices should now attack the remaining helper families directly, not optimize the grep count cosmetically

This is still not the end state.

## Remaining work

The remaining helper removal should proceed in this order:

1. Convert watch-version, keyspace-notification, and blocking-readiness helpers to accept explicit DB-scoped inputs instead of reading thread-local selected DB.
2. Delete residual internal `current_request_selected_db()` peeks once those helper families no longer need ambient request context.
3. Tighten any remaining test-only harnesses that still default to DB0 when they are meant to exercise nonzero DB behavior.

## Acceptance criteria

The lane is complete only when all of the following are true:

- command/storage helper APIs that touch memory state require explicit DB
- no internal helper defaults to DB0
- ambient selected-DB lookup is confined to true external boundaries or removed entirely
- MultiDB regressions are covered by exact Rust tests before targeted external reruns
