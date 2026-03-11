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

This is intentionally a first slice, not the end state.

## Remaining work

The remaining helper removal should proceed in this order:

1. Convert string/object write, delete, expiration, and snapshot helpers to `DbKeyRef` or `DbScopedKey`.
2. Remove `current_auxiliary_db_name()` branching from storage helpers.
3. Thread `DbName` through command invocation context so command handlers stop pulling it from thread-local request state.
4. Convert blocking / background / migration paths to carry DB in their key structs instead of reconstructing it ad hoc.
5. Delete obsolete implicit-selected-DB helpers once all internal callers are migrated.

## Acceptance criteria

The lane is complete only when all of the following are true:

- command/storage helper APIs that touch memory state require explicit DB
- no internal helper defaults to DB0
- ambient selected-DB lookup is confined to true external boundaries or removed entirely
- MultiDB regressions are covered by exact Rust tests before targeted external reruns
