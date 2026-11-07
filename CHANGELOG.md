## 0.1.0

* Initial release of `sync_pilot`.
* Added `SyncPilotEngine<T>` for database-agnostic, offline-first synchronization.
* Added `OutboxQueue` with automatic operation compaction (squashing redundant creates, updates, and deletes while offline).
* Added optimistic temporary ID mapping (`temp_123` -> `srv_999`) with automatic rewriting of local records, subsequent outbox mutations, and cross-table foreign keys via `SyncCoordinator`.
* Added batch push support (`enableBatchPush`, `pushBatchSize`, and `RemoteSyncAdapter.pushBatch`).
* Added pluggable `ConflictResolver<T>` with built-in strategies:
  * `ServerWins`
  * `ClientWins`
  * `LastWriteWins`
  * `FieldLevelMerge` (3-way and changed-field JSON merging)
  * `Custom` callback resolver
* Added ready-to-use storage and transport adapters:
  * `RestSyncAdapter<T>` (REST/Dio/HTTP with partial `PATCH`, `409 Conflict` parsing, and retry classification)
  * `SqlSyncStore<T>` and `SqlOutboxStorage` (SQLite/`sqflite`/`sqlite3`/`Drift`)
  * `KeyValueSyncStore<T>` and `JsonStringOutboxStorage` (Hive/`SharedPreferences`)
* Added `SyncPayloadCodec` (`custom`, `xorBase64`, `chained` / `.then()`) for at-rest encryption and compression of local store and outbox payloads.
* Added paginated delta synchronization (`RemoteDeltaPage`) with checkpoint persistence.
* Added `RetryPolicy` with configurable exponential backoff and jitter.
* Added reactive streams for data observation (`watchAll`, `watchById`), engine status (`statusStream`), ID mappings (`idMappingStream`), and per-record sync state (`getRecordSyncState`).
