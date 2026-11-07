/// A database-agnostic, offline-first synchronization engine for Dart and
/// Flutter.
///
/// Provides:
/// * `SyncPilotEngine` for optimistic local CRUD and automatic background sync.
/// * `OutboxQueue` with smart operation compaction (squashing redundant offline
///   mutations).
/// * `ConflictResolver` with built-in `serverWins`, `clientWins`,
///   `lastWriteWins`, `fieldLevelMerge`, and `custom` strategies.
/// * `LocalSyncStore` and `InMemorySyncStore` for storage abstraction.
/// * `RemoteSyncAdapter` and `CallbackRemoteSyncAdapter` for backend transport
///   abstraction.
/// * `RetryPolicy` with exponential backoff and jitter.
/// * `SyncCoordinator` for multi-entity relational synchronization.
library sync_pilot;

export 'src/conflict/conflict_resolver.dart';
export 'src/connectivity/connectivity_monitor.dart';
export 'src/engine/sync_coordinator.dart';
export 'src/engine/sync_pilot_engine.dart';
export 'src/models/sync_delta.dart';
export 'src/models/sync_operation.dart';
export 'src/models/sync_status.dart';
export 'src/policy/retry_policy.dart';
export 'src/queue/outbox_queue.dart';
export 'src/security/sync_payload_codec.dart';
export 'src/storage/key_value_sync_store.dart';
export 'src/storage/local_sync_store.dart';
export 'src/storage/sql_sync_store.dart';
export 'src/transport/remote_sync_adapter.dart';
export 'src/transport/rest_sync_adapter.dart';
