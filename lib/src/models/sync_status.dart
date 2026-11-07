import 'dart:collection';

import 'sync_operation.dart';

/// Event emitted when an optimistic client-generated temporary ID (e.g.
/// `'temp_123'`) is replaced by a canonical server-assigned ID (e.g.
/// `'srv_999'`) after a `create` operation succeeds.
class IdMappingEvent {
  /// Creates an [IdMappingEvent].
  const IdMappingEvent({
    required this.entityName,
    required this.temporaryId,
    required this.permanentId,
    required this.mappedAt,
  });

  /// Logical entity name where the ID replacement occurred.
  final String entityName;

  /// Client-generated temporary ID used while offline.
  final String temporaryId;

  /// Canonical permanent ID assigned by the remote server.
  final String permanentId;

  /// UTC timestamp when the mapping was recorded.
  final DateTime mappedAt;

  @override
  String toString() {
    return 'IdMappingEvent('
        'entity: $entityName, '
        '$temporaryId -> $permanentId'
        ')';
  }
}

/// Current execution phase of a `SyncPilotEngine`.
enum SyncPhase {
  /// The engine is online and idle (no sync currently running).
  idle,

  /// The engine is pushing pending outbox mutations to the remote server.
  pushing,

  /// The engine is pulling delta changes from the remote server.
  pulling,

  /// The engine is resolving conflicts between local and remote records.
  resolvingConflicts,

  /// The device is currently offline; mutations are queued locally.
  offline,

  /// The most recent sync cycle encountered an error.
  error,
}

/// Synchronization state of an individual local record.
///
/// Useful for rendering per-item UI indicators (e.g. a clock icon for pending
/// changes, a double-check icon for synced items, or a warning icon on error).
enum SyncRecordState {
  /// The record is fully synchronized with the remote server.
  synced,

  /// The record was created locally and is waiting to be pushed.
  pendingCreate,

  /// The record has local modifications waiting to be pushed.
  pendingUpdate,

  /// The record is marked for deletion and waiting to be pushed.
  pendingDelete,

  /// Pushing this record failed and exceeded the configured retry limit.
  failed,
}

/// Immutable snapshot of the engine's synchronization state.
class SyncStatusSnapshot {
  /// Creates a new [SyncStatusSnapshot].
  const SyncStatusSnapshot({
    required this.entityName,
    required this.phase,
    required this.isOnline,
    required this.pendingCount,
    required this.failedCount,
    this.lastSyncedAt,
    this.lastError,
  });

  /// Logical entity name managed by the engine.
  final String entityName;

  /// Current execution phase of the engine.
  final SyncPhase phase;

  /// Whether the engine currently has network connectivity.
  final bool isOnline;

  /// Number of pending mutations in the outbox waiting to be synced.
  final int pendingCount;

  /// Number of operations in the outbox that exhausted their retry limit.
  final int failedCount;

  /// UTC timestamp of the last successful sync cycle, if any.
  final DateTime? lastSyncedAt;

  /// Error description from the last failed sync attempt, if any.
  final String? lastError;

  /// Whether the engine is actively pushing, pulling, or resolving conflicts.
  bool get isSyncing =>
      phase == SyncPhase.pushing ||
      phase == SyncPhase.pulling ||
      phase == SyncPhase.resolvingConflicts;

  /// Whether all local mutations have been pushed and no errors are present.
  bool get isFullySynced => pendingCount == 0 && failedCount == 0 && !isSyncing;

  /// Returns a copy of this status snapshot with updated fields.
  SyncStatusSnapshot copyWith({
    String? entityName,
    SyncPhase? phase,
    bool? isOnline,
    int? pendingCount,
    int? failedCount,
    DateTime? lastSyncedAt,
    String? lastError,
    bool clearLastError = false,
  }) {
    return SyncStatusSnapshot(
      entityName: entityName ?? this.entityName,
      phase: phase ?? this.phase,
      isOnline: isOnline ?? this.isOnline,
      pendingCount: pendingCount ?? this.pendingCount,
      failedCount: failedCount ?? this.failedCount,
      lastSyncedAt: lastSyncedAt ?? this.lastSyncedAt,
      lastError: clearLastError ? null : (lastError ?? this.lastError),
    );
  }

  @override
  String toString() {
    return 'SyncStatusSnapshot('
        'entity: $entityName, '
        'phase: ${phase.name}, '
        'isOnline: $isOnline, '
        'pending: $pendingCount, '
        'failed: $failedCount, '
        'lastSyncedAt: $lastSyncedAt'
        ')';
  }
}

/// Summary report returned after a synchronization cycle completes.
class SyncCycleReport {
  /// Creates a new [SyncCycleReport].
  SyncCycleReport({
    required this.entityName,
    required this.pushedCount,
    required this.pulledUpsertedCount,
    required this.pulledDeletedCount,
    required this.compactedCount,
    required this.conflictsResolvedCount,
    required this.failedOperations,
    required this.duration,
    required this.completedAt,
    Map<String, String>? idMappings,
    this.skippedOffline = false,
  }) : idMappings = UnmodifiableMapView<String, String>(
          idMappings == null
              ? const <String, String>{}
              : Map<String, String>.of(idMappings),
        );

  /// Logical entity name that was synchronized.
  final String entityName;

  /// Number of outbox operations successfully pushed to the remote server.
  final int pushedCount;

  /// Number of remote records inserted or updated locally during delta pull.
  final int pulledUpsertedCount;

  /// Number of remote deletions applied locally during delta pull.
  final int pulledDeletedCount;

  /// Number of redundant offline operations eliminated via outbox compaction.
  final int compactedCount;

  /// Number of local/remote conflicts automatically or manually resolved.
  final int conflictsResolvedCount;

  /// Operations that failed during this cycle.
  final List<SyncOperation> failedOperations;

  /// Map of optimistic temporary IDs to canonical server-assigned IDs
  /// resolved during this sync cycle (`temporaryId -> permanentId`).
  final Map<String, String> idMappings;

  /// Total elapsed wall-clock time for the sync cycle.
  final Duration duration;

  /// UTC timestamp when the sync cycle finished.
  final DateTime completedAt;

  /// True if the sync cycle was skipped because the device was offline.
  final bool skippedOffline;

  /// True if no operations failed and the cycle was not skipped due to offline
  /// state.
  bool get isSuccess => !skippedOffline && failedOperations.isEmpty;

  @override
  String toString() {
    return 'SyncCycleReport('
        'entity: $entityName, '
        'pushed: $pushedCount, '
        'pulledUpserted: $pulledUpsertedCount, '
        'pulledDeleted: $pulledDeletedCount, '
        'compacted: $compactedCount, '
        'conflictsResolved: $conflictsResolvedCount, '
        'idMappings: ${idMappings.length}, '
        'failed: ${failedOperations.length}, '
        'duration: ${duration.inMilliseconds}ms'
        ')';
  }
}
