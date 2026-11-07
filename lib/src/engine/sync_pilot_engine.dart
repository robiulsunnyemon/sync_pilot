import 'dart:async';
import 'dart:collection';

import '../conflict/conflict_resolver.dart';
import '../connectivity/connectivity_monitor.dart';
import '../models/sync_delta.dart';
import '../models/sync_operation.dart';
import '../models/sync_status.dart';
import '../policy/retry_policy.dart';
import '../queue/outbox_queue.dart';
import '../storage/local_sync_store.dart';
import '../transport/remote_sync_adapter.dart';

/// A database-agnostic, offline-first synchronization engine for an entity
/// type [T].
///
/// `SyncPilotEngine` coordinates optimistic local reads/writes, outbox queuing
/// with automatic operation compaction, optimistic temporary ID mapping
/// (`temp_123` -> `srv_999`), batch pushing, conflict resolution, exponential
/// backoff retries, and incremental delta pulling.
class SyncPilotEngine<T> {
  /// Creates a new [SyncPilotEngine] for [entityName].
  SyncPilotEngine({
    required this.entityName,
    required this.getId,
    required this.toJson,
    required this.fromJson,
    required this.localStore,
    required this.remoteAdapter,
    OutboxQueue? outbox,
    ConflictResolver<T>? conflictResolver,
    ConnectivityMonitor? connectivityMonitor,
    this.retryPolicy = const RetryPolicy(),
    this.syncInterval,
    this.autoSyncOnMutation = false,
    this.autoSyncOnReconnect = true,
    this.enableBatchPush = false,
    this.pushBatchSize = 20,
    this.maxDeltaPagesPerSync = 50,
  })  : assert(pushBatchSize > 0, 'pushBatchSize must be greater than 0'),
        outbox = outbox ?? OutboxQueue(entityName: entityName),
        conflictResolver = conflictResolver ?? ConflictResolver<T>.serverWins(),
        connectivityMonitor =
            connectivityMonitor ?? ManualConnectivityMonitor() {
    _init();
  }

  /// Logical collection or table name (e.g. `'todos'`, `'orders'`).
  final String entityName;

  /// Extracts the primary key string from an entity instance.
  final String Function(T item) getId;

  /// Serializes an entity instance to a JSON map.
  final Map<String, Object?> Function(T item) toJson;

  /// Deserializes an entity instance from a JSON map.
  final T Function(Map<String, Object?> json) fromJson;

  /// Local database adapter providing instant offline reads and writes.
  final LocalSyncStore<T> localStore;

  /// Remote transport adapter for pushing mutations and pulling deltas.
  final RemoteSyncAdapter<T> remoteAdapter;

  /// Durable outbox queue managing pending offline mutations.
  final OutboxQueue outbox;

  /// Strategy used when local and remote versions of a record conflict.
  final ConflictResolver<T> conflictResolver;

  /// Connectivity detector used to pause/resume synchronization.
  final ConnectivityMonitor connectivityMonitor;

  /// Retry policy governing transient push failures.
  final RetryPolicy retryPolicy;

  /// Optional interval for periodic background synchronization while online.
  final Duration? syncInterval;

  /// Whether to trigger a non-blocking background `sync()` immediately after
  /// every local `create`, `update`, `save`, or `delete` when online.
  final bool autoSyncOnMutation;

  /// Whether to automatically trigger `sync()` when connectivity transitions
  /// from offline to online.
  final bool autoSyncOnReconnect;

  /// Whether to push pending outbox operations in bulk batches using
  /// `RemoteSyncAdapter.pushBatch`.
  final bool enableBatchPush;

  /// Maximum number of operations to include in a single batch push call when
  /// [enableBatchPush] is `true`.
  final int pushBatchSize;

  /// Maximum number of delta pages to pull in a single sync cycle.
  final int maxDeltaPagesPerSync;

  final StreamController<SyncStatusSnapshot> _statusController =
      StreamController<SyncStatusSnapshot>.broadcast();
  final StreamController<IdMappingEvent> _idMappingController =
      StreamController<IdMappingEvent>.broadcast();
  final Map<String, String> _resolvedIdMappings = <String, String>{};

  late SyncStatusSnapshot _currentStatus;
  StreamSubscription<bool>? _connectivitySubscription;
  Timer? _periodicTimer;
  Future<SyncCycleReport>? _activeSyncFuture;
  bool _isDisposed = false;

  void _init() {
    _currentStatus = SyncStatusSnapshot(
      entityName: entityName,
      phase: SyncPhase.idle,
      isOnline: true,
      pendingCount: 0,
      failedCount: 0,
    );

    _connectivitySubscription =
        connectivityMonitor.onConnectivityChanged.listen((online) async {
      await _refreshStatus(
        phase: online ? SyncPhase.idle : SyncPhase.offline,
        isOnline: online,
      );
      if (online && autoSyncOnReconnect && !_isDisposed) {
        unawaited(sync());
      }
    });

    final interval = syncInterval;
    if (interval != null && interval > Duration.zero) {
      _periodicTimer = Timer.periodic(interval, (_) {
        if (!_isDisposed) {
          unawaited(sync());
        }
      });
    }
  }

  /// Current synchronization status snapshot.
  SyncStatusSnapshot get currentStatus => _currentStatus;

  /// Reactive stream of [SyncStatusSnapshot] updates.
  ///
  /// Immediately emits [currentStatus] upon subscription.
  Stream<SyncStatusSnapshot> get statusStream async* {
    yield _currentStatus;
    yield* _statusController.stream;
  }

  /// Stream of [IdMappingEvent] notifications emitted whenever an optimistic
  /// client-generated temporary ID (e.g. `'temp_123'`) is replaced by a
  /// server-assigned permanent ID (e.g. `'srv_999'`).
  Stream<IdMappingEvent> get idMappingStream => _idMappingController.stream;

  /// Unmodifiable map of all temporary-to-permanent ID mappings resolved by
  /// this engine (`temporaryId -> permanentId`).
  Map<String, String> get resolvedIdMappings =>
      UnmodifiableMapView<String, String>(_resolvedIdMappings);

  /// Resolves [id] through [resolvedIdMappings], returning the permanent
  /// server ID if [id] was an optimistic temporary ID that has since been
  /// mapped, or [id] itself otherwise.
  String resolveId(String id) => _resolvedIdMappings[id] ?? id;

  /// Retrieves a single entity by [id] from local storage (automatically
  /// resolving [id] if it was a mapped temporary ID).
  Future<T?> getById(String id) => localStore.getById(resolveId(id));

  /// Retrieves all entities from local storage.
  Future<List<T>> getAll() => localStore.getAll();

  /// Reactive stream of all locally stored entities.
  Stream<List<T>> watchAll() => localStore.watchAll();

  /// Reactive stream of a single entity by [id].
  Stream<T?> watchById(String id) => localStore.watchById(resolveId(id));

  /// Returns the synchronization state of the record with [id].
  Future<SyncRecordState> getRecordSyncState(String id) async {
    final effectiveId = resolveId(id);
    final op = await outbox.getForEntity(effectiveId);
    if (op == null) return SyncRecordState.synced;
    if (!retryPolicy.canRetry(op.retryCount) && op.lastError != null) {
      return SyncRecordState.failed;
    }
    return switch (op.type) {
      SyncOperationType.create => SyncRecordState.pendingCreate,
      SyncOperationType.update => SyncRecordState.pendingUpdate,
      SyncOperationType.delete => SyncRecordState.pendingDelete,
    };
  }

  /// Saves [item] locally and queues it for synchronization.
  ///
  /// Automatically determines whether the record is a new creation or an update
  /// to an existing local record.
  Future<EnqueueResult> save(
    T item, {
    Map<String, Object?>? metadata,
  }) async {
    final id = getId(item);
    final existing = await localStore.getById(id);
    if (existing == null) {
      return create(item, metadata: metadata);
    } else {
      return update(item, metadata: metadata);
    }
  }

  /// Optimistically inserts a newly created [item] into local storage and
  /// enqueues a `create` operation in the outbox.
  Future<EnqueueResult> create(
    T item, {
    Map<String, Object?>? metadata,
  }) async {
    final id = getId(item);
    final payload = toJson(item);
    await localStore.upsert(item);

    final result = await outbox.enqueue(
      entityId: id,
      type: SyncOperationType.create,
      payload: payload,
      changedFields: payload.keys.toSet(),
      metadata: metadata,
    );

    await _refreshStatus();
    _maybeTriggerAutoSync();
    return result;
  }

  /// Optimistically updates [item] in local storage and enqueues an `update`
  /// operation in the outbox (automatically compacting with any prior queued
  /// operation for the same ID).
  ///
  /// If [changedFields] is omitted, `SyncPilotEngine` automatically diffs
  /// [item] against the existing local record to determine which fields were
  /// modified.
  Future<EnqueueResult> update(
    T item, {
    Set<String>? changedFields,
    Map<String, Object?>? metadata,
  }) async {
    final id = getId(item);
    final existing = await localStore.getById(id);
    final basePayload = existing != null ? toJson(existing) : null;
    final newPayload = toJson(item);

    final effectiveChangedFields = changedFields ??
        _computeChangedFields(basePayload: basePayload, newPayload: newPayload);

    await localStore.upsert(item);

    final result = await outbox.enqueue(
      entityId: id,
      type: SyncOperationType.update,
      payload: newPayload,
      basePayload: basePayload,
      changedFields: effectiveChangedFields,
      metadata: metadata,
    );

    await _refreshStatus();
    _maybeTriggerAutoSync();
    return result;
  }

  /// Optimistically deletes the record with [id] from local storage and
  /// enqueues a `delete` operation in the outbox.
  ///
  /// If the record was created offline and had not yet been pushed to the
  /// server, outbox compaction cancels both operations automatically.
  Future<EnqueueResult> delete(
    String id, {
    Map<String, Object?>? metadata,
  }) async {
    final effectiveId = resolveId(id);
    await localStore.deleteById(effectiveId);

    final result = await outbox.enqueue(
      entityId: effectiveId,
      type: SyncOperationType.delete,
      metadata: metadata,
    );

    await _refreshStatus();
    _maybeTriggerAutoSync();
    return result;
  }

  /// Propagates an optimistic temporary-to-permanent ID mapping (`oldId` ->
  /// `newId`) across both the local database and the outbox queue.
  ///
  /// Rewrites any matching primary keys or foreign-key references (e.g.
  /// `projectId: 'temp_123'` -> `projectId: 'srv_999'`).
  ///
  /// Returns the number of local records and outbox operations updated.
  Future<int> propagateIdMapping({
    required String oldId,
    required String newId,
  }) async {
    if (oldId == newId) return 0;
    _resolvedIdMappings[oldId] = newId;

    final outboxModified = await outbox.replaceIdReferences(
      oldId: oldId,
      newId: newId,
    );

    var localModified = 0;
    final allLocal = await localStore.getAll();
    final updatedItems = <T>[];

    for (final item in allLocal) {
      final itemJson = toJson(item);
      final (rewrittenJson, changed) = _replaceStringInMap(
        itemJson,
        oldId: oldId,
        newId: newId,
      );
      if (changed) {
        final previousItemId = getId(item);
        final updatedItem = fromJson(rewrittenJson);
        final newItemId = getId(updatedItem);
        if (previousItemId != newItemId) {
          await localStore.deleteById(previousItemId);
        }
        updatedItems.add(updatedItem);
        localModified++;
      }
    }

    if (updatedItems.isNotEmpty) {
      await localStore.upsertBatch(updatedItems);
    }

    return outboxModified + localModified;
  }

  /// Resets any failed operations in the outbox and triggers a synchronization
  /// cycle.
  Future<SyncCycleReport> retryFailedOperations() async {
    await outbox.resetFailedOperations(maxRetries: retryPolicy.maxRetries);
    await _refreshStatus();
    return sync();
  }

  /// Executes a complete synchronization cycle:
  /// 1. Pushes pending outbox mutations (individually or in batches) in FIFO
  ///    order with retry, temporary ID mapping, and conflict resolution.
  /// 2. Pulls paginated delta changes from the remote server and updates the
  ///    local store and sync checkpoint.
  ///
  /// Concurrent calls to [sync] are coalesced onto the active cycle.
  Future<SyncCycleReport> sync() {
    final existing = _activeSyncFuture;
    if (existing != null) {
      return existing;
    }

    final future = _runSyncCycle();
    _activeSyncFuture = future;
    return future.whenComplete(() {
      _activeSyncFuture = null;
    });
  }

  Future<SyncCycleReport> _runSyncCycle() async {
    final stopwatch = Stopwatch()..start();
    final online = await connectivityMonitor.isOnline;

    if (!online) {
      await _refreshStatus(phase: SyncPhase.offline, isOnline: false);
      stopwatch.stop();
      return SyncCycleReport(
        entityName: entityName,
        pushedCount: 0,
        pulledUpsertedCount: 0,
        pulledDeletedCount: 0,
        compactedCount: outbox.takeCompactedCount(),
        conflictsResolvedCount: 0,
        failedOperations: const <SyncOperation>[],
        duration: stopwatch.elapsed,
        completedAt: DateTime.now().toUtc(),
        skippedOffline: true,
      );
    }

    var pushedCount = 0;
    var pulledUpsertedCount = 0;
    var pulledDeletedCount = 0;
    var conflictsResolvedCount = 0;
    final failedOperations = <SyncOperation>[];
    final cycleIdMappings = <String, String>{};
    String? cycleError;

    try {
      // -----------------------------------------------------------------------
      // PHASE 1: PUSH OUTBOX MUTATIONS
      // -----------------------------------------------------------------------
      await _refreshStatus(
        phase: SyncPhase.pushing,
        isOnline: true,
        clearLastError: true,
      );

      if (enableBatchPush) {
        final batchOutcome = await _pushOutboxInBatches(cycleIdMappings);
        pushedCount += batchOutcome.pushed;
        conflictsResolvedCount += batchOutcome.conflictsResolved;
        failedOperations.addAll(batchOutcome.failedOperations);
        if (batchOutcome.failedOperations.isNotEmpty) {
          cycleError ??= batchOutcome.failedOperations.first.lastError;
        }
      } else {
        // Re-read from outbox on each step so temporary ID rewrites immediately
        // apply to subsequent operations in the queue.
        final processedOpIds = <String>{};
        while (true) {
          final currentOps = await outbox.getAll();
          SyncOperation? nextOp;
          for (final candidate in currentOps) {
            if (!processedOpIds.contains(candidate.id)) {
              nextOp = candidate;
              break;
            }
          }
          if (nextOp == null) break;
          processedOpIds.add(nextOp.id);

          if (!retryPolicy.canRetry(nextOp.retryCount) &&
              nextOp.lastError != null) {
            failedOperations.add(nextOp);
            continue;
          }

          final outcome = await _pushSingleOperationWithRetry(
            nextOp,
            cycleIdMappings: cycleIdMappings,
          );
          pushedCount += outcome.pushed;
          conflictsResolvedCount += outcome.conflictsResolved;
          if (outcome.failedOperation != null) {
            failedOperations.add(outcome.failedOperation!);
            cycleError ??= outcome.failedOperation!.lastError;
          }
        }
      }

      // -----------------------------------------------------------------------
      // PHASE 2: PULL REMOTE DELTAS
      // -----------------------------------------------------------------------
      await _refreshStatus(phase: SyncPhase.pulling, isOnline: true);

      var checkpoint = await localStore.getSyncCheckpoint(entityName);
      var pageCount = 0;
      var hasMore = true;

      while (hasMore && pageCount < maxDeltaPagesPerSync) {
        pageCount++;
        final deltaPage = await remoteAdapter.pullDelta(checkpoint: checkpoint);

        // Protect any record that still has an unpushed local mutation.
        final remainingOutbox = await outbox.getAll();
        final dirtyEntityIds = remainingOutbox.map((op) => op.entityId).toSet();

        final safeUpserts = <T>[];
        for (final remoteItem in deltaPage.upserted) {
          final itemId = getId(remoteItem);
          if (dirtyEntityIds.contains(itemId)) {
            final pendingOp = remainingOutbox.firstWhere(
              (op) => op.entityId == itemId,
            );
            final resolved = await _resolvePullConflict(
              pendingOp: pendingOp,
              remoteItem: remoteItem,
            );
            if (resolved) {
              conflictsResolvedCount++;
            }
          } else {
            safeUpserts.add(remoteItem);
          }
        }

        if (safeUpserts.isNotEmpty) {
          await localStore.upsertBatch(safeUpserts);
          pulledUpsertedCount += safeUpserts.length;
        }

        for (final deletedId in deltaPage.deletedIds) {
          if (!dirtyEntityIds.contains(deletedId)) {
            final removed = await localStore.deleteById(deletedId);
            if (removed) {
              pulledDeletedCount++;
            }
          }
        }

        if (deltaPage.nextCheckpoint != null) {
          checkpoint = deltaPage.nextCheckpoint;
          await localStore.saveSyncCheckpoint(entityName, checkpoint);
        }

        hasMore = deltaPage.hasMore;
      }
    } catch (error) {
      cycleError = error.toString();
    }

    stopwatch.stop();
    final completedAt = DateTime.now().toUtc();

    await _refreshStatus(
      phase: cycleError == null ? SyncPhase.idle : SyncPhase.error,
      isOnline: true,
      lastSyncedAt: cycleError == null ? completedAt : null,
      lastError: cycleError,
      clearLastError: cycleError == null,
    );

    return SyncCycleReport(
      entityName: entityName,
      pushedCount: pushedCount,
      pulledUpsertedCount: pulledUpsertedCount,
      pulledDeletedCount: pulledDeletedCount,
      compactedCount: outbox.takeCompactedCount(),
      conflictsResolvedCount: conflictsResolvedCount,
      failedOperations: List<SyncOperation>.unmodifiable(failedOperations),
      idMappings: cycleIdMappings,
      duration: stopwatch.elapsed,
      completedAt: completedAt,
    );
  }

  Future<_BatchPushOutcome> _pushOutboxInBatches(
    Map<String, String> cycleIdMappings,
  ) async {
    var totalPushed = 0;
    var totalConflictsResolved = 0;
    final failedOperations = <SyncOperation>[];
    final processedOpIds = <String>{};

    while (true) {
      final allOps = await outbox.getAll();
      final batch = <SyncOperation>[];

      for (final op in allOps) {
        if (processedOpIds.contains(op.id)) continue;
        if (!retryPolicy.canRetry(op.retryCount) && op.lastError != null) {
          processedOpIds.add(op.id);
          failedOperations.add(op);
          continue;
        }
        batch.add(op);
        if (batch.length >= pushBatchSize) break;
      }

      if (batch.isEmpty) break;
      for (final op in batch) {
        processedOpIds.add(op.id);
      }

      final results = await remoteAdapter.pushBatch(batch);
      if (results.length != batch.length) {
        throw StateError(
          'RemoteSyncAdapter.pushBatch returned ${results.length} results '
          'for a batch of ${batch.length} operations.',
        );
      }

      for (var i = 0; i < batch.length; i++) {
        final op = batch[i];
        final result = results[i];
        final outcome = await _processPushResult(
          op: op,
          initialResult: result,
          cycleIdMappings: cycleIdMappings,
        );
        totalPushed += outcome.pushed;
        totalConflictsResolved += outcome.conflictsResolved;
        if (outcome.failedOperation != null) {
          failedOperations.add(outcome.failedOperation!);
        }
      }
    }

    return _BatchPushOutcome(
      pushed: totalPushed,
      conflictsResolved: totalConflictsResolved,
      failedOperations: failedOperations,
    );
  }

  Future<_PushOutcome> _pushSingleOperationWithRetry(
    SyncOperation op, {
    required Map<String, String> cycleIdMappings,
  }) async {
    final initialResult = await remoteAdapter.pushOperation(op);
    return _processPushResult(
      op: op,
      initialResult: initialResult,
      cycleIdMappings: cycleIdMappings,
    );
  }

  Future<_PushOutcome> _processPushResult({
    required SyncOperation op,
    required PushOperationResult<T> initialResult,
    required Map<String, String> cycleIdMappings,
  }) async {
    var currentOp = op;
    var result = initialResult;

    while (true) {
      switch (result) {
        case PushSuccess<T>(:final serverRecord):
          await outbox.removeById(currentOp.id);

          if (serverRecord != null &&
              currentOp.type != SyncOperationType.delete) {
            final serverId = getId(serverRecord);
            final originalId = currentOp.entityId;

            if (serverId != originalId) {
              // Optimistic temporary ID was replaced by a canonical server ID!
              await localStore.deleteById(originalId);
              await localStore.upsert(serverRecord);
              await propagateIdMapping(oldId: originalId, newId: serverId);

              cycleIdMappings[originalId] = serverId;
              if (!_idMappingController.isClosed) {
                _idMappingController.add(
                  IdMappingEvent(
                    entityName: entityName,
                    temporaryId: originalId,
                    permanentId: serverId,
                    mappedAt: DateTime.now().toUtc(),
                  ),
                );
              }
            } else {
              await localStore.upsert(serverRecord);
            }
          }
          return const _PushOutcome(pushed: 1, conflictsResolved: 0);

        case PushConflict<T>(:final remoteRecord):
          await _refreshStatus(
            phase: SyncPhase.resolvingConflicts,
            isOnline: true,
          );
          return _handlePushConflict(
            op: currentOp,
            remoteRecord: remoteRecord,
          );

        case PushPermanentFailure<T>(:final message):
          final failedOp = currentOp.copyWith(
            retryCount: retryPolicy.maxRetries + 1,
            lastError: message,
          );
          await outbox.updateOperation(failedOp);
          return _PushOutcome(
            pushed: 0,
            conflictsResolved: 0,
            failedOperation: failedOp,
          );

        case PushRetryableFailure<T>(:final message):
          final nextRetryCount = currentOp.retryCount + 1;
          currentOp = currentOp.copyWith(
            retryCount: nextRetryCount,
            lastError: message,
          );
          await outbox.updateOperation(currentOp);

          if (!retryPolicy.canRetry(nextRetryCount)) {
            return _PushOutcome(
              pushed: 0,
              conflictsResolved: 0,
              failedOperation: currentOp,
            );
          }

          final delay = retryPolicy.delayForAttempt(nextRetryCount);
          if (delay > Duration.zero) {
            await Future<void>.delayed(delay);
          }
          result = await remoteAdapter.pushOperation(currentOp);
      }
    }
  }

  Future<_PushOutcome> _handlePushConflict({
    required SyncOperation op,
    required T remoteRecord,
  }) async {
    final localItem = await localStore.getById(op.entityId);
    final localJson = localItem != null ? toJson(localItem) : op.payload;
    final remoteJson = toJson(remoteRecord);

    final context = ConflictContext<T>(
      entityName: entityName,
      entityId: op.entityId,
      pendingOperation: op,
      localItem: localItem,
      remoteItem: remoteRecord,
      localJson: localJson,
      remoteJson: remoteJson,
      baseJson: op.basePayload,
    );

    final resolution = await conflictResolver.resolve(context);

    switch (resolution) {
      case UseRemote<T>(:final remoteRecord):
        await localStore.upsert(remoteRecord);
        await outbox.removeById(op.id);
        return const _PushOutcome(pushed: 0, conflictsResolved: 1);

      case UseLocal<T>(:final localRecord):
        await localStore.upsert(localRecord);
        final resolvedOp = op.copyWith(
          payload: toJson(localRecord),
          metadata: <String, Object?>{
            ...op.metadata,
            'conflictResolved': true,
            'resolutionStrategy': 'useLocal',
          },
        );
        final retryResult = await remoteAdapter.pushOperation(resolvedOp);
        if (retryResult case PushSuccess<T>(:final serverRecord)) {
          if (serverRecord != null) {
            await localStore.upsert(serverRecord);
          }
          await outbox.removeById(op.id);
          return const _PushOutcome(pushed: 1, conflictsResolved: 1);
        }
        final failed = resolvedOp.copyWith(
          retryCount: retryPolicy.maxRetries + 1,
          lastError: 'Failed to push local record after conflict resolution.',
        );
        await outbox.updateOperation(failed);
        return _PushOutcome(
          pushed: 0,
          conflictsResolved: 1,
          failedOperation: failed,
        );

      case UseMerged<T>(:final mergedRecord):
        await localStore.upsert(mergedRecord);
        final mergedPayload = toJson(mergedRecord);
        final resolvedOp = op.copyWith(
          payload: mergedPayload,
          changedFields: mergedPayload.keys.toSet(),
          metadata: <String, Object?>{
            ...op.metadata,
            'conflictResolved': true,
            'resolutionStrategy': 'useMerged',
          },
        );
        final retryResult = await remoteAdapter.pushOperation(resolvedOp);
        if (retryResult case PushSuccess<T>(:final serverRecord)) {
          if (serverRecord != null) {
            await localStore.upsert(serverRecord);
          }
          await outbox.removeById(op.id);
          return const _PushOutcome(pushed: 1, conflictsResolved: 1);
        }
        final failed = resolvedOp.copyWith(
          retryCount: retryPolicy.maxRetries + 1,
          lastError: 'Failed to push merged record after conflict resolution.',
        );
        await outbox.updateOperation(failed);
        return _PushOutcome(
          pushed: 0,
          conflictsResolved: 1,
          failedOperation: failed,
        );

      case DeferConflict<T>(:final reason):
        final deferredOp = op.copyWith(
          retryCount: retryPolicy.maxRetries + 1,
          lastError: reason ?? 'Conflict deferred for manual resolution.',
        );
        await outbox.updateOperation(deferredOp);
        return _PushOutcome(
          pushed: 0,
          conflictsResolved: 0,
          failedOperation: deferredOp,
        );
    }
  }

  Future<bool> _resolvePullConflict({
    required SyncOperation pendingOp,
    required T remoteItem,
  }) async {
    final localItem = await localStore.getById(pendingOp.entityId);
    final context = ConflictContext<T>(
      entityName: entityName,
      entityId: pendingOp.entityId,
      pendingOperation: pendingOp,
      localItem: localItem,
      remoteItem: remoteItem,
      localJson: localItem != null ? toJson(localItem) : pendingOp.payload,
      remoteJson: toJson(remoteItem),
      baseJson: pendingOp.basePayload,
    );

    final resolution = await conflictResolver.resolve(context);
    switch (resolution) {
      case UseRemote<T>(:final remoteRecord):
        await localStore.upsert(remoteRecord);
        await outbox.removeById(pendingOp.id);
        return true;
      case UseLocal<T>(:final localRecord):
        await localStore.upsert(localRecord);
        return true;
      case UseMerged<T>(:final mergedRecord):
        await localStore.upsert(mergedRecord);
        final mergedJson = toJson(mergedRecord);
        await outbox.updateOperation(
          pendingOp.copyWith(
            payload: mergedJson,
            changedFields: mergedJson.keys.toSet(),
          ),
        );
        return true;
      case DeferConflict<T>():
        return false;
    }
  }

  void _maybeTriggerAutoSync() {
    if (autoSyncOnMutation && !_isDisposed) {
      unawaited(sync());
    }
  }

  Future<void> _refreshStatus({
    SyncPhase? phase,
    bool? isOnline,
    DateTime? lastSyncedAt,
    String? lastError,
    bool clearLastError = false,
  }) async {
    final ops = await outbox.getAll();
    var failedCount = 0;
    for (final op in ops) {
      if (!retryPolicy.canRetry(op.retryCount) && op.lastError != null) {
        failedCount++;
      }
    }
    final pendingCount = ops.length - failedCount;
    final online = isOnline ?? await connectivityMonitor.isOnline;

    _currentStatus = _currentStatus.copyWith(
      phase: phase ?? (online ? _currentStatus.phase : SyncPhase.offline),
      isOnline: online,
      pendingCount: pendingCount,
      failedCount: failedCount,
      lastSyncedAt: lastSyncedAt,
      lastError: lastError,
      clearLastError: clearLastError,
    );

    if (!_statusController.isClosed) {
      _statusController.add(_currentStatus);
    }
  }

  static Set<String> _computeChangedFields({
    required Map<String, Object?>? basePayload,
    required Map<String, Object?> newPayload,
  }) {
    if (basePayload == null) {
      return newPayload.keys.toSet();
    }
    final changed = <String>{};
    final allKeys = <String>{...basePayload.keys, ...newPayload.keys};
    for (final key in allKeys) {
      if (basePayload[key] != newPayload[key]) {
        changed.add(key);
      }
    }
    return changed;
  }

  static (Map<String, Object?>, bool) _replaceStringInMap(
    Map<String, Object?> source, {
    required String oldId,
    required String newId,
  }) {
    var anyChanged = false;
    final result = <String, Object?>{};
    for (final entry in source.entries) {
      final (replaced, changed) = _replaceStringInValue(
        entry.value,
        oldId: oldId,
        newId: newId,
      );
      if (changed) anyChanged = true;
      result[entry.key] = replaced;
    }
    return (anyChanged ? result : source, anyChanged);
  }

  static (Object?, bool) _replaceStringInValue(
    Object? value, {
    required String oldId,
    required String newId,
  }) {
    if (value is String) {
      if (value == oldId) return (newId, true);
      return (value, false);
    }
    if (value is Map<String, Object?>) {
      return _replaceStringInMap(value, oldId: oldId, newId: newId);
    }
    if (value is List<Object?>) {
      var listChanged = false;
      final newList = <Object?>[];
      for (final item in value) {
        final (replacedItem, itemChanged) = _replaceStringInValue(
          item,
          oldId: oldId,
          newId: newId,
        );
        if (itemChanged) listChanged = true;
        newList.add(replacedItem);
      }
      return (listChanged ? newList : value, listChanged);
    }
    return (value, false);
  }

  /// Cancels timers and subscriptions and releases resources.
  Future<void> dispose() async {
    _isDisposed = true;
    _periodicTimer?.cancel();
    await _connectivitySubscription?.cancel();
    await _statusController.close();
    await _idMappingController.close();
  }
}

final class _PushOutcome {
  const _PushOutcome({
    required this.pushed,
    required this.conflictsResolved,
    this.failedOperation,
  });

  final int pushed;
  final int conflictsResolved;
  final SyncOperation? failedOperation;
}

final class _BatchPushOutcome {
  const _BatchPushOutcome({
    required this.pushed,
    required this.conflictsResolved,
    required this.failedOperations,
  });

  final int pushed;
  final int conflictsResolved;
  final List<SyncOperation> failedOperations;
}
