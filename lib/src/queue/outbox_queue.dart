import 'dart:convert';

import '../models/sync_operation.dart';
import '../security/sync_payload_codec.dart';

/// Persistence interface for storing queued [SyncOperation] entries.
///
/// Implement this interface to back the outbox with SQLite, Drift, Hive, Isar,
/// or file storage. [InMemoryOutboxStorage] and [JsonStringOutboxStorage] are
/// provided out of the box.
abstract interface class OutboxStorage {
  /// Returns all queued operations for [entityName] in FIFO order.
  Future<List<SyncOperation>> loadOperations(String entityName);

  /// Replaces the persisted operations list for [entityName].
  Future<void> saveOperations(
    String entityName,
    List<SyncOperation> operations,
  );
}

/// Default in-memory implementation of [OutboxStorage].
class InMemoryOutboxStorage implements OutboxStorage {
  final Map<String, List<SyncOperation>> _store =
      <String, List<SyncOperation>>{};

  @override
  Future<List<SyncOperation>> loadOperations(String entityName) async {
    final list = _store[entityName];
    if (list == null) return <SyncOperation>[];
    return List<SyncOperation>.of(list);
  }

  @override
  Future<void> saveOperations(
    String entityName,
    List<SyncOperation> operations,
  ) async {
    _store[entityName] = List<SyncOperation>.of(operations);
  }
}

/// An [OutboxStorage] adapter that serializes the outbox queue to a JSON string
/// via simple `read` and `write` callbacks, with optional encryption or
/// compression via [codec].
///
/// Makes it trivial to persist the outbox in `SharedPreferences`, `Hive`,
/// `FlutterSecureStorage`, or a local file in two lines of code.
class JsonStringOutboxStorage implements OutboxStorage {
  /// Creates a [JsonStringOutboxStorage] with [read] and [write] callbacks and
  /// an optional [codec] for payload encryption/compression.
  const JsonStringOutboxStorage({
    required this.read,
    required this.write,
    this.keyPrefix = 'sync_pilot_outbox_',
    this.codec = const SyncPayloadCodec.identity(),
  });

  /// Reads the serialized (or encrypted) string for `key`, or `null` if not
  /// present.
  final Future<String?> Function(String key) read;

  /// Persists the serialized (or encrypted) string `value` under `key`.
  final Future<void> Function(String key, String value) write;

  /// Prefix prepended to `entityName` when forming storage keys.
  final String keyPrefix;

  /// Optional codec for encrypting or compressing the serialized outbox JSON
  /// before it is written to storage.
  final SyncPayloadCodec codec;

  @override
  Future<List<SyncOperation>> loadOperations(String entityName) async {
    final raw = await read('$keyPrefix$entityName');
    if (raw == null || raw.isEmpty) return <SyncOperation>[];
    final plainJson = await codec.decode(raw);
    final decoded = jsonDecode(plainJson) as List<Object?>;
    return decoded
        .cast<Map<Object?, Object?>>()
        .map(
          (item) => SyncOperation.fromJson(
            item.map((k, v) => MapEntry(k.toString(), v)),
          ),
        )
        .toList();
  }

  @override
  Future<void> saveOperations(
    String entityName,
    List<SyncOperation> operations,
  ) async {
    final plainJson = jsonEncode(
      operations.map((op) => op.toJson()).toList(growable: false),
    );
    final encoded = await codec.encode(plainJson);
    await write('$keyPrefix$entityName', encoded);
  }
}

/// Result of enqueueing a mutation into the [OutboxQueue].
class EnqueueResult {
  /// Creates an [EnqueueResult].
  const EnqueueResult({
    required this.compacted,
    required this.eliminated,
    this.operation,
  });

  /// The resulting queued operation, or `null` if the operation was completely
  /// eliminated (for example, an offline `create` followed by a `delete`).
  final SyncOperation? operation;

  /// True if the incoming mutation was merged/compacted with an existing
  /// pending operation for the same entity ID.
  final bool compacted;

  /// True if an existing pending `create` and the new `delete` canceled each
  /// other out, resulting in zero network requests needed.
  final bool eliminated;
}

/// Manages a durable FIFO queue of offline mutations with automatic
/// **Operation Compaction (Squashing)**.
///
/// ### Compaction Truth Table (for the same `entityId`):
/// * `create` + `update` -> single `create` with merged payload.
/// * `create` + `delete` -> both operations are removed (`0` network calls).
/// * `update` + `update` -> single `update` with merged payload and union of
///   `changedFields`.
/// * `update` + `delete` -> single `delete` operation.
/// * `delete` + `create` -> single `update` operation with full payload.
class OutboxQueue {
  /// Creates an [OutboxQueue] for [entityName].
  OutboxQueue({
    required this.entityName,
    OutboxStorage? storage,
    this.enableCompaction = true,
  }) : _storage = storage ?? InMemoryOutboxStorage();

  /// The logical entity name managed by this queue.
  final String entityName;

  /// Whether automatic operation compaction is enabled.
  final bool enableCompaction;

  final OutboxStorage _storage;
  int _idCounter = 0;
  int _totalCompactedCount = 0;

  /// Cumulative number of redundant operations eliminated by compaction since
  /// the last reset.
  int get totalCompactedCount => _totalCompactedCount;

  /// Resets the [totalCompactedCount] counter to zero and returns its previous
  /// value.
  int takeCompactedCount() {
    final count = _totalCompactedCount;
    _totalCompactedCount = 0;
    return count;
  }

  /// Enqueues a new mutation and automatically compacts it with any existing
  /// pending operation for the same [entityId] if [enableCompaction] is true.
  Future<EnqueueResult> enqueue({
    required String entityId,
    required SyncOperationType type,
    Map<String, Object?>? payload,
    Map<String, Object?>? basePayload,
    Set<String>? changedFields,
    Map<String, Object?>? metadata,
    DateTime? timestamp,
  }) async {
    final now = (timestamp ?? DateTime.now()).toUtc();
    final operations = await _storage.loadOperations(entityName);

    if (enableCompaction) {
      final existingIndex = operations.indexWhere(
        (op) => op.entityId == entityId,
      );

      if (existingIndex != -1) {
        final existing = operations[existingIndex];
        final result = _compact(
          existing: existing,
          incomingType: type,
          incomingPayload: payload,
          incomingBasePayload: basePayload,
          incomingChangedFields: changedFields,
          incomingMetadata: metadata,
          now: now,
        );

        _totalCompactedCount++;
        if (result == null) {
          // create + delete canceled out completely!
          operations.removeAt(existingIndex);
          await _storage.saveOperations(entityName, operations);
          return const EnqueueResult(
            compacted: true,
            eliminated: true,
          );
        } else {
          operations[existingIndex] = result;
          await _storage.saveOperations(entityName, operations);
          return EnqueueResult(
            operation: result,
            compacted: true,
            eliminated: false,
          );
        }
      }
    }

    final newOp = SyncOperation(
      id: _generateOperationId(now),
      entityName: entityName,
      entityId: entityId,
      type: type,
      payload: payload,
      basePayload: basePayload,
      changedFields: changedFields,
      createdAt: now,
      updatedAt: now,
      metadata: metadata,
    );

    operations.add(newOp);
    await _storage.saveOperations(entityName, operations);
    return EnqueueResult(
      operation: newOp,
      compacted: false,
      eliminated: false,
    );
  }

  /// Returns all operations currently in the outbox for [entityName].
  Future<List<SyncOperation>> getAll() {
    return _storage.loadOperations(entityName);
  }

  /// Returns the pending operation for [entityId], or `null` if none exists.
  Future<SyncOperation?> getForEntity(String entityId) async {
    final ops = await _storage.loadOperations(entityName);
    for (final op in ops) {
      if (op.entityId == entityId) return op;
    }
    return null;
  }

  /// Removes the operation with [operationId] from the queue.
  Future<void> removeById(String operationId) async {
    final ops = await _storage.loadOperations(entityName);
    ops.removeWhere((op) => op.id == operationId);
    await _storage.saveOperations(entityName, ops);
  }

  /// Removes any pending operation targeting [entityId].
  Future<void> removeForEntity(String entityId) async {
    final ops = await _storage.loadOperations(entityName);
    ops.removeWhere((op) => op.entityId == entityId);
    await _storage.saveOperations(entityName, ops);
  }

  /// Updates an existing operation in place (e.g. to increment `retryCount`
  /// or record `lastError`).
  Future<void> updateOperation(SyncOperation updated) async {
    final ops = await _storage.loadOperations(entityName);
    final index = ops.indexWhere((op) => op.id == updated.id);
    if (index != -1) {
      ops[index] = updated;
      await _storage.saveOperations(entityName, ops);
    }
  }

  /// Resets the retry counter for all failed operations so they can be retried
  /// on the next sync cycle.
  Future<int> resetFailedOperations({required int maxRetries}) async {
    final ops = await _storage.loadOperations(entityName);
    var resetCount = 0;
    for (var i = 0; i < ops.length; i++) {
      if (ops[i].retryCount >= maxRetries) {
        ops[i] = ops[i].copyWith(retryCount: 0, clearLastError: true);
        resetCount++;
      }
    }
    if (resetCount > 0) {
      await _storage.saveOperations(entityName, ops);
    }
    return resetCount;
  }

  /// Clears all operations for [entityName].
  Future<void> clear() async {
    await _storage.saveOperations(entityName, const <SyncOperation>[]);
  }

  /// Replaces all occurrences of an optimistic temporary ID [oldId] with the
  /// server-assigned permanent ID [newId] across all queued operations.
  ///
  /// Updates:
  /// * `SyncOperation.entityId` when it matches [oldId].
  /// * Any top-level or nested string values in `payload` and `basePayload`
  ///   matching [oldId] (including foreign-key fields like `projectId`).
  ///
  /// Returns the number of queued operations that were modified.
  Future<int> replaceIdReferences({
    required String oldId,
    required String newId,
  }) async {
    if (oldId == newId) return 0;
    final ops = await _storage.loadOperations(entityName);
    var modifiedCount = 0;

    for (var i = 0; i < ops.length; i++) {
      final op = ops[i];
      var changed = false;

      final updatedEntityId = op.entityId == oldId ? newId : op.entityId;
      if (updatedEntityId != op.entityId) {
        changed = true;
      }

      var updatedPayload = op.payload;
      if (op.payload != null) {
        final (rewritten, payloadChanged) = _replaceStringInMap(
          op.payload!,
          oldId: oldId,
          newId: newId,
        );
        if (payloadChanged) {
          updatedPayload = rewritten;
          changed = true;
        }
      }

      var updatedBasePayload = op.basePayload;
      if (op.basePayload != null) {
        final (rewrittenBase, baseChanged) = _replaceStringInMap(
          op.basePayload!,
          oldId: oldId,
          newId: newId,
        );
        if (baseChanged) {
          updatedBasePayload = rewrittenBase;
          changed = true;
        }
      }

      if (changed) {
        ops[i] = op.copyWith(
          entityId: updatedEntityId,
          payload: updatedPayload,
          basePayload: updatedBasePayload,
          updatedAt: DateTime.now().toUtc(),
        );
        modifiedCount++;
      }
    }

    if (modifiedCount > 0) {
      await _storage.saveOperations(entityName, ops);
    }
    return modifiedCount;
  }

  static (Map<String, Object?>, bool) _replaceStringInMap(
    Map<String, Object?> source, {
    required String oldId,
    required String newId,
  }) {
    var anyChanged = false;
    final result = <String, Object?>{};

    for (final entry in source.entries) {
      final (newValue, changed) = _replaceStringInValue(
        entry.value,
        oldId: oldId,
        newId: newId,
      );
      if (changed) {
        anyChanged = true;
      }
      result[entry.key] = newValue;
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

  SyncOperation? _compact({
    required SyncOperation existing,
    required SyncOperationType incomingType,
    required Map<String, Object?>? incomingPayload,
    required Map<String, Object?>? incomingBasePayload,
    required Set<String>? incomingChangedFields,
    required Map<String, Object?>? incomingMetadata,
    required DateTime now,
  }) {
    final mergedMetadata = <String, Object?>{
      ...existing.metadata,
      if (incomingMetadata != null) ...incomingMetadata,
    };

    switch ((existing.type, incomingType)) {
      case (SyncOperationType.create, SyncOperationType.update):
      case (SyncOperationType.create, SyncOperationType.create):
        final mergedPayload = <String, Object?>{
          if (existing.payload != null) ...existing.payload!,
          if (incomingPayload != null) ...incomingPayload,
        };
        final mergedFields = <String>{
          ...existing.changedFields,
          if (incomingChangedFields != null) ...incomingChangedFields,
        };
        return existing.copyWith(
          type: SyncOperationType.create,
          payload: mergedPayload,
          changedFields: mergedFields,
          updatedAt: now,
          retryCount: 0,
          clearLastError: true,
          metadata: mergedMetadata,
        );

      case (SyncOperationType.create, SyncOperationType.delete):
        // Created offline and deleted before ever syncing -> cancel both!
        return null;

      case (SyncOperationType.update, SyncOperationType.update):
      case (SyncOperationType.update, SyncOperationType.create):
        final mergedPayload = <String, Object?>{
          if (existing.payload != null) ...existing.payload!,
          if (incomingPayload != null) ...incomingPayload,
        };
        final mergedFields = <String>{
          ...existing.changedFields,
          if (incomingChangedFields != null) ...incomingChangedFields,
        };
        return existing.copyWith(
          type: SyncOperationType.update,
          payload: mergedPayload,
          basePayload: existing.basePayload ?? incomingBasePayload,
          changedFields: mergedFields,
          updatedAt: now,
          retryCount: 0,
          clearLastError: true,
          metadata: mergedMetadata,
        );

      case (SyncOperationType.update, SyncOperationType.delete):
      case (SyncOperationType.delete, SyncOperationType.delete):
        return existing.copyWith(
          type: SyncOperationType.delete,
          clearPayload: true,
          changedFields: const <String>{},
          updatedAt: now,
          retryCount: 0,
          clearLastError: true,
          metadata: mergedMetadata,
        );

      case (SyncOperationType.delete, SyncOperationType.create):
      case (SyncOperationType.delete, SyncOperationType.update):
        // Record existed on server, was deleted locally, and then re-created
        // with the same ID -> treat as an update of the server record.
        return existing.copyWith(
          type: SyncOperationType.update,
          payload: incomingPayload,
          changedFields: incomingChangedFields ??
              incomingPayload?.keys.toSet() ??
              const <String>{},
          updatedAt: now,
          retryCount: 0,
          clearLastError: true,
          metadata: mergedMetadata,
        );
    }
  }

  String _generateOperationId(DateTime now) {
    _idCounter++;
    return '${entityName}_op_${now.microsecondsSinceEpoch}_$_idCounter';
  }
}
