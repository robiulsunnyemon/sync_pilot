import 'dart:collection';

/// The type of mutation performed on an entity while offline or online.
enum SyncOperationType {
  /// A newly created entity that has not yet been pushed to the remote server.
  create,

  /// A modification to an existing entity.
  update,

  /// A deletion of an entity.
  delete,
}

/// Represents a single queued mutation waiting in the outbox to be pushed
/// to the remote server.
///
/// Operations are immutable and JSON-serializable so they can be persisted in
/// any local storage engine (SQLite, Drift, Hive, Isar, SharedPreferences, or
/// file storage).
class SyncOperation {
  /// Creates a new [SyncOperation].
  SyncOperation({
    required this.id,
    required this.entityName,
    required this.entityId,
    required this.type,
    required this.createdAt,
    required this.updatedAt,
    Map<String, Object?>? payload,
    Map<String, Object?>? basePayload,
    Set<String>? changedFields,
    this.retryCount = 0,
    this.lastError,
    Map<String, Object?>? metadata,
  })  : payload = payload == null
            ? null
            : UnmodifiableMapView<String, Object?>(
                Map<String, Object?>.of(payload),
              ),
        basePayload = basePayload == null
            ? null
            : UnmodifiableMapView<String, Object?>(
                Map<String, Object?>.of(basePayload),
              ),
        changedFields = UnmodifiableSetView<String>(
          changedFields == null ? <String>{} : Set<String>.of(changedFields),
        ),
        metadata = UnmodifiableMapView<String, Object?>(
          metadata == null
              ? <String, Object?>{}
              : Map<String, Object?>.of(metadata),
        );

  /// Creates a [SyncOperation] from a serialized JSON map.
  factory SyncOperation.fromJson(Map<String, Object?> json) {
    final rawChanged = json['changedFields'] as List<Object?>?;
    final rawPayload = json['payload'] as Map<Object?, Object?>?;
    final rawBasePayload = json['basePayload'] as Map<Object?, Object?>?;
    final rawMetadata = json['metadata'] as Map<Object?, Object?>?;

    return SyncOperation(
      id: json['id']! as String,
      entityName: json['entityName']! as String,
      entityId: json['entityId']! as String,
      type: SyncOperationType.values.byName(json['type']! as String),
      payload: rawPayload?.map(
        (key, value) => MapEntry(key.toString(), value),
      ),
      basePayload: rawBasePayload?.map(
        (key, value) => MapEntry(key.toString(), value),
      ),
      changedFields: rawChanged?.map((e) => e.toString()).toSet(),
      createdAt: DateTime.parse(json['createdAt']! as String).toUtc(),
      updatedAt: DateTime.parse(json['updatedAt']! as String).toUtc(),
      retryCount: (json['retryCount'] as int?) ?? 0,
      lastError: json['lastError'] as String?,
      metadata: rawMetadata?.map(
        (key, value) => MapEntry(key.toString(), value),
      ),
    );
  }

  /// Unique identifier for this outbox operation.
  final String id;

  /// Logical collection or table name (e.g. `'todos'`, `'invoices'`).
  final String entityName;

  /// Unique primary key of the entity being mutated.
  final String entityId;

  /// Mutation type ([SyncOperationType.create], [SyncOperationType.update],
  /// or [SyncOperationType.delete]).
  final SyncOperationType type;

  /// Serialized entity snapshot for [SyncOperationType.create] and
  /// [SyncOperationType.update]. Null for [SyncOperationType.delete].
  final Map<String, Object?>? payload;

  /// Optional snapshot of the entity before local offline modifications began.
  ///
  /// Used by 3-way field-level conflict resolvers to distinguish fields changed
  /// locally from fields changed remotely.
  final Map<String, Object?>? basePayload;

  /// Set of top-level JSON field names modified in this operation.
  ///
  /// Automatically accumulated when multiple offline updates are compacted.
  final Set<String> changedFields;

  /// UTC timestamp when the operation was originally queued.
  final DateTime createdAt;

  /// UTC timestamp when the operation was last updated or compacted.
  final DateTime updatedAt;

  /// Number of failed push attempts so far.
  final int retryCount;

  /// Human-readable error message from the most recent failed push attempt.
  final String? lastError;

  /// Arbitrary user or adapter metadata (e.g. idempotency key, custom headers).
  final Map<String, Object?> metadata;

  /// Returns a copy of this operation with selected fields replaced.
  SyncOperation copyWith({
    String? id,
    String? entityName,
    String? entityId,
    SyncOperationType? type,
    Map<String, Object?>? payload,
    bool clearPayload = false,
    Map<String, Object?>? basePayload,
    Set<String>? changedFields,
    DateTime? createdAt,
    DateTime? updatedAt,
    int? retryCount,
    String? lastError,
    bool clearLastError = false,
    Map<String, Object?>? metadata,
  }) {
    return SyncOperation(
      id: id ?? this.id,
      entityName: entityName ?? this.entityName,
      entityId: entityId ?? this.entityId,
      type: type ?? this.type,
      payload: clearPayload ? null : (payload ?? this.payload),
      basePayload: basePayload ?? this.basePayload,
      changedFields: changedFields ?? this.changedFields,
      createdAt: createdAt ?? this.createdAt,
      updatedAt: updatedAt ?? this.updatedAt,
      retryCount: retryCount ?? this.retryCount,
      lastError: clearLastError ? null : (lastError ?? this.lastError),
      metadata: metadata ?? this.metadata,
    );
  }

  /// Converts this operation to a JSON-compatible map for persistence.
  Map<String, Object?> toJson() {
    return <String, Object?>{
      'id': id,
      'entityName': entityName,
      'entityId': entityId,
      'type': type.name,
      'payload': payload,
      'basePayload': basePayload,
      'changedFields': changedFields.toList(growable: false),
      'createdAt': createdAt.toUtc().toIso8601String(),
      'updatedAt': updatedAt.toUtc().toIso8601String(),
      'retryCount': retryCount,
      'lastError': lastError,
      'metadata': metadata,
    };
  }

  @override
  String toString() {
    return 'SyncOperation('
        'id: $id, '
        'entity: $entityName/$entityId, '
        'type: ${type.name}, '
        'changedFields: $changedFields, '
        'retryCount: $retryCount'
        ')';
  }
}
