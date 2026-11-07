/// Represents a single page of delta changes pulled from a remote backend.
///
/// Supports both cursor/token-based pagination ([nextCheckpoint]) and
/// timestamp-based incremental synchronization.
class RemoteDeltaPage<T> {
  /// Creates a new [RemoteDeltaPage].
  const RemoteDeltaPage({
    this.upserted = const [],
    this.deletedIds = const [],
    this.nextCheckpoint,
    this.hasMore = false,
  });

  /// Records created or updated on the remote server since the previous
  /// checkpoint.
  final List<T> upserted;

  /// IDs of records deleted on the remote server (tombstones) since the
  /// previous checkpoint.
  final List<String> deletedIds;

  /// Opaque checkpoint string (e.g. an ISO-8601 server timestamp, sequence
  /// number, or cursor token) to persist locally and pass into the next pull.
  final String? nextCheckpoint;

  /// Whether additional pages of delta changes are immediately available.
  final bool hasMore;
}

/// Sealed result hierarchy returned by a `RemoteSyncAdapter` when pushing a
/// single `SyncOperation`.
sealed class PushOperationResult<T> {
  /// Base constructor for [PushOperationResult].
  const PushOperationResult();

  /// The operation succeeded on the remote server.
  ///
  /// Optionally provides the canonical [serverRecord] returned by the backend
  /// (e.g. with server-populated timestamps or normalized fields).
  const factory PushOperationResult.success({T? serverRecord}) = PushSuccess<T>;

  /// The remote server rejected the push due to a version or state conflict
  /// (e.g. HTTP `409 Conflict` or `412 Precondition Failed`).
  ///
  /// Provides the authoritative [remoteRecord] currently stored on the server
  /// so the configured `ConflictResolver` can resolve the discrepancy.
  const factory PushOperationResult.conflict({required T remoteRecord}) =
      PushConflict<T>;

  /// A transient failure occurred (e.g. network timeout, HTTP `429` or `503`).
  ///
  /// The engine will keep the operation in the outbox and retry according to
  /// its `RetryPolicy`.
  const factory PushOperationResult.retryableFailure({
    required String message,
    Object? error,
  }) = PushRetryableFailure<T>;

  /// A permanent failure occurred (e.g. HTTP `400 Bad Request` or `422`
  /// validation error) that should not be retried automatically.
  const factory PushOperationResult.permanentFailure({
    required String message,
    Object? error,
  }) = PushPermanentFailure<T>;
}

/// Indicates that pushing a mutation to the remote server succeeded.
final class PushSuccess<T> extends PushOperationResult<T> {
  /// Creates a [PushSuccess] result.
  const PushSuccess({this.serverRecord});

  /// Optional canonical entity returned by the remote server.
  final T? serverRecord;
}

/// Indicates that the remote server detected a conflict for the pushed entity.
final class PushConflict<T> extends PushOperationResult<T> {
  /// Creates a [PushConflict] result with the current [remoteRecord].
  const PushConflict({required this.remoteRecord});

  /// Current version of the entity on the remote server.
  final T remoteRecord;
}

/// Indicates a transient error that is eligible for automatic retry with
/// exponential backoff.
final class PushRetryableFailure<T> extends PushOperationResult<T> {
  /// Creates a [PushRetryableFailure] result.
  const PushRetryableFailure({required this.message, this.error});

  /// Description of the transient failure.
  final String message;

  /// Optional underlying exception or error object.
  final Object? error;
}

/// Indicates an unrecoverable error for this operation.
final class PushPermanentFailure<T> extends PushOperationResult<T> {
  /// Creates a [PushPermanentFailure] result.
  const PushPermanentFailure({required this.message, this.error});

  /// Description of the permanent failure.
  final String message;

  /// Optional underlying exception or error object.
  final Object? error;
}
