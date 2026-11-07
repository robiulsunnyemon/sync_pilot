import '../models/sync_delta.dart';
import '../models/sync_operation.dart';

/// Backend-agnostic transport adapter for pushing local mutations and pulling
/// remote delta changes.
///
/// Extend or implement this class (or use [CallbackRemoteSyncAdapter]) to
/// connect `SyncPilotEngine` to **REST APIs (`dio` / `http`)**, **GraphQL**,
/// **Supabase**, **Firebase**, **Appwrite**, **PocketBase**, or **gRPC**.
abstract class RemoteSyncAdapter<T> {
  /// Base constructor for [RemoteSyncAdapter].
  const RemoteSyncAdapter();

  /// Pushes a single queued [operation] (`create`, `update`, or `delete`) to
  /// the remote server.
  ///
  /// Return:
  /// * [PushSuccess] when the server accepts the mutation.
  /// * [PushConflict] with the server's current record when a version/state
  ///   conflict occurs (e.g. HTTP `409 Conflict`).
  /// * [PushRetryableFailure] on transient network or `5xx` errors.
  /// * [PushPermanentFailure] on validation (`400`/`422`) or unrecoverable
  ///   errors.
  Future<PushOperationResult<T>> pushOperation(SyncOperation operation);

  /// Pushes a batch of [operations] in a single request when bulk push is
  /// enabled on `SyncPilotEngine`.
  ///
  /// Must return a list of [PushOperationResult] in the exact same order and
  /// length as [operations].
  ///
  /// By default, sequentially invokes [pushOperation] for each item in
  /// [operations]. Override this method to send a single bulk HTTP/GraphQL
  /// request.
  Future<List<PushOperationResult<T>>> pushBatch(
    List<SyncOperation> operations,
  ) async {
    final results = <PushOperationResult<T>>[];
    for (final op in operations) {
      results.add(await pushOperation(op));
    }
    return results;
  }

  /// Pulls a page of delta changes from the remote server since [checkpoint].
  ///
  /// When [checkpoint] is `null`, the adapter should perform an initial sync
  /// (fetching the initial dataset and returning a `nextCheckpoint`).
  Future<RemoteDeltaPage<T>> pullDelta({String? checkpoint});
}

/// Functional implementation of [RemoteSyncAdapter] that accepts callbacks for
/// [onPush], optional [onPushBatch], and [onPullDelta].
///
/// Eliminates boilerplate subclassing when wiring up HTTP clients like `dio`
/// or `http`.
class CallbackRemoteSyncAdapter<T> extends RemoteSyncAdapter<T> {
  /// Creates a [CallbackRemoteSyncAdapter] from push and pull callbacks.
  const CallbackRemoteSyncAdapter({
    required this.onPush,
    required this.onPullDelta,
    this.onPushBatch,
  });

  /// Callback invoked for each outbox operation to push to the backend.
  final Future<PushOperationResult<T>> Function(SyncOperation operation) onPush;

  /// Optional bulk callback invoked when `enableBatchPush` is active on
  /// `SyncPilotEngine`.
  ///
  /// If omitted, [pushBatch] falls back to invoking [onPush] sequentially.
  final Future<List<PushOperationResult<T>>> Function(
    List<SyncOperation> operations,
  )? onPushBatch;

  /// Callback invoked to fetch a page of changes since `checkpoint`.
  final Future<RemoteDeltaPage<T>> Function(String? checkpoint) onPullDelta;

  @override
  Future<PushOperationResult<T>> pushOperation(SyncOperation operation) {
    return onPush(operation);
  }

  @override
  Future<List<PushOperationResult<T>>> pushBatch(
    List<SyncOperation> operations,
  ) {
    final batchCallback = onPushBatch;
    if (batchCallback != null) {
      return batchCallback(operations);
    }
    return super.pushBatch(operations);
  }

  @override
  Future<RemoteDeltaPage<T>> pullDelta({String? checkpoint}) {
    return onPullDelta(checkpoint);
  }
}
