import 'dart:convert';

import '../models/sync_delta.dart';
import '../models/sync_operation.dart';
import 'remote_sync_adapter.dart';

/// HTTP method used by [RestRequest].
enum RestMethod {
  /// HTTP `GET` request.
  get,

  /// HTTP `POST` request.
  post,

  /// HTTP `PUT` request.
  put,

  /// HTTP `PATCH` request.
  patch,

  /// HTTP `DELETE` request.
  delete,
}

/// Client-agnostic representation of an outgoing HTTP request produced by
/// [RestSyncAdapter].
class RestRequest {
  /// Creates a [RestRequest].
  const RestRequest({
    required this.method,
    required this.path,
    this.queryParameters = const <String, String>{},
    this.headers = const <String, String>{},
    this.body,
  });

  /// HTTP verb (`GET`, `POST`, `PUT`, `PATCH`, `DELETE`).
  final RestMethod method;

  /// Relative or absolute endpoint path (e.g. `'/api/v1/todos/123'`).
  final String path;

  /// URL query parameters (e.g. `{'updated_after': '2026-01-01T00:00:00Z'}`).
  final Map<String, String> queryParameters;

  /// Request headers (including idempotency keys or custom operation metadata).
  final Map<String, String> headers;

  /// Decoded JSON body (`Map<String, Object?>` or `List<Object?>`), or `null`
  /// for `GET`/`DELETE` requests.
  final Object? body;
}

/// Client-agnostic representation of an HTTP response returned to
/// [RestSyncAdapter].
class RestResponse {
  /// Creates a [RestResponse].
  const RestResponse({
    required this.statusCode,
    this.body,
    this.headers = const <String, String>{},
  });

  /// HTTP status code (e.g. `200`, `201`, `204`, `409`, `422`, `503`).
  final int statusCode;

  /// Response body as a decoded JSON object (`Map` or `List`), a raw JSON
  /// `String`, or `null`.
  final Object? body;

  /// Optional response headers.
  final Map<String, String> headers;

  /// Returns [body] decoded as a `Map<String, Object?>` if possible, or `null`.
  Map<String, Object?>? get bodyAsMap {
    final raw = body;
    if (raw == null) return null;
    if (raw is Map<String, Object?>) return raw;
    if (raw is Map) {
      return raw.map((k, v) => MapEntry(k.toString(), v));
    }
    if (raw is String && raw.trim().isNotEmpty) {
      final decoded = jsonDecode(raw);
      if (decoded is Map) {
        return decoded.map((k, v) => MapEntry(k.toString(), v));
      }
    }
    return null;
  }
}

/// Ready-to-use REST/HTTP implementation of [RemoteSyncAdapter] that works with
/// **`dio`**, **`package:http`**, **`chopper`**, or **`dart:io` `HttpClient`**.
///
/// Automatically handles:
/// * `POST <basePath>` for `create`
/// * `PATCH <basePath>/<id>` (sending only `changedFields`!) or
///   `PUT <basePath>/<id>` for `update`
/// * `DELETE <basePath>/<id>` for `delete`
/// * `POST <basePath>/batch` for `pushBatch`
/// * `GET <basePath>?updated_after=<checkpoint>` for `pullDelta`
/// * Status code classification:
///   * `200–299`: [PushSuccess] (parses canonical server record if present)
///   * `409` / `412`: [PushConflict] (extracts server record for
///     `ConflictResolver`)
///   * `408` / `429` / `500–599` / network errors: [PushRetryableFailure]
///   * `400–499` (other): [PushPermanentFailure]
class RestSyncAdapter<T> extends RemoteSyncAdapter<T> {
  /// Creates a [RestSyncAdapter].
  const RestSyncAdapter({
    required this.basePath,
    required this.fromJson,
    required this.sendRequest,
    this.usePatchForUpdates = true,
    this.sendOnlyChangedFieldsOnPatch = true,
    this.checkpointQueryParam = 'updated_after',
    this.upsertedKey = 'upserted',
    this.deletedIdsKey = 'deletedIds',
    this.nextCheckpointKey = 'nextCheckpoint',
    this.hasMoreKey = 'hasMore',
    this.conflictRecordKey,
    this.defaultHeaders = const <String, String>{},
  });

  /// Base endpoint path for the resource (e.g. `'/api/v1/todos'`).
  final String basePath;

  /// Deserializes an entity [T] from a JSON map.
  final T Function(Map<String, Object?> json) fromJson;

  /// Pluggable HTTP transport callback (easily wired to `Dio.request` or
  /// `http.Client`).
  final Future<RestResponse> Function(RestRequest request) sendRequest;

  /// Whether to use `PATCH` (true) or `PUT` (false) for `update` operations.
  final bool usePatchForUpdates;

  /// When `true` and [usePatchForUpdates] is enabled, only the fields modified
  /// locally (`operation.changedFields`) are sent in the `PATCH` body.
  final bool sendOnlyChangedFieldsOnPatch;

  /// Query parameter name used to pass the delta checkpoint on `GET` requests.
  final String checkpointQueryParam;

  /// JSON key in the delta response containing the list of upserted items.
  final String upsertedKey;

  /// JSON key in the delta response containing the list of deleted item IDs.
  final String deletedIdsKey;

  /// JSON key in the delta response containing the next checkpoint cursor/time.
  final String nextCheckpointKey;

  /// JSON key in the delta response indicating whether more pages exist.
  final String hasMoreKey;

  /// Optional nested JSON key under which a `409 Conflict` response wraps the
  /// authoritative server record (e.g. `'serverRecord'` or `'data'`).
  ///
  /// If `null`, the top-level `409` response body map is parsed directly.
  final String? conflictRecordKey;

  /// Default headers attached to every outgoing [RestRequest].
  final Map<String, String> defaultHeaders;

  String get _normalizedBasePath => basePath.endsWith('/')
      ? basePath.substring(0, basePath.length - 1)
      : basePath;

  @override
  Future<PushOperationResult<T>> pushOperation(SyncOperation operation) async {
    final headers = <String, String>{
      'Content-Type': 'application/json',
      'X-Idempotency-Key': operation.id,
      ...defaultHeaders,
      if (operation.metadata['conflictResolved'] == true)
        'X-Conflict-Resolved': 'true',
    };

    late final RestRequest request;
    switch (operation.type) {
      case SyncOperationType.create:
        request = RestRequest(
          method: RestMethod.post,
          path: _normalizedBasePath,
          headers: headers,
          body: operation.payload,
        );

      case SyncOperationType.update:
        final method = usePatchForUpdates ? RestMethod.patch : RestMethod.put;
        final fullPayload = operation.payload ?? const <String, Object?>{};
        final Map<String, Object?> body;
        if (usePatchForUpdates &&
            sendOnlyChangedFieldsOnPatch &&
            operation.changedFields.isNotEmpty) {
          body = <String, Object?>{
            for (final key in operation.changedFields)
              if (fullPayload.containsKey(key)) key: fullPayload[key],
          };
        } else {
          body = fullPayload;
        }
        request = RestRequest(
          method: method,
          path: '$_normalizedBasePath/${operation.entityId}',
          headers: headers,
          body: body,
        );

      case SyncOperationType.delete:
        request = RestRequest(
          method: RestMethod.delete,
          path: '$_normalizedBasePath/${operation.entityId}',
          headers: headers,
        );
    }

    try {
      final response = await sendRequest(request);
      return _classifyPushResponse(response, operation);
    } catch (error) {
      return PushOperationResult<T>.retryableFailure(
        message: 'Network error while pushing ${operation.id}: $error',
        error: error,
      );
    }
  }

  @override
  Future<List<PushOperationResult<T>>> pushBatch(
    List<SyncOperation> operations,
  ) async {
    if (operations.isEmpty) return const [];

    final request = RestRequest(
      method: RestMethod.post,
      path: '$_normalizedBasePath/batch',
      headers: <String, String>{
        'Content-Type': 'application/json',
        ...defaultHeaders,
      },
      body: <String, Object?>{
        'operations': operations.map((op) => op.toJson()).toList(),
      },
    );

    try {
      final response = await sendRequest(request);
      if (response.statusCode >= 200 && response.statusCode < 300) {
        final bodyMap = response.bodyAsMap;
        final resultsList = bodyMap?['results'] as List<Object?>?;
        if (resultsList != null && resultsList.length == operations.length) {
          return List<PushOperationResult<T>>.generate(operations.length, (i) {
            final item = resultsList[i];
            if (item is Map) {
              final itemMap = item.map((k, v) => MapEntry(k.toString(), v));
              final status = (itemMap['statusCode'] as int?) ?? 200;
              return _classifyPushResponse(
                RestResponse(statusCode: status, body: itemMap['body']),
                operations[i],
              );
            }
            return PushOperationResult<T>.success();
          });
        }
        return operations
            .map((_) => PushOperationResult<T>.success())
            .toList(growable: false);
      }

      if (_isRetryableStatusCode(response.statusCode)) {
        return operations
            .map(
              (_) => PushOperationResult<T>.retryableFailure(
                message: 'Batch push failed with HTTP ${response.statusCode}',
              ),
            )
            .toList(growable: false);
      }

      return operations
          .map(
            (_) => PushOperationResult<T>.permanentFailure(
              message: 'Batch push rejected with HTTP ${response.statusCode}',
            ),
          )
          .toList(growable: false);
    } catch (error) {
      return operations
          .map(
            (_) => PushOperationResult<T>.retryableFailure(
              message: 'Batch network error: $error',
              error: error,
            ),
          )
          .toList(growable: false);
    }
  }

  @override
  Future<RemoteDeltaPage<T>> pullDelta({String? checkpoint}) async {
    final query = <String, String>{
      if (checkpoint != null) checkpointQueryParam: checkpoint,
    };

    final response = await sendRequest(
      RestRequest(
        method: RestMethod.get,
        path: _normalizedBasePath,
        queryParameters: query,
        headers: <String, String>{
          'Accept': 'application/json',
          ...defaultHeaders,
        },
      ),
    );

    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw StateError(
        'Delta pull failed for $basePath with HTTP ${response.statusCode}',
      );
    }

    final rawBody = response.body;
    // Support either a structured delta object `{upserted, deletedIds, ...}`
    // or a plain JSON array `[...]` of items.
    if (rawBody is List) {
      final items = rawBody
          .whereType<Map<Object?, Object?>>()
          .map(
            (m) => fromJson(m.map((k, v) => MapEntry(k.toString(), v))),
          )
          .toList();
      return RemoteDeltaPage<T>(upserted: items);
    }

    final map = response.bodyAsMap ?? const <String, Object?>{};
    final rawUpserted = (map[upsertedKey] as List<Object?>?) ?? const [];
    final rawDeleted = (map[deletedIdsKey] as List<Object?>?) ?? const [];

    final upserted = rawUpserted
        .whereType<Map<Object?, Object?>>()
        .map(
          (m) => fromJson(m.map((k, v) => MapEntry(k.toString(), v))),
        )
        .toList();

    final deletedIds = rawDeleted.map((e) => e.toString()).toList();

    return RemoteDeltaPage<T>(
      upserted: upserted,
      deletedIds: deletedIds,
      nextCheckpoint: map[nextCheckpointKey] as String?,
      hasMore: (map[hasMoreKey] as bool?) ?? false,
    );
  }

  PushOperationResult<T> _classifyPushResponse(
    RestResponse response,
    SyncOperation operation,
  ) {
    final code = response.statusCode;

    if (code >= 200 && code < 300) {
      if (operation.type == SyncOperationType.delete) {
        return PushOperationResult<T>.success();
      }
      final bodyMap = response.bodyAsMap;
      if (bodyMap != null && bodyMap.isNotEmpty) {
        return PushOperationResult<T>.success(
          serverRecord: fromJson(bodyMap),
        );
      }
      return PushOperationResult<T>.success();
    }

    if (code == 409 || code == 412) {
      final bodyMap = response.bodyAsMap;
      if (bodyMap != null) {
        final recordMap = conflictRecordKey != null
            ? (bodyMap[conflictRecordKey] as Map<Object?, Object?>?)?.map(
                (k, v) => MapEntry(k.toString(), v),
              )
            : bodyMap;
        if (recordMap != null) {
          return PushOperationResult<T>.conflict(
            remoteRecord: fromJson(recordMap),
          );
        }
      }
    }

    if (_isRetryableStatusCode(code)) {
      return PushOperationResult<T>.retryableFailure(
        message: 'Server returned retryable HTTP $code',
      );
    }

    return PushOperationResult<T>.permanentFailure(
      message: 'Server rejected operation with HTTP $code',
    );
  }

  static bool _isRetryableStatusCode(int statusCode) {
    return statusCode == 408 ||
        statusCode == 425 ||
        statusCode == 429 ||
        (statusCode >= 500 && statusCode < 600);
  }
}
