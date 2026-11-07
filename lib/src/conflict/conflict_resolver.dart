import 'dart:async';

import '../models/sync_operation.dart';

/// Contextual information passed to a [ConflictResolver] when a conflict occurs
/// between a local mutation and the remote server's state.
class ConflictContext<T> {
  /// Creates a new [ConflictContext].
  const ConflictContext({
    required this.entityName,
    required this.entityId,
    required this.pendingOperation,
    required this.localItem,
    required this.remoteItem,
    required this.localJson,
    required this.remoteJson,
    this.baseJson,
  });

  /// Logical entity name (e.g. `'todos'`).
  final String entityName;

  /// Primary key of the conflicting record.
  final String entityId;

  /// The outbox operation that triggered or encountered the conflict.
  final SyncOperation pendingOperation;

  /// Current local entity instance (null if the local operation was a delete).
  final T? localItem;

  /// Authoritative entity instance returned by the remote server.
  final T remoteItem;

  /// JSON representation of [localItem], if available.
  final Map<String, Object?>? localJson;

  /// JSON representation of [remoteItem].
  final Map<String, Object?> remoteJson;

  /// Optional JSON snapshot of the entity prior to offline modifications.
  ///
  /// Enables true 3-way merging by comparing both [localJson] and [remoteJson]
  /// against their common ancestor.
  final Map<String, Object?>? baseJson;
}

/// Sealed outcome of resolving a synchronization conflict.
sealed class ConflictResolution<T> {
  /// Base constructor for [ConflictResolution].
  const ConflictResolution();

  /// Discard the local mutation and accept the remote server's version.
  const factory ConflictResolution.useRemote(T remoteRecord) = UseRemote<T>;

  /// Force-push the local version to overwrite the remote server's version.
  const factory ConflictResolution.useLocal(T localRecord) = UseLocal<T>;

  /// Save and push a newly merged entity that combines local and remote
  /// changes.
  const factory ConflictResolution.useMerged(T mergedRecord) = UseMerged<T>;

  /// Leave the conflict unresolved in the outbox for manual user intervention.
  const factory ConflictResolution.defer({String? reason}) = DeferConflict<T>;
}

/// Resolution that accepts the remote server's version and discards the
/// conflicting local mutation.
final class UseRemote<T> extends ConflictResolution<T> {
  /// Creates a [UseRemote] resolution.
  const UseRemote(this.remoteRecord);

  /// The remote entity to persist locally.
  final T remoteRecord;
}

/// Resolution that keeps the local version and re-pushes it to the server.
final class UseLocal<T> extends ConflictResolution<T> {
  /// Creates a [UseLocal] resolution.
  const UseLocal(this.localRecord);

  /// The local entity to keep and push.
  final T localRecord;
}

/// Resolution that produces a merged entity to persist locally and push to the
/// remote server.
final class UseMerged<T> extends ConflictResolution<T> {
  /// Creates a [UseMerged] resolution.
  const UseMerged(this.mergedRecord);

  /// The merged entity combining local and remote modifications.
  final T mergedRecord;
}

/// Resolution that defers conflict handling for later manual intervention.
final class DeferConflict<T> extends ConflictResolution<T> {
  /// Creates a [DeferConflict] resolution.
  const DeferConflict({this.reason});

  /// Optional explanation for why the conflict was deferred.
  final String? reason;
}

/// Strategy interface for resolving synchronization conflicts between local
/// offline mutations and remote server state.
abstract interface class ConflictResolver<T> {
  /// Always accept the remote server's version when a conflict occurs.
  factory ConflictResolver.serverWins() = _ServerWinsResolver<T>;

  /// Always prioritize the local client's version when a conflict occurs.
  factory ConflictResolver.clientWins() = _ClientWinsResolver<T>;

  /// Compare timestamps between the local and remote items and pick whichever
  /// was modified more recently.
  ///
  /// If both timestamps are equal, [preferRemoteOnTie] controls the winner.
  factory ConflictResolver.lastWriteWins({
    required DateTime Function(T item) timestampExtractor,
    bool preferRemoteOnTie,
  }) = _LastWriteWinsResolver<T>;

  /// Intelligently merge non-conflicting field changes from both the local
  /// mutation and the remote record.
  ///
  /// Starts with `remoteJson` as the base and overlays only the fields that
  /// were modified locally (tracked via `SyncOperation.changedFields` or
  /// computed by diffing `localJson` against `baseJson`).
  ///
  /// If the same field was modified both locally and remotely (compared to
  /// `baseJson`), [preferLocalOnFieldCollision] determines which value wins.
  factory ConflictResolver.fieldLevelMerge({
    required T Function(Map<String, Object?> json) fromJson,
    bool preferLocalOnFieldCollision,
    Set<String> alwaysKeepRemoteFields,
  }) = _FieldLevelMergeResolver<T>;

  /// Delegate conflict resolution to a custom synchronous or asynchronous
  /// callback function.
  factory ConflictResolver.custom(
    FutureOr<ConflictResolution<T>> Function(ConflictContext<T> context)
        resolver,
  ) = _CustomConflictResolver<T>;

  /// Resolves a conflict described by [context].
  FutureOr<ConflictResolution<T>> resolve(ConflictContext<T> context);
}

final class _ServerWinsResolver<T> implements ConflictResolver<T> {
  const _ServerWinsResolver();

  @override
  ConflictResolution<T> resolve(ConflictContext<T> context) {
    return ConflictResolution<T>.useRemote(context.remoteItem);
  }
}

final class _ClientWinsResolver<T> implements ConflictResolver<T> {
  const _ClientWinsResolver();

  @override
  ConflictResolution<T> resolve(ConflictContext<T> context) {
    final local = context.localItem;
    if (local == null) {
      return ConflictResolution<T>.useRemote(context.remoteItem);
    }
    return ConflictResolution<T>.useLocal(local);
  }
}

final class _LastWriteWinsResolver<T> implements ConflictResolver<T> {
  const _LastWriteWinsResolver({
    required this.timestampExtractor,
    this.preferRemoteOnTie = true,
  });

  final DateTime Function(T item) timestampExtractor;
  final bool preferRemoteOnTie;

  @override
  ConflictResolution<T> resolve(ConflictContext<T> context) {
    final local = context.localItem;
    if (local == null) {
      return ConflictResolution<T>.useRemote(context.remoteItem);
    }

    final localTime = timestampExtractor(local).toUtc();
    final remoteTime = timestampExtractor(context.remoteItem).toUtc();

    if (localTime.isAfter(remoteTime)) {
      return ConflictResolution<T>.useLocal(local);
    }
    if (remoteTime.isAfter(localTime)) {
      return ConflictResolution<T>.useRemote(context.remoteItem);
    }
    return preferRemoteOnTie
        ? ConflictResolution<T>.useRemote(context.remoteItem)
        : ConflictResolution<T>.useLocal(local);
  }
}

final class _FieldLevelMergeResolver<T> implements ConflictResolver<T> {
  const _FieldLevelMergeResolver({
    required this.fromJson,
    this.preferLocalOnFieldCollision = true,
    this.alwaysKeepRemoteFields = const <String>{},
  });

  final T Function(Map<String, Object?> json) fromJson;
  final bool preferLocalOnFieldCollision;
  final Set<String> alwaysKeepRemoteFields;

  @override
  ConflictResolution<T> resolve(ConflictContext<T> context) {
    final localJson = context.localJson;
    if (localJson == null) {
      return ConflictResolution<T>.useRemote(context.remoteItem);
    }

    final remoteJson = context.remoteJson;
    final baseJson = context.baseJson;
    final merged = Map<String, Object?>.of(remoteJson);

    // Determine which fields changed locally.
    final localChangedFields = <String>{
      ...context.pendingOperation.changedFields,
    };
    if (localChangedFields.isEmpty && baseJson != null) {
      for (final entry in localJson.entries) {
        if (!_deepEquals(entry.value, baseJson[entry.key])) {
          localChangedFields.add(entry.key);
        }
      }
    } else if (localChangedFields.isEmpty) {
      // Fallback: consider any field differing from remoteJson as candidate.
      for (final entry in localJson.entries) {
        if (!_deepEquals(entry.value, remoteJson[entry.key])) {
          localChangedFields.add(entry.key);
        }
      }
    }

    for (final field in localChangedFields) {
      if (alwaysKeepRemoteFields.contains(field)) {
        continue;
      }
      if (!localJson.containsKey(field)) {
        continue;
      }

      // Check if the remote also modified this same field relative to baseJson.
      final remoteAlsoChangedField = baseJson != null &&
          !_deepEquals(remoteJson[field], baseJson[field]) &&
          !_deepEquals(localJson[field], remoteJson[field]);

      if (remoteAlsoChangedField && !preferLocalOnFieldCollision) {
        // Keep remoteJson[field].
        continue;
      }

      merged[field] = localJson[field];
    }

    return ConflictResolution<T>.useMerged(fromJson(merged));
  }

  static bool _deepEquals(Object? a, Object? b) {
    if (identical(a, b)) return true;
    if (a is List && b is List) {
      if (a.length != b.length) return false;
      for (var i = 0; i < a.length; i++) {
        if (!_deepEquals(a[i], b[i])) return false;
      }
      return true;
    }
    if (a is Map && b is Map) {
      if (a.length != b.length) return false;
      for (final key in a.keys) {
        if (!b.containsKey(key) || !_deepEquals(a[key], b[key])) {
          return false;
        }
      }
      return true;
    }
    return a == b;
  }
}

final class _CustomConflictResolver<T> implements ConflictResolver<T> {
  const _CustomConflictResolver(this._resolver);

  final FutureOr<ConflictResolution<T>> Function(ConflictContext<T> context)
      _resolver;

  @override
  FutureOr<ConflictResolution<T>> resolve(ConflictContext<T> context) {
    return _resolver(context);
  }
}
