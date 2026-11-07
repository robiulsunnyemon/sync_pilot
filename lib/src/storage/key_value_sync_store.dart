import 'dart:async';
import 'dart:convert';

import '../security/sync_payload_codec.dart';
import 'local_sync_store.dart';

/// A reactive [LocalSyncStore] adapter backed by any key-value or NoSQL box
/// storage such as **Hive (`Box<Map>` / `Box<String>`)**, **Hive CE**,
/// **SharedPreferences**, **MMKV**, or **Isar Key-Value**.
///
/// Automatically broadcasts changes to [watchAll] and [watchById] subscribers
/// whenever items are inserted, updated, or deleted.
class KeyValueSyncStore<T> implements LocalSyncStore<T> {
  /// Creates a [KeyValueSyncStore] backed by JSON map entries (ideal for
  /// Hive `Box<Map>`).
  KeyValueSyncStore({
    required this.getId,
    required this.toJson,
    required this.fromJson,
    required FutureOr<Map<String, Map<String, Object?>>> Function()
        readAllEntries,
    required FutureOr<Map<String, Object?>?> Function(String id) readEntry,
    required FutureOr<void> Function(String id, Map<String, Object?> value)
        putEntry,
    required FutureOr<void> Function(String id) deleteEntry,
    required FutureOr<void> Function() clearEntries,
    required FutureOr<String?> Function(String entityName) readCheckpoint,
    required FutureOr<void> Function(String entityName, String? checkpoint)
        writeCheckpoint,
    FutureOr<void> Function(Map<String, Map<String, Object?>> entries)?
        putEntriesBatch,
  })  : _readAllEntries = readAllEntries,
        _readEntry = readEntry,
        _putEntry = putEntry,
        _putEntriesBatch = putEntriesBatch,
        _deleteEntry = deleteEntry,
        _clearEntries = clearEntries,
        _readCheckpoint = readCheckpoint,
        _writeCheckpoint = writeCheckpoint;

  /// Creates a [KeyValueSyncStore] backed by JSON-encoded `String` values
  /// (ideal for Hive `Box<String>`, `SharedPreferences`, or secure key-value
  /// stores), with optional at-rest encryption or compression via [codec].
  factory KeyValueSyncStore.fromJsonStrings({
    required String Function(T item) getId,
    required Map<String, Object?> Function(T item) toJson,
    required T Function(Map<String, Object?> json) fromJson,
    required FutureOr<Map<String, String>> Function() readAllStrings,
    required FutureOr<String?> Function(String id) readString,
    required FutureOr<void> Function(String id, String jsonString) putString,
    required FutureOr<void> Function(String id) deleteString,
    required FutureOr<void> Function() clearStrings,
    required FutureOr<String?> Function(String entityName) readCheckpoint,
    required FutureOr<void> Function(String entityName, String? checkpoint)
        writeCheckpoint,
    SyncPayloadCodec codec = const SyncPayloadCodec.identity(),
  }) {
    return KeyValueSyncStore<T>(
      getId: getId,
      toJson: toJson,
      fromJson: fromJson,
      readAllEntries: () async {
        final rawMap = await readAllStrings();
        final decoded = <String, Map<String, Object?>>{};
        for (final entry in rawMap.entries) {
          final plainJson = await codec.decode(entry.value);
          final jsonMap = (jsonDecode(plainJson) as Map<Object?, Object?>).map(
            (k, v) => MapEntry(k.toString(), v),
          );
          decoded[entry.key] = jsonMap;
        }
        return decoded;
      },
      readEntry: (id) async {
        final raw = await readString(id);
        if (raw == null) return null;
        final plainJson = await codec.decode(raw);
        return (jsonDecode(plainJson) as Map<Object?, Object?>).map(
          (k, v) => MapEntry(k.toString(), v),
        );
      },
      putEntry: (id, value) async {
        final encoded = await codec.encode(jsonEncode(value));
        await putString(id, encoded);
      },
      deleteEntry: deleteString,
      clearEntries: clearStrings,
      readCheckpoint: readCheckpoint,
      writeCheckpoint: writeCheckpoint,
    );
  }

  /// Extracts the primary key from an entity instance.
  final String Function(T item) getId;

  /// Serializes an entity instance to a JSON map.
  final Map<String, Object?> Function(T item) toJson;

  /// Deserializes an entity instance from a JSON map.
  final T Function(Map<String, Object?> json) fromJson;

  final FutureOr<Map<String, Map<String, Object?>>> Function() _readAllEntries;
  final FutureOr<Map<String, Object?>?> Function(String id) _readEntry;
  final FutureOr<void> Function(String id, Map<String, Object?> value)
      _putEntry;
  final FutureOr<void> Function(Map<String, Map<String, Object?>> entries)?
      _putEntriesBatch;
  final FutureOr<void> Function(String id) _deleteEntry;
  final FutureOr<void> Function() _clearEntries;
  final FutureOr<String?> Function(String entityName) _readCheckpoint;
  final FutureOr<void> Function(String entityName, String? checkpoint)
      _writeCheckpoint;

  final StreamController<List<T>> _allController =
      StreamController<List<T>>.broadcast();

  @override
  Future<T?> getById(String id) async {
    final json = await _readEntry(id);
    if (json == null) return null;
    return fromJson(json);
  }

  @override
  Future<List<T>> getAll() async {
    final entries = await _readAllEntries();
    return List<T>.unmodifiable(entries.values.map(fromJson));
  }

  @override
  Future<void> upsert(T item) async {
    final id = getId(item);
    await _putEntry(id, toJson(item));
    await _emitAll();
  }

  @override
  Future<void> upsertBatch(List<T> items) async {
    if (items.isEmpty) return;
    final batchWriter = _putEntriesBatch;
    if (batchWriter != null) {
      final map = <String, Map<String, Object?>>{
        for (final item in items) getId(item): toJson(item),
      };
      await batchWriter(map);
    } else {
      for (final item in items) {
        await _putEntry(getId(item), toJson(item));
      }
    }
    await _emitAll();
  }

  @override
  Future<bool> deleteById(String id) async {
    final existing = await _readEntry(id);
    if (existing == null) return false;
    await _deleteEntry(id);
    await _emitAll();
    return true;
  }

  @override
  Future<void> clear() async {
    await _clearEntries();
    await _emitAll();
  }

  @override
  Future<String?> getSyncCheckpoint(String entityName) async {
    return _readCheckpoint(entityName);
  }

  @override
  Future<void> saveSyncCheckpoint(
    String entityName,
    String? checkpoint,
  ) async {
    await _writeCheckpoint(entityName, checkpoint);
  }

  @override
  Stream<List<T>> watchAll() async* {
    yield await getAll();
    yield* _allController.stream;
  }

  @override
  Stream<T?> watchById(String id) async* {
    yield await getById(id);
    await for (final _ in _allController.stream) {
      yield await getById(id);
    }
  }

  Future<void> _emitAll() async {
    if (!_allController.isClosed) {
      _allController.add(await getAll());
    }
  }

  @override
  Future<void> dispose() async {
    await _allController.close();
  }
}
