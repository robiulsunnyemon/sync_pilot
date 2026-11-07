import 'dart:async';

/// Database-agnostic local storage interface for entities of type [T].
///
/// Implement this interface to connect `SyncPilotEngine` to any local database
/// such as **SQLite (`sqflite`)**, **Drift**, **Hive**, **Isar**, **ObjectBox**,
/// or file storage.
///
/// For rapid prototyping, unit testing, or lightweight caching, use the
/// built-in [InMemorySyncStore].
abstract interface class LocalSyncStore<T> {
  /// Retrieves a single entity by its primary key [id], or `null` if not found.
  Future<T?> getById(String id);

  /// Retrieves all locally stored entities in this collection.
  Future<List<T>> getAll();

  /// Inserts or updates [item] in local storage.
  Future<void> upsert(T item);

  /// Inserts or updates multiple [items] in a single batch or transaction.
  Future<void> upsertBatch(List<T> items);

  /// Deletes the entity with [id] from local storage.
  ///
  /// Returns `true` if a record existed and was removed.
  Future<bool> deleteById(String id);

  ///Deletes all entities in this local store.
  Future<void> clear();

  /// Reads the last saved delta synchronization checkpoint for [entityName].
  Future<String?> getSyncCheckpoint(String entityName);

  /// Persists the latest delta synchronization [checkpoint] for [entityName].
  Future<void> saveSyncCheckpoint(String entityName, String? checkpoint);

  /// Reactive stream emitting the complete list of entities whenever local data
  /// changes.
  ///
  /// Immediately emits the current list upon subscription.
  Stream<List<T>> watchAll();

  /// Reactive stream emitting the entity with [id] (or `null` if deleted)
  /// whenever it changes.
  ///
  /// Immediately emits the current value upon subscription.
  Stream<T?> watchById(String id);

  /// Releases any open streams or database handles.
  Future<void> dispose();
}

/// Ready-to-use, reactive in-memory implementation of [LocalSyncStore].
///
/// Maintains insertion order and broadcasts real-time updates to [watchAll]
/// and [watchById] subscribers.
class InMemorySyncStore<T> implements LocalSyncStore<T> {
  /// Creates an [InMemorySyncStore] using [getId] to extract each entity's
  /// primary key.
  InMemorySyncStore({
    required this.getId,
    Iterable<T>? initialItems,
  }) {
    if (initialItems != null) {
      for (final item in initialItems) {
        _items[getId(item)] = item;
      }
    }
  }

  /// Extracts the unique string identifier from an entity instance.
  final String Function(T item) getId;

  final Map<String, T> _items = <String, T>{};
  final Map<String, String?> _checkpoints = <String, String?>{};
  final StreamController<List<T>> _allController =
      StreamController<List<T>>.broadcast();

  @override
  Future<T?> getById(String id) async => _items[id];

  @override
  Future<List<T>> getAll() async => List<T>.unmodifiable(_items.values);

  @override
  Future<void> upsert(T item) async {
    _items[getId(item)] = item;
    _notifyListeners();
  }

  @override
  Future<void> upsertBatch(List<T> items) async {
    if (items.isEmpty) return;
    for (final item in items) {
      _items[getId(item)] = item;
    }
    _notifyListeners();
  }

  @override
  Future<bool> deleteById(String id) async {
    final existed = _items.containsKey(id);
    if (existed) {
      _items.remove(id);
      _notifyListeners();
    }
    return existed;
  }

  @override
  Future<void> clear() async {
    if (_items.isEmpty) return;
    _items.clear();
    _notifyListeners();
  }

  @override
  Future<String?> getSyncCheckpoint(String entityName) async {
    return _checkpoints[entityName];
  }

  @override
  Future<void> saveSyncCheckpoint(
    String entityName,
    String? checkpoint,
  ) async {
    _checkpoints[entityName] = checkpoint;
  }

  @override
  Stream<List<T>> watchAll() async* {
    yield List<T>.unmodifiable(_items.values);
    yield* _allController.stream;
  }

  @override
  Stream<T?> watchById(String id) async* {
    yield _items[id];
    await for (final _ in _allController.stream) {
      yield _items[id];
    }
  }

  void _notifyListeners() {
    if (!_allController.isClosed) {
      _allController.add(List<T>.unmodifiable(_items.values));
    }
  }

  @override
  Future<void> dispose() async {
    await _allController.close();
  }
}
