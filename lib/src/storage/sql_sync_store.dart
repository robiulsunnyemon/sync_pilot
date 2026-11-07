import 'dart:async';
import 'dart:convert';

import '../models/sync_operation.dart';
import '../queue/outbox_queue.dart';
import '../security/sync_payload_codec.dart';
import 'local_sync_store.dart';

/// Minimal SQL execution interface compatible with **`sqflite`**, **`sqlite3`**,
/// **`drift`**, and **`sqlite_async`** without requiring any external package
/// dependency.
///
/// Use [CallbackSqlExecutor] to wrap `db.rawQuery` and `db.rawExecute` in two
/// lines of code.
abstract interface class SqlExecutor {
  /// Executes a `SELECT` SQL statement with positional `?` [arguments] and
  /// returns the resulting rows.
  Future<List<Map<String, Object?>>> query(
    String sql, [
    List<Object?> arguments = const <Object?>[],
  ]);

  /// Executes a DDL or DML SQL statement (`CREATE TABLE`, `INSERT`, `UPDATE`,
  /// `DELETE`) with positional `?` [arguments].
  Future<void> execute(
    String sql, [
    List<Object?> arguments = const <Object?>[],
  ]);
}

/// Functional adapter for [SqlExecutor] that wraps `query` and `execute`
/// callbacks (e.g. `sqflite`'s `db.rawQuery` and `db.execute`).
class CallbackSqlExecutor implements SqlExecutor {
  /// Creates a [CallbackSqlExecutor].
  const CallbackSqlExecutor({
    required this.onQuery,
    required this.onExecute,
  });

  /// Callback invoked for `SELECT` queries.
  final Future<List<Map<String, Object?>>> Function(
    String sql,
    List<Object?> arguments,
  ) onQuery;

  /// Callback invoked for `CREATE`, `INSERT`, `UPDATE`, and `DELETE`
  /// statements.
  final Future<void> Function(
    String sql,
    List<Object?> arguments,
  ) onExecute;

  @override
  Future<List<Map<String, Object?>>> query(
    String sql, [
    List<Object?> arguments = const <Object?>[],
  ]) {
    return onQuery(sql, arguments);
  }

  @override
  Future<void> execute(
    String sql, [
    List<Object?> arguments = const <Object?>[],
  ]) {
    return onExecute(sql, arguments);
  }
}

/// Ready-to-use SQL / SQLite / Drift implementation of [LocalSyncStore].
///
/// Stores entities as indexed JSON documents inside [tableName] and manages
/// delta checkpoints in [checkpointTableName].
///
/// Call [ensureTables] once during initialization to create the required
/// SQLite tables automatically.
class SqlSyncStore<T> implements LocalSyncStore<T> {
  /// Creates a [SqlSyncStore] backed by [executor].
  SqlSyncStore({
    required this.executor,
    required this.tableName,
    required this.getId,
    required this.toJson,
    required this.fromJson,
    this.checkpointTableName = 'sync_pilot_checkpoints',
    this.codec = const SyncPayloadCodec.identity(),
  })  : assert(
          _validIdentifier.hasMatch(tableName),
          'Invalid SQL table name: $tableName',
        ),
        assert(
          _validIdentifier.hasMatch(checkpointTableName),
          'Invalid SQL checkpoint table name: $checkpointTableName',
        );

  static final RegExp _validIdentifier = RegExp(r'^[a-zA-Z_][a-zA-Z0-9_]*$');

  /// Underlying SQL database executor.
  final SqlExecutor executor;

  /// SQL table name used to store entities of type [T].
  final String tableName;

  /// SQL table name used to store delta sync checkpoints.
  final String checkpointTableName;

  /// Extracts the primary key from an entity instance.
  final String Function(T item) getId;

  /// Serializes an entity instance to a JSON map.
  final Map<String, Object?> Function(T item) toJson;

  /// Deserializes an entity instance from a JSON map.
  final T Function(Map<String, Object?> json) fromJson;

  /// Optional codec for encrypting or compressing entity JSON payloads before
  /// storing them in SQLite.
  final SyncPayloadCodec codec;

  final StreamController<List<T>> _allController =
      StreamController<List<T>>.broadcast();
  bool _tablesInitialized = false;

  /// Creates [tableName] and [checkpointTableName] if they do not already
  /// exist.
  Future<void> ensureTables() async {
    if (_tablesInitialized) return;
    await executor.execute(
      'CREATE TABLE IF NOT EXISTS $tableName ('
      'id TEXT PRIMARY KEY NOT NULL, '
      'payload TEXT NOT NULL, '
      'updated_at TEXT NOT NULL'
      ')',
    );
    await executor.execute(
      'CREATE TABLE IF NOT EXISTS $checkpointTableName ('
      'entity_name TEXT PRIMARY KEY NOT NULL, '
      'checkpoint TEXT'
      ')',
    );
    _tablesInitialized = true;
  }

  @override
  Future<T?> getById(String id) async {
    await ensureTables();
    final rows = await executor.query(
      'SELECT payload FROM $tableName WHERE id = ? LIMIT 1',
      <Object?>[id],
    );
    if (rows.isEmpty) return null;
    return _decodeRow(rows.first);
  }

  @override
  Future<List<T>> getAll() async {
    await ensureTables();
    final rows = await executor.query(
      'SELECT payload FROM $tableName ORDER BY rowid ASC',
    );
    final items = <T>[];
    for (final row in rows) {
      items.add(await _decodeRow(row));
    }
    return List<T>.unmodifiable(items);
  }

  @override
  Future<void> upsert(T item) async {
    await ensureTables();
    final id = getId(item);
    final payload = await codec.encode(jsonEncode(toJson(item)));
    final now = DateTime.now().toUtc().toIso8601String();
    await executor.execute(
      'INSERT OR REPLACE INTO $tableName (id, payload, updated_at) '
      'VALUES (?, ?, ?)',
      <Object?>[id, payload, now],
    );
    await _emitAll();
  }

  @override
  Future<void> upsertBatch(List<T> items) async {
    if (items.isEmpty) return;
    await ensureTables();
    final now = DateTime.now().toUtc().toIso8601String();
    for (final item in items) {
      final id = getId(item);
      final payload = await codec.encode(jsonEncode(toJson(item)));
      await executor.execute(
        'INSERT OR REPLACE INTO $tableName (id, payload, updated_at) '
        'VALUES (?, ?, ?)',
        <Object?>[id, payload, now],
      );
    }
    await _emitAll();
  }

  @override
  Future<bool> deleteById(String id) async {
    await ensureTables();
    final existing = await getById(id);
    if (existing == null) return false;
    await executor.execute(
      'DELETE FROM $tableName WHERE id = ?',
      <Object?>[id],
    );
    await _emitAll();
    return true;
  }

  @override
  Future<void> clear() async {
    await ensureTables();
    await executor.execute('DELETE FROM $tableName');
    await _emitAll();
  }

  @override
  Future<String?> getSyncCheckpoint(String entityName) async {
    await ensureTables();
    final rows = await executor.query(
      'SELECT checkpoint FROM $checkpointTableName WHERE entity_name = ? LIMIT 1',
      <Object?>[entityName],
    );
    if (rows.isEmpty) return null;
    return rows.first['checkpoint'] as String?;
  }

  @override
  Future<void> saveSyncCheckpoint(
    String entityName,
    String? checkpoint,
  ) async {
    await ensureTables();
    await executor.execute(
      'INSERT OR REPLACE INTO $checkpointTableName (entity_name, checkpoint) '
      'VALUES (?, ?)',
      <Object?>[entityName, checkpoint],
    );
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

  Future<T> _decodeRow(Map<String, Object?> row) async {
    final rawPayload = row['payload']! as String;
    final plainJson = await codec.decode(rawPayload);
    final map = (jsonDecode(plainJson) as Map<Object?, Object?>).map(
      (k, v) => MapEntry(k.toString(), v),
    );
    return fromJson(map);
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

/// Durable SQL / SQLite / Drift implementation of [OutboxStorage].
///
/// Persists queued [SyncOperation] entries in a SQLite table so offline
/// mutations survive app restarts, with optional at-rest encryption or
/// compression via [codec].
class SqlOutboxStorage implements OutboxStorage {
  /// Creates a [SqlOutboxStorage] backed by [executor].
  SqlOutboxStorage({
    required this.executor,
    this.tableName = 'sync_pilot_outbox',
    this.codec = const SyncPayloadCodec.identity(),
  }) : assert(
          SqlSyncStore._validIdentifier.hasMatch(tableName),
          'Invalid SQL outbox table name: $tableName',
        );

  /// Underlying SQL database executor.
  final SqlExecutor executor;

  /// SQL table name used to persist outbox operations.
  final String tableName;

  /// Optional codec for encrypting or compressing queued operations before
  /// writing them to SQLite.
  final SyncPayloadCodec codec;

  bool _tableInitialized = false;

  /// Creates the outbox SQL table if it does not already exist.
  Future<void> ensureTable() async {
    if (_tableInitialized) return;
    await executor.execute(
      'CREATE TABLE IF NOT EXISTS $tableName ('
      'seq INTEGER PRIMARY KEY AUTOINCREMENT, '
      'id TEXT UNIQUE NOT NULL, '
      'entity_name TEXT NOT NULL, '
      'entity_id TEXT NOT NULL, '
      'operation_json TEXT NOT NULL'
      ')',
    );
    _tableInitialized = true;
  }

  @override
  Future<List<SyncOperation>> loadOperations(String entityName) async {
    await ensureTable();
    final rows = await executor.query(
      'SELECT operation_json FROM $tableName '
      'WHERE entity_name = ? ORDER BY seq ASC',
      <Object?>[entityName],
    );
    final operations = <SyncOperation>[];
    for (final row in rows) {
      final raw = row['operation_json']! as String;
      final plainJson = await codec.decode(raw);
      final map = (jsonDecode(plainJson) as Map<Object?, Object?>).map(
        (k, v) => MapEntry(k.toString(), v),
      );
      operations.add(SyncOperation.fromJson(map));
    }
    return operations;
  }

  @override
  Future<void> saveOperations(
    String entityName,
    List<SyncOperation> operations,
  ) async {
    await ensureTable();
    await executor.execute(
      'DELETE FROM $tableName WHERE entity_name = ?',
      <Object?>[entityName],
    );
    for (final op in operations) {
      final encodedJson = await codec.encode(jsonEncode(op.toJson()));
      await executor.execute(
        'INSERT INTO $tableName (id, entity_name, entity_id, operation_json) '
        'VALUES (?, ?, ?, ?)',
        <Object?>[
          op.id,
          entityName,
          op.entityId,
          encodedJson,
        ],
      );
    }
  }
}
