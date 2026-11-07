import 'dart:async';

import 'package:sync_pilot/sync_pilot.dart';

class TestNote {
  const TestNote({
    required this.id,
    required this.title,
    required this.body,
    required this.starred,
    required this.updatedAt,
  });

  factory TestNote.fromJson(Map<String, Object?> json) {
    return TestNote(
      id: json['id']! as String,
      title: json['title']! as String,
      body: json['body']! as String,
      starred: (json['starred'] as bool?) ?? false,
      updatedAt: DateTime.parse(json['updatedAt']! as String).toUtc(),
    );
  }

  final String id;
  final String title;
  final String body;
  final bool starred;
  final DateTime updatedAt;

  TestNote copyWith({
    String? title,
    String? body,
    bool? starred,
    DateTime? updatedAt,
  }) {
    return TestNote(
      id: id,
      title: title ?? this.title,
      body: body ?? this.body,
      starred: starred ?? this.starred,
      updatedAt: updatedAt ?? this.updatedAt,
    );
  }

  Map<String, Object?> toJson() => <String, Object?>{
        'id': id,
        'title': title,
        'body': body,
        'starred': starred,
        'updatedAt': updatedAt.toUtc().toIso8601String(),
      };
}

void _expect(bool condition, String message) {
  if (!condition) {
    throw StateError('Test assertion failed: $message');
  }
}

void _expectEquals(Object? actual, Object? expected, [String? reason]) {
  if (actual != expected) {
    throw StateError(
      'Expected <$expected>, but got <$actual>'
      '${reason != null ? " ($reason)" : ""}',
    );
  }
}

Future<void> main() async {
  var passed = 0;

  Future<void> runTest(String name, FutureOr<void> Function() body) async {
    await body();
    passed++;
    print('  [PASS] $name');
  }

  print('Running sync_pilot test suite...');

  await runTest(
    'OutboxQueue compacts create + multiple updates into a single create',
    () async {
      final queue = OutboxQueue(entityName: 'notes');

      await queue.enqueue(
        entityId: '1',
        type: SyncOperationType.create,
        payload: <String, Object?>{
          'id': '1',
          'title': 'Draft',
          'starred': false,
        },
        changedFields: <String>{'id', 'title', 'starred'},
      );

      final updateResult = await queue.enqueue(
        entityId: '1',
        type: SyncOperationType.update,
        payload: <String, Object?>{
          'id': '1',
          'title': 'Final',
          'starred': true,
        },
        changedFields: <String>{'title', 'starred'},
      );

      _expect(updateResult.compacted, 'Should compact create + update');
      _expect(!updateResult.eliminated, 'Should not eliminate create + update');

      final all = await queue.getAll();
      _expectEquals(all.length, 1);
      _expectEquals(all.first.type, SyncOperationType.create);
      _expectEquals(all.first.payload!['title'], 'Final');
      _expectEquals(all.first.payload!['starred'], true);
    },
  );

  await runTest(
    'OutboxQueue eliminates create + delete completely (0 network calls)',
    () async {
      final queue = OutboxQueue(entityName: 'notes');

      await queue.enqueue(
        entityId: 'temp-1',
        type: SyncOperationType.create,
        payload: <String, Object?>{'id': 'temp-1', 'title': 'Temp'},
      );

      final deleteResult = await queue.enqueue(
        entityId: 'temp-1',
        type: SyncOperationType.delete,
      );

      _expect(deleteResult.compacted, 'Should compact create + delete');
      _expect(deleteResult.eliminated, 'Should eliminate create + delete');
      _expect((await queue.getAll()).isEmpty, 'Queue should be empty');
    },
  );

  await runTest(
    'OutboxQueue compacts update + delete into a single delete',
    () async {
      final queue = OutboxQueue(entityName: 'notes');

      await queue.enqueue(
        entityId: 'existing-1',
        type: SyncOperationType.update,
        payload: <String, Object?>{'id': 'existing-1', 'title': 'Edited'},
        changedFields: <String>{'title'},
      );

      await queue.enqueue(
        entityId: 'existing-1',
        type: SyncOperationType.delete,
      );

      final all = await queue.getAll();
      _expectEquals(all.length, 1);
      _expectEquals(all.first.type, SyncOperationType.delete);
      _expectEquals(all.first.payload, null);
    },
  );

  await runTest(
    'JsonStringOutboxStorage persists and restores operations',
    () async {
      final backingMap = <String, String>{};
      final storage = JsonStringOutboxStorage(
        read: (key) async => backingMap[key],
        write: (key, value) async => backingMap[key] = value,
      );

      final queue1 = OutboxQueue(entityName: 'notes', storage: storage);
      await queue1.enqueue(
        entityId: 'n1',
        type: SyncOperationType.create,
        payload: <String, Object?>{'id': 'n1', 'title': 'Persisted'},
        changedFields: <String>{'id', 'title'},
      );

      final queue2 = OutboxQueue(entityName: 'notes', storage: storage);
      final restored = await queue2.getAll();
      _expectEquals(restored.length, 1);
      _expectEquals(restored.first.entityId, 'n1');
      _expectEquals(restored.first.payload!['title'], 'Persisted');
    },
  );

  await runTest(
    'SyncPilotEngine resolves 3-way field-level conflicts without data loss',
    () async {
      final baseNote = TestNote(
        id: 'n1',
        title: 'Original Title',
        body: 'Original Body',
        starred: false,
        updatedAt: DateTime.utc(2026, 1, 1, 10),
      );

      final remoteStore = <String, TestNote>{
        'n1': baseNote.copyWith(
          body: 'Remote Updated Body',
          updatedAt: DateTime.utc(2026, 1, 1, 12),
        ),
      };

      final connectivity = ManualConnectivityMonitor(initialOnline: false);

      final engine = SyncPilotEngine<TestNote>(
        entityName: 'notes',
        getId: (n) => n.id,
        toJson: (n) => n.toJson(),
        fromJson: TestNote.fromJson,
        localStore: InMemorySyncStore<TestNote>(
          getId: (n) => n.id,
          initialItems: <TestNote>[baseNote],
        ),
        connectivityMonitor: connectivity,
        conflictResolver: ConflictResolver<TestNote>.fieldLevelMerge(
          fromJson: TestNote.fromJson,
        ),
        remoteAdapter: CallbackRemoteSyncAdapter<TestNote>(
          onPush: (op) async {
            final incoming = TestNote.fromJson(op.payload!);
            if (op.metadata['conflictResolved'] != true) {
              return PushOperationResult<TestNote>.conflict(
                remoteRecord: remoteStore[op.entityId]!,
              );
            }
            remoteStore[op.entityId] = incoming;
            return PushOperationResult<TestNote>.success(
              serverRecord: incoming,
            );
          },
          onPullDelta: (checkpoint) async {
            return RemoteDeltaPage<TestNote>(
              upserted: remoteStore.values.toList(),
              nextCheckpoint: 'cp-1',
            );
          },
        ),
      );

      await engine.update(
        baseNote.copyWith(
          starred: true,
          updatedAt: DateTime.utc(2026, 1, 1, 11),
        ),
      );

      _expectEquals(
        await engine.getRecordSyncState('n1'),
        SyncRecordState.pendingUpdate,
      );

      connectivity.setOnline(true);
      final report = await engine.sync();

      _expect(report.isSuccess, 'Sync cycle should succeed');
      _expectEquals(report.conflictsResolvedCount, 1);

      final merged = await engine.getById('n1');
      _expect(merged != null, 'Merged note should exist');
      _expectEquals(merged!.body, 'Remote Updated Body');
      _expectEquals(merged.starred, true);
      _expectEquals(
        await engine.getRecordSyncState('n1'),
        SyncRecordState.synced,
      );

      await engine.dispose();
    },
  );

  await runTest(
    'SyncPilotEngine retries transient failures with exponential backoff',
    () async {
      var attempts = 0;
      final engine = SyncPilotEngine<TestNote>(
        entityName: 'notes',
        getId: (n) => n.id,
        toJson: (n) => n.toJson(),
        fromJson: TestNote.fromJson,
        localStore: InMemorySyncStore<TestNote>(getId: (n) => n.id),
        retryPolicy: RetryPolicy.fast,
        remoteAdapter: CallbackRemoteSyncAdapter<TestNote>(
          onPush: (op) async {
            attempts++;
            if (attempts < 3) {
              return const PushOperationResult<TestNote>.retryableFailure(
                message: 'HTTP 503 Service Unavailable',
              );
            }
            return PushOperationResult<TestNote>.success(
              serverRecord: TestNote.fromJson(op.payload!),
            );
          },
          onPullDelta: (_) async => const RemoteDeltaPage<TestNote>(),
        ),
      );

      await engine.create(
        TestNote(
          id: 'n2',
          title: 'Retry Test',
          body: 'Body',
          starred: false,
          updatedAt: DateTime.utc(2026, 1, 1),
        ),
      );

      final report = await engine.sync();
      _expect(report.isSuccess, 'Retry should eventually succeed');
      _expectEquals(report.pushedCount, 1);
      _expectEquals(attempts, 3);

      await engine.dispose();
    },
  );

  await runTest(
    'SyncPilotEngine pulls paginated delta pages and updates checkpoint',
    () async {
      final engine = SyncPilotEngine<TestNote>(
        entityName: 'notes',
        getId: (n) => n.id,
        toJson: (n) => n.toJson(),
        fromJson: TestNote.fromJson,
        localStore: InMemorySyncStore<TestNote>(getId: (n) => n.id),
        remoteAdapter: CallbackRemoteSyncAdapter<TestNote>(
          onPush: (_) async => const PushOperationResult<TestNote>.success(),
          onPullDelta: (checkpoint) async {
            if (checkpoint == null) {
              return RemoteDeltaPage<TestNote>(
                upserted: <TestNote>[
                  TestNote(
                    id: 'p1',
                    title: 'Page 1',
                    body: '',
                    starred: false,
                    updatedAt: DateTime.utc(2026, 1, 1),
                  ),
                ],
                nextCheckpoint: 'cursor-1',
                hasMore: true,
              );
            }
            return RemoteDeltaPage<TestNote>(
              upserted: <TestNote>[
                TestNote(
                  id: 'p2',
                  title: 'Page 2',
                  body: '',
                  starred: true,
                  updatedAt: DateTime.utc(2026, 1, 2),
                ),
              ],
              nextCheckpoint: 'cursor-2',
              hasMore: false,
            );
          },
        ),
      );

      final report = await engine.sync();
      _expectEquals(report.pulledUpsertedCount, 2);
      _expectEquals((await engine.getAll()).length, 2);
      _expectEquals(
        await engine.localStore.getSyncCheckpoint('notes'),
        'cursor-2',
      );

      await engine.dispose();
    },
  );

  await runTest(
    'SyncCoordinator synchronizes multiple engines in dependency order',
    () async {
      final syncOrder = <String>[];

      SyncPilotEngine<TestNote> createEngine(String name) {
        return SyncPilotEngine<TestNote>(
          entityName: name,
          getId: (n) => n.id,
          toJson: (n) => n.toJson(),
          fromJson: TestNote.fromJson,
          localStore: InMemorySyncStore<TestNote>(getId: (n) => n.id),
          remoteAdapter: CallbackRemoteSyncAdapter<TestNote>(
            onPush: (_) async => const PushOperationResult<TestNote>.success(),
            onPullDelta: (_) async {
              syncOrder.add(name);
              return const RemoteDeltaPage<TestNote>();
            },
          ),
        );
      }

      final coordinator = SyncCoordinator(
        engines: <SyncPilotEngine<Object?>>[
          createEngine('workspaces'),
          createEngine('projects'),
          createEngine('tasks'),
        ],
      );

      final reports = await coordinator.syncAll();
      _expectEquals(reports.length, 3);
      _expectEquals(syncOrder.join(','), 'workspaces,projects,tasks');

      await coordinator.disposeAll();
    },
  );

  await runTest(
    'Optimistic Temporary ID Mapping replaces local & subsequent outbox IDs',
    () async {
      final pushedOperations = <SyncOperation>[];
      final mappingEvents = <IdMappingEvent>[];

      final engine = SyncPilotEngine<TestNote>(
        entityName: 'notes',
        getId: (n) => n.id,
        toJson: (n) => n.toJson(),
        fromJson: TestNote.fromJson,
        localStore: InMemorySyncStore<TestNote>(getId: (n) => n.id),
        // Disable compaction to simulate two separate queued operations on
        // the same temporary ID (`create` followed by `update`).
        outbox: OutboxQueue(entityName: 'notes', enableCompaction: false),
        remoteAdapter: CallbackRemoteSyncAdapter<TestNote>(
          onPush: (op) async {
            pushedOperations.add(op);
            if (op.type == SyncOperationType.create) {
              // Server replaces temporary ID 'temp_123' with 'srv_999'!
              final assigned = TestNote.fromJson(op.payload!).copyWith();
              final serverJson = <String, Object?>{
                ...assigned.toJson(),
                'id': 'srv_999',
              };
              return PushOperationResult<TestNote>.success(
                serverRecord: TestNote.fromJson(serverJson),
              );
            }
            return PushOperationResult<TestNote>.success(
              serverRecord: TestNote.fromJson(op.payload!),
            );
          },
          onPullDelta: (_) async => const RemoteDeltaPage<TestNote>(),
        ),
      );

      final sub = engine.idMappingStream.listen(mappingEvents.add);

      final draft = TestNote(
        id: 'temp_123',
        title: 'Offline Draft',
        body: 'Initial',
        starred: false,
        updatedAt: DateTime.utc(2026, 1, 1, 10),
      );
      await engine.create(draft);
      await engine.update(draft.copyWith(title: 'Offline Edited'));

      final report = await engine.sync();
      await Future<void>.delayed(Duration.zero);

      _expect(report.isSuccess, 'Sync should succeed');
      _expectEquals(report.idMappings['temp_123'], 'srv_999');
      _expectEquals(mappingEvents.length, 1);
      _expectEquals(mappingEvents.first.temporaryId, 'temp_123');
      _expectEquals(mappingEvents.first.permanentId, 'srv_999');

      // The second operation (update) must have been rewritten to 'srv_999'
      // BEFORE it was pushed to the server!
      _expectEquals(pushedOperations.length, 2);
      _expectEquals(pushedOperations[0].entityId, 'temp_123');
      _expectEquals(pushedOperations[1].entityId, 'srv_999');
      _expectEquals(pushedOperations[1].payload!['id'], 'srv_999');

      // Local store should only contain 'srv_999', and lookup by old temp ID
      // should transparently resolve to the permanent record!
      final allLocal = await engine.getAll();
      _expectEquals(allLocal.length, 1);
      _expectEquals(allLocal.first.id, 'srv_999');
      _expectEquals(allLocal.first.title, 'Offline Edited');
      _expectEquals((await engine.getById('temp_123'))?.id, 'srv_999');

      await sub.cancel();
      await engine.dispose();
    },
  );

  await runTest(
    'SyncCoordinator propagates parent temporary ID mappings to child foreign keys',
    () async {
      // Parent entity: Project (stored as TestNote where id is projectId)
      // Child entity: Task (stored as TestNote where body holds parent projectId)
      final pushedChildTasks = <SyncOperation>[];

      final projectsEngine = SyncPilotEngine<TestNote>(
        entityName: 'projects',
        getId: (p) => p.id,
        toJson: (p) => p.toJson(),
        fromJson: TestNote.fromJson,
        localStore: InMemorySyncStore<TestNote>(getId: (p) => p.id),
        remoteAdapter: CallbackRemoteSyncAdapter<TestNote>(
          onPush: (op) async {
            final serverJson = <String, Object?>{
              ...op.payload!,
              'id': 'srv_proj_777',
            };
            return PushOperationResult<TestNote>.success(
              serverRecord: TestNote.fromJson(serverJson),
            );
          },
          onPullDelta: (_) async => const RemoteDeltaPage<TestNote>(),
        ),
      );

      final tasksEngine = SyncPilotEngine<TestNote>(
        entityName: 'tasks',
        getId: (t) => t.id,
        toJson: (t) => t.toJson(),
        fromJson: TestNote.fromJson,
        localStore: InMemorySyncStore<TestNote>(getId: (t) => t.id),
        remoteAdapter: CallbackRemoteSyncAdapter<TestNote>(
          onPush: (op) async {
            pushedChildTasks.add(op);
            final serverJson = <String, Object?>{
              ...op.payload!,
              'id': 'srv_task_888',
            };
            return PushOperationResult<TestNote>.success(
              serverRecord: TestNote.fromJson(serverJson),
            );
          },
          onPullDelta: (_) async => const RemoteDeltaPage<TestNote>(),
        ),
      );

      // Offline: Create parent project with 'temp_proj_1' and child task
      // referencing 'temp_proj_1' in its `body` field.
      await projectsEngine.create(
        TestNote(
          id: 'temp_proj_1',
          title: 'New Project',
          body: 'Root',
          starred: false,
          updatedAt: DateTime.utc(2026, 1, 1),
        ),
      );
      await tasksEngine.create(
        TestNote(
          id: 'temp_task_1',
          title: 'Child Task',
          body: 'temp_proj_1', // foreign key reference to parent project
          starred: false,
          updatedAt: DateTime.utc(2026, 1, 1),
        ),
      );

      final coordinator = SyncCoordinator(
        engines: <SyncPilotEngine<Object?>>[projectsEngine, tasksEngine],
      );

      final reports = await coordinator.syncAll();
      _expectEquals(reports.length, 2);
      _expectEquals(pushedChildTasks.length, 1);

      // Verify child task payload was updated with 'srv_proj_777' before push!
      _expectEquals(pushedChildTasks.first.payload!['body'], 'srv_proj_777');

      final syncedTask = await tasksEngine.getById('temp_task_1');
      _expectEquals(syncedTask?.id, 'srv_task_888');
      _expectEquals(syncedTask?.body, 'srv_proj_777');

      await coordinator.disposeAll();
    },
  );

  await runTest(
    'Batch Push groups operations into chunks of pushBatchSize',
    () async {
      final batchSizes = <int>[];

      final engine = SyncPilotEngine<TestNote>(
        entityName: 'notes',
        getId: (n) => n.id,
        toJson: (n) => n.toJson(),
        fromJson: TestNote.fromJson,
        localStore: InMemorySyncStore<TestNote>(getId: (n) => n.id),
        enableBatchPush: true,
        pushBatchSize: 2,
        remoteAdapter: CallbackRemoteSyncAdapter<TestNote>(
          onPush: (_) async => const PushOperationResult<TestNote>.success(),
          onPushBatch: (operations) async {
            batchSizes.add(operations.length);
            return operations
                .map(
                  (op) => PushOperationResult<TestNote>.success(
                    serverRecord: TestNote.fromJson(op.payload!),
                  ),
                )
                .toList();
          },
          onPullDelta: (_) async => const RemoteDeltaPage<TestNote>(),
        ),
      );

      for (var i = 1; i <= 5; i++) {
        await engine.create(
          TestNote(
            id: 'n_$i',
            title: 'Note $i',
            body: 'Body $i',
            starred: false,
            updatedAt: DateTime.utc(2026, 1, 1),
          ),
        );
      }

      final report = await engine.sync();
      _expect(report.isSuccess, 'Batch sync should succeed');
      _expectEquals(report.pushedCount, 5);
      _expectEquals(batchSizes.join(','), '2,2,1');

      await engine.dispose();
    },
  );

  await runTest(
    'RestSyncAdapter sends POST, partial PATCH, DELETE, and handles 409/GET delta',
    () async {
      final recordedRequests = <RestRequest>[];

      final adapter = RestSyncAdapter<TestNote>(
        basePath: '/api/v1/notes',
        fromJson: TestNote.fromJson,
        sendRequest: (request) async {
          recordedRequests.add(request);
          if (request.method == RestMethod.post) {
            return RestResponse(
              statusCode: 201,
              body: request.body,
            );
          }
          if (request.method == RestMethod.patch) {
            // Simulate 409 conflict on first PATCH, then 200 on resolved retry
            if (request.headers['X-Conflict-Resolved'] != 'true') {
              return const RestResponse(
                statusCode: 409,
                body: <String, Object?>{
                  'id': 'n1',
                  'title': 'Server Title',
                  'body': 'Original Body',
                  'starred': false,
                  'updatedAt': '2026-01-01T12:00:00.000Z',
                },
              );
            }
            return RestResponse(statusCode: 200, body: request.body);
          }
          if (request.method == RestMethod.get) {
            return const RestResponse(
              statusCode: 200,
              body: <String, Object?>{
                'upserted': <Object?>[],
                'deletedIds': <Object?>[],
                'nextCheckpoint': 'cp-rest-99',
                'hasMore': false,
              },
            );
          }
          return const RestResponse(statusCode: 204);
        },
      );

      final initialNote = TestNote(
        id: 'n1',
        title: 'Original Title',
        body: 'Original Body',
        starred: false,
        updatedAt: DateTime.utc(2026, 1, 1, 10),
      );

      final engine = SyncPilotEngine<TestNote>(
        entityName: 'notes',
        getId: (n) => n.id,
        toJson: (n) => n.toJson(),
        fromJson: TestNote.fromJson,
        localStore: InMemorySyncStore<TestNote>(
          getId: (n) => n.id,
          initialItems: <TestNote>[initialNote],
        ),
        conflictResolver: ConflictResolver<TestNote>.fieldLevelMerge(
          fromJson: TestNote.fromJson,
        ),
        remoteAdapter: adapter,
      );

      // Update ONLY `starred` locally -> first PATCH should send ONLY `starred`
      await engine.update(initialNote.copyWith(starred: true));

      final report = await engine.sync();
      _expect(report.isSuccess, 'REST sync should succeed');
      _expectEquals(report.conflictsResolvedCount, 1);

      // Request 0: initial PATCH (only `starred` field!)
      _expectEquals(recordedRequests[0].method, RestMethod.patch);
      _expectEquals(recordedRequests[0].path, '/api/v1/notes/n1');
      final firstPatchBody = recordedRequests[0].body! as Map<String, Object?>;
      _expectEquals(firstPatchBody.keys.join(','), 'starred');
      _expectEquals(firstPatchBody['starred'], true);

      // Request 1: conflict-resolved PATCH with merged record
      _expectEquals(recordedRequests[1].method, RestMethod.patch);
      _expectEquals(
        recordedRequests[1].headers['X-Conflict-Resolved'],
        'true',
      );

      // Request 2: GET delta pull
      _expectEquals(recordedRequests[2].method, RestMethod.get);
      _expectEquals(
        await engine.localStore.getSyncCheckpoint('notes'),
        'cp-rest-99',
      );

      await engine.dispose();
    },
  );

  await runTest(
    'KeyValueSyncStore (Hive/SharedPreferences adapter) supports reactive CRUD',
    () async {
      final box = <String, String>{};
      final checkpoints = <String, String?>{};

      final store = KeyValueSyncStore<TestNote>.fromJsonStrings(
        getId: (n) => n.id,
        toJson: (n) => n.toJson(),
        fromJson: TestNote.fromJson,
        readAllStrings: () => Map<String, String>.of(box),
        readString: (id) => box[id],
        putString: (id, json) => box[id] = json,
        deleteString: box.remove,
        clearStrings: box.clear,
        readCheckpoint: (entity) => checkpoints[entity],
        writeCheckpoint: (entity, cp) => checkpoints[entity] = cp,
      );

      final note = TestNote(
        id: 'kv-1',
        title: 'Hive Note',
        body: 'Stored in KV box',
        starred: true,
        updatedAt: DateTime.utc(2026, 1, 1),
      );

      await store.upsert(note);
      final fetched = await store.getById('kv-1');
      _expectEquals(fetched?.title, 'Hive Note');

      await store.saveSyncCheckpoint('notes', 'cp-kv-1');
      _expectEquals(await store.getSyncCheckpoint('notes'), 'cp-kv-1');

      final deleted = await store.deleteById('kv-1');
      _expect(deleted, 'Should delete kv-1');
      _expectEquals(await store.getById('kv-1'), null);

      await store.dispose();
    },
  );

  await runTest(
    'SqlSyncStore and SqlOutboxStorage execute SQLite DDL/DML statements cleanly',
    () async {
      // Lightweight in-memory SQL simulator for verifying SQLite adapter queries
      final tables = <String, Map<String, Map<String, Object?>>>{};
      final executedSql = <String>[];

      final executor = CallbackSqlExecutor(
        onExecute: (sql, args) async {
          executedSql.add(sql);
          if (sql.startsWith('CREATE TABLE IF NOT EXISTS ')) {
            final name =
                sql.split('CREATE TABLE IF NOT EXISTS ')[1].split(' ')[0];
            tables.putIfAbsent(name, () => <String, Map<String, Object?>>{});
          } else if (sql.startsWith('INSERT OR REPLACE INTO notes')) {
            tables['notes']![args[0]! as String] = <String, Object?>{
              'id': args[0],
              'payload': args[1],
              'updated_at': args[2],
            };
          } else if (sql.startsWith('DELETE FROM notes WHERE id = ?')) {
            tables['notes']?.remove(args[0]! as String);
          } else if (sql.startsWith(
            'INSERT OR REPLACE INTO sync_pilot_checkpoints',
          )) {
            tables['sync_pilot_checkpoints']![args[0]! as String] =
                <String, Object?>{
              'entity_name': args[0],
              'checkpoint': args[1],
            };
          } else if (sql.startsWith(
            'DELETE FROM sync_pilot_outbox WHERE entity_name = ?',
          )) {
            tables['sync_pilot_outbox']?.clear();
          } else if (sql.startsWith('INSERT INTO sync_pilot_outbox')) {
            tables['sync_pilot_outbox']![args[0]! as String] =
                <String, Object?>{
              'id': args[0],
              'entity_name': args[1],
              'entity_id': args[2],
              'operation_json': args[3],
            };
          }
        },
        onQuery: (sql, args) async {
          if (sql.startsWith('SELECT payload FROM notes WHERE id = ?')) {
            final row = tables['notes']?[args[0]! as String];
            return row != null ? <Map<String, Object?>>[row] : const [];
          }
          if (sql.startsWith('SELECT payload FROM notes ORDER BY')) {
            return tables['notes']?.values.toList() ?? const [];
          }
          if (sql.startsWith(
            'SELECT checkpoint FROM sync_pilot_checkpoints',
          )) {
            final row = tables['sync_pilot_checkpoints']?[args[0]! as String];
            return row != null ? <Map<String, Object?>>[row] : const [];
          }
          if (sql.startsWith(
            'SELECT operation_json FROM sync_pilot_outbox',
          )) {
            return tables['sync_pilot_outbox']?.values.toList() ?? const [];
          }
          return const [];
        },
      );

      final sqlStore = SqlSyncStore<TestNote>(
        executor: executor,
        tableName: 'notes',
        getId: (n) => n.id,
        toJson: (n) => n.toJson(),
        fromJson: TestNote.fromJson,
      );

      final sqlOutbox = SqlOutboxStorage(executor: executor);
      final outboxQueue = OutboxQueue(
        entityName: 'notes',
        storage: sqlOutbox,
      );

      final note = TestNote(
        id: 'sql-1',
        title: 'SQLite Note',
        body: 'Persisted via SqlSyncStore',
        starred: false,
        updatedAt: DateTime.utc(2026, 1, 1),
      );

      await sqlStore.upsert(note);
      _expectEquals((await sqlStore.getById('sql-1'))?.title, 'SQLite Note');

      await sqlStore.saveSyncCheckpoint('notes', 'cp-sql-42');
      _expectEquals(await sqlStore.getSyncCheckpoint('notes'), 'cp-sql-42');

      await outboxQueue.enqueue(
        entityId: 'sql-1',
        type: SyncOperationType.create,
        payload: note.toJson(),
      );
      final loadedOps = await outboxQueue.getAll();
      _expectEquals(loadedOps.length, 1);
      _expectEquals(loadedOps.first.entityId, 'sql-1');
      _expect(
        executedSql.any((s) => s.contains('CREATE TABLE IF NOT EXISTS notes')),
        'Should auto-create SQL entity table',
      );

      await sqlStore.dispose();
    },
  );

  await runTest(
    'SyncPayloadCodec encrypts/encodes outbox & local store payloads at rest',
    () async {
      final rawOutboxDisk = <String, String>{};
      final rawKvDisk = <String, String>{};

      // Chained pipeline: custom prefix/compression stage -> xorBase64 cipher
      final stage1 = SyncPayloadCodec.custom(
        encode: (plain) => 'COMPRESSED:$plain',
        decode: (encoded) => encoded.replaceFirst('COMPRESSED:', ''),
      );
      final stage2 = SyncPayloadCodec.xorBase64(
        keyBytes: const <int>[0x42, 0x99, 0x17, 0xE3],
      );
      final pipelineCodec = stage1.then(stage2);

      final encryptedOutbox = JsonStringOutboxStorage(
        read: (k) async => rawOutboxDisk[k],
        write: (k, v) async => rawOutboxDisk[k] = v,
        codec: pipelineCodec,
      );

      final encryptedKvStore = KeyValueSyncStore<TestNote>.fromJsonStrings(
        getId: (n) => n.id,
        toJson: (n) => n.toJson(),
        fromJson: TestNote.fromJson,
        readAllStrings: () => Map<String, String>.of(rawKvDisk),
        readString: (id) => rawKvDisk[id],
        putString: (id, v) => rawKvDisk[id] = v,
        deleteString: rawKvDisk.remove,
        clearStrings: rawKvDisk.clear,
        readCheckpoint: (_) => null,
        writeCheckpoint: (_, __) {},
        codec: pipelineCodec,
      );

      final secretNote = TestNote(
        id: 'sec-1',
        title: 'Patient Diagnosis',
        body: 'Account PIN 987654',
        starred: true,
        updatedAt: DateTime.utc(2026, 1, 1),
      );

      await encryptedKvStore.upsert(secretNote);
      final queue = OutboxQueue(entityName: 'notes', storage: encryptedOutbox);
      await queue.enqueue(
        entityId: 'sec-1',
        type: SyncOperationType.create,
        payload: secretNote.toJson(),
      );

      // Verify raw strings on disk do NOT expose plain-text sensitive fields!
      final rawStoredEntity = rawKvDisk['sec-1']!;
      final rawStoredOutbox = rawOutboxDisk['sync_pilot_outbox_notes']!;
      _expect(
        rawStoredEntity.startsWith('sp_v1:'),
        'Stored entity should have encrypted header',
      );
      _expect(
        !rawStoredEntity.contains('987654') &&
            !rawStoredEntity.contains('Patient Diagnosis'),
        'Raw entity on disk must not leak plain text',
      );
      _expect(
        !rawStoredOutbox.contains('987654'),
        'Raw outbox on disk must not leak plain text',
      );

      // Verify transparent decryption when reading back
      final decryptedNote = await encryptedKvStore.getById('sec-1');
      _expectEquals(decryptedNote?.title, 'Patient Diagnosis');
      _expectEquals(decryptedNote?.body, 'Account PIN 987654');

      final decryptedOps = await queue.getAll();
      _expectEquals(decryptedOps.length, 1);
      _expectEquals(decryptedOps.first.payload!['body'], 'Account PIN 987654');

      await encryptedKvStore.dispose();
    },
  );

  print('\nAll $passed tests passed successfully!');
}
