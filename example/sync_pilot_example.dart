import 'package:sync_pilot/sync_pilot.dart';

/// Domain model for a collaborative Task item.
class Task {
  const Task({
    required this.id,
    required this.title,
    required this.completed,
    required this.assignee,
    required this.updatedAt,
  });

  factory Task.fromJson(Map<String, Object?> json) {
    return Task(
      id: json['id']! as String,
      title: json['title']! as String,
      completed: (json['completed'] as bool?) ?? false,
      assignee: (json['assignee'] as String?) ?? 'Unassigned',
      updatedAt: DateTime.parse(json['updatedAt']! as String).toUtc(),
    );
  }

  final String id;
  final String title;
  final bool completed;
  final String assignee;
  final DateTime updatedAt;

  Task copyWith({
    String? title,
    bool? completed,
    String? assignee,
    DateTime? updatedAt,
  }) {
    return Task(
      id: id,
      title: title ?? this.title,
      completed: completed ?? this.completed,
      assignee: assignee ?? this.assignee,
      updatedAt: updatedAt ?? this.updatedAt,
    );
  }

  Map<String, Object?> toJson() => <String, Object?>{
        'id': id,
        'title': title,
        'completed': completed,
        'assignee': assignee,
        'updatedAt': updatedAt.toUtc().toIso8601String(),
      };

  @override
  String toString() =>
      'Task(id: $id, title: "$title", completed: $completed, assignee: $assignee)';
}

Future<void> main() async {
  // Simulated remote backend store.
  final remoteDatabase = <String, Task>{
    'task-1': Task(
      id: 'task-1',
      title: 'Initial Roadmap',
      completed: false,
      assignee: 'Alice',
      updatedAt: DateTime.utc(2026, 1, 1, 10),
    ),
  };

  final connectivity = ManualConnectivityMonitor(initialOnline: false);

  final engine = SyncPilotEngine<Task>(
    entityName: 'tasks',
    getId: (task) => task.id,
    toJson: (task) => task.toJson(),
    fromJson: Task.fromJson,
    localStore: InMemorySyncStore<Task>(
      getId: (task) => task.id,
      initialItems: remoteDatabase.values,
    ),
    connectivityMonitor: connectivity,
    conflictResolver: ConflictResolver<Task>.fieldLevelMerge(
      fromJson: Task.fromJson,
    ),
    remoteAdapter: CallbackRemoteSyncAdapter<Task>(
      onPush: (operation) async {
        final id = operation.entityId;
        switch (operation.type) {
          case SyncOperationType.create:
            final created = Task.fromJson(operation.payload!);
            remoteDatabase[id] = created;
            return PushOperationResult.success(serverRecord: created);

          case SyncOperationType.update:
            final incoming = Task.fromJson(operation.payload!);
            final serverCurrent = remoteDatabase[id];
            final alreadyResolved =
                operation.metadata['conflictResolved'] == true;

            // Simulate a server conflict if the server record is newer than
            // the client's base snapshot and conflict hasn't been resolved yet.
            if (!alreadyResolved &&
                serverCurrent != null &&
                operation.basePayload != null) {
              final baseTime = DateTime.parse(
                operation.basePayload!['updatedAt']! as String,
              ).toUtc();
              if (serverCurrent.updatedAt.isAfter(baseTime)) {
                return PushOperationResult.conflict(
                  remoteRecord: serverCurrent,
                );
              }
            }
            remoteDatabase[id] = incoming;
            return PushOperationResult.success(serverRecord: incoming);

          case SyncOperationType.delete:
            remoteDatabase.remove(id);
            return const PushOperationResult.success();
        }
      },
      onPullDelta: (checkpoint) async {
        return RemoteDeltaPage<Task>(
          upserted: remoteDatabase.values.toList(),
          nextCheckpoint: DateTime.now().toUtc().toIso8601String(),
          hasMore: false,
        );
      },
    ),
  );

  print('--- 1. Working Offline ---');
  // Create a new task offline and edit it twice -> compacts into 1 CREATE.
  final draft = Task(
    id: 'task-2',
    title: 'Write Docs',
    completed: false,
    assignee: 'Bob',
    updatedAt: DateTime.utc(2026, 1, 1, 11),
  );
  await engine.create(draft);
  await engine.update(draft.copyWith(title: 'Write Comprehensive Docs'));
  await engine.update(
    draft.copyWith(title: 'Write Comprehensive Docs', completed: true),
  );

  // Create a temporary task offline and delete it -> compacts into 0 ops!
  await engine.create(
    Task(
      id: 'task-temp',
      title: 'Accidental Draft',
      completed: false,
      assignee: 'Bob',
      updatedAt: DateTime.utc(2026, 1, 1, 11, 5),
    ),
  );
  await engine.delete('task-temp');

  // Meanwhile, on the server, someone reassigns task-1 to 'Charlie',
  // while locally offline we mark task-1 as completed!
  remoteDatabase['task-1'] = remoteDatabase['task-1']!.copyWith(
    assignee: 'Charlie',
    updatedAt: DateTime.utc(2026, 1, 1, 12),
  );
  final localTask1 = (await engine.getById('task-1'))!;
  await engine.update(
    localTask1.copyWith(
      completed: true,
      updatedAt: DateTime.utc(2026, 1, 1, 11, 30),
    ),
  );

  print('Pending operations in outbox: ${engine.currentStatus.pendingCount}');

  print('\n--- 2. Going Online & Syncing ---');
  connectivity.setOnline(true);
  final report = await engine.sync();
  print('Sync Report: $report');

  print('\n--- 3. Final Synchronized Tasks ---');
  for (final task in await engine.getAll()) {
    print(' - $task');
  }

  await engine.dispose();
}
