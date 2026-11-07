import 'dart:async';

import '../models/sync_status.dart';
import 'sync_pilot_engine.dart';

/// Orchestrates multiple [SyncPilotEngine] instances in registration order.
///
/// Useful when an application has relational entities that must be synchronized
/// sequentially (for example: `workspaces` -> `projects` -> `tasks`) so foreign
/// key references are satisfied on the backend.
///
/// Automatically propagates optimistic temporary-to-permanent ID mappings
/// (`temp_123` -> `srv_999`) from parent engines to all registered child
/// engines during [syncAll].
class SyncCoordinator {
  /// Creates a [SyncCoordinator] with an optional initial list of [engines].
  SyncCoordinator({Iterable<SyncPilotEngine<Object?>>? engines}) {
    if (engines != null) {
      for (final engine in engines) {
        register(engine);
      }
    }
  }

  final Map<String, SyncPilotEngine<Object?>> _engines =
      <String, SyncPilotEngine<Object?>>{};

  /// Names of all registered entity engines in synchronization order.
  Iterable<String> get registeredEntities => _engines.keys;

  /// Registers an [engine] with this coordinator.
  void register(SyncPilotEngine<Object?> engine) {
    _engines[engine.entityName] = engine;
  }

  /// Unregisters the engine for [entityName].
  void unregister(String entityName) {
    _engines.remove(entityName);
  }

  /// Synchronizes all registered engines sequentially in registration order
  /// and returns their [SyncCycleReport] list.
  ///
  /// Any optimistic temporary IDs resolved by an earlier engine
  /// (`report.idMappings`) are automatically propagated across all other
  /// registered engines' local stores and outbox queues before subsequent
  /// engines push their mutations.
  ///
  /// If [stopOnFirstFailure] is `true`, remaining engines are skipped if an
  /// earlier engine finishes with failed operations.
  Future<List<SyncCycleReport>> syncAll({
    bool stopOnFirstFailure = false,
  }) async {
    final reports = <SyncCycleReport>[];
    final engineList = _engines.values.toList(growable: false);

    for (var i = 0; i < engineList.length; i++) {
      final engine = engineList[i];
      final report = await engine.sync();
      reports.add(report);

      // Propagate any temporary -> permanent ID mappings to all other engines
      // so foreign keys (e.g. projectId: 'temp_1' -> 'srv_99') are rewritten
      // before child engines push!
      if (report.idMappings.isNotEmpty) {
        for (final entry in report.idMappings.entries) {
          for (var j = 0; j < engineList.length; j++) {
            if (i == j) continue;
            await engineList[j].propagateIdMapping(
              oldId: entry.key,
              newId: entry.value,
            );
          }
        }
      }

      if (stopOnFirstFailure && !report.isSuccess) {
        break;
      }
    }
    return reports;
  }

  /// Disposes all registered engines.
  Future<void> disposeAll() async {
    for (final engine in _engines.values) {
      await engine.dispose();
    }
    _engines.clear();
  }
}
