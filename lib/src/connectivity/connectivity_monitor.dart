import 'dart:async';

/// Abstraction for monitoring network connectivity state.
///
/// Allows `SyncPilotEngine` to stay 100% pure Dart while seamlessly integrating
/// with `connectivity_plus`, `internet_connection_checker`, WebSocket state,
/// or custom health-check streams in Flutter or Dart CLI applications.
abstract interface class ConnectivityMonitor {
  /// Whether the device currently has network connectivity.
  FutureOr<bool> get isOnline;

  /// Stream that emits `true` when connectivity is restored and `false` when
  /// connectivity is lost.
  Stream<bool> get onConnectivityChanged;

  /// Releases any resources held by this monitor.
  Future<void> dispose();
}

/// A controllable [ConnectivityMonitor] that can be toggled programmatically
/// or bridged to an external connectivity stream.
///
/// Ideal for:
/// * Bridging `connectivity_plus` (`setOnline(...)` on stream events).
/// * Toggling offline mode in user settings ("Work Offline" switch).
/// * Deterministic testing of offline-to-online transitions.
class ManualConnectivityMonitor implements ConnectivityMonitor {
  /// Creates a [ManualConnectivityMonitor] with an initial [initialOnline]
  /// status.
  ManualConnectivityMonitor({bool initialOnline = true})
      : _isOnline = initialOnline;

  /// Creates a [ManualConnectivityMonitor] that listens to an external
  /// boolean connectivity [stream].
  factory ManualConnectivityMonitor.fromStream(
    Stream<bool> stream, {
    bool initialOnline = true,
  }) {
    final monitor = ManualConnectivityMonitor(initialOnline: initialOnline);
    monitor._externalSubscription = stream.listen(monitor.setOnline);
    return monitor;
  }

  bool _isOnline;
  StreamSubscription<bool>? _externalSubscription;
  final StreamController<bool> _controller = StreamController<bool>.broadcast();

  @override
  bool get isOnline => _isOnline;

  @override
  Stream<bool> get onConnectivityChanged => _controller.stream;

  /// Updates the current connectivity status and notifies listeners if changed.
  void setOnline(bool online) {
    if (_isOnline == online) return;
    _isOnline = online;
    if (!_controller.isClosed) {
      _controller.add(online);
    }
  }

  @override
  Future<void> dispose() async {
    await _externalSubscription?.cancel();
    await _controller.close();
  }
}
