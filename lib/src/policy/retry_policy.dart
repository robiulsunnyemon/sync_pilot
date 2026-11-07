import 'dart:math';

/// Configures how `SyncPilotEngine` retries transient failures when pushing
/// operations to the remote server.
class RetryPolicy {
  /// Creates a [RetryPolicy] with exponential backoff and optional jitter.
  const RetryPolicy({
    this.maxRetries = 5,
    this.initialDelay = const Duration(milliseconds: 500),
    this.maxDelay = const Duration(seconds: 30),
    this.backoffMultiplier = 2.0,
    this.useJitter = true,
  })  : assert(maxRetries >= 0, 'maxRetries must be non-negative'),
        assert(backoffMultiplier >= 1.0, 'backoffMultiplier must be >= 1.0');

  /// A retry policy that disables automatic in-cycle retries (`maxRetries: 0`).
  static const RetryPolicy none = RetryPolicy(
    maxRetries: 0,
    initialDelay: Duration.zero,
  );

  /// Fast retry policy suitable for unit tests and local development.
  static const RetryPolicy fast = RetryPolicy(
    maxRetries: 3,
    initialDelay: Duration(milliseconds: 10),
    maxDelay: Duration(milliseconds: 100),
    useJitter: false,
  );

  /// Maximum number of retry attempts before an operation is marked as failed.
  final int maxRetries;

  /// Base delay before the first retry attempt.
  final Duration initialDelay;

  /// Upper bound on the computed backoff delay.
  final Duration maxDelay;

  /// Multiplicative factor applied to the delay after each attempt.
  final double backoffMultiplier;

  /// Whether to add randomized jitter (`50%–100%` of computed delay) to avoid
  /// thundering-herd spikes when many clients reconnect simultaneously.
  final bool useJitter;

  /// Returns `true` if an operation with [currentRetryCount] failed attempts
  /// is still eligible for retry.
  bool canRetry(int currentRetryCount) => currentRetryCount < maxRetries;

  /// Computes the delay before retry attempt [attemptNumber] (`1`-based).
  Duration delayForAttempt(int attemptNumber, {Random? random}) {
    if (attemptNumber <= 0 || initialDelay == Duration.zero) {
      return Duration.zero;
    }

    final factor = pow(backoffMultiplier, attemptNumber - 1);
    final rawMs = (initialDelay.inMilliseconds * factor).round();
    final cappedMs = min(rawMs, maxDelay.inMilliseconds);

    if (!useJitter) {
      return Duration(milliseconds: cappedMs);
    }

    final rng = random ?? Random();
    // Equal jitter: between 50% and 100% of cappedMs.
    final halfMs = cappedMs ~/ 2;
    final jitteredMs = halfMs + rng.nextInt(max(1, cappedMs - halfMs + 1));
    return Duration(milliseconds: jitteredMs);
  }
}
