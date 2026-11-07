import 'dart:async';
import 'dart:convert';

/// Pluggable transformation hook for encrypting, decrypting, compressing, or
/// decompressing serialized JSON payloads stored at rest in local storage and
/// the outbox queue.
///
/// Ideal for protecting sensitive offline data (such as financial transactions,
/// medical records, or private messages) in SQLite, Hive, or
/// `SharedPreferences`.
///
/// Codecs can be composed into pipelines (for example, compress then encrypt)
/// using [SyncPayloadCodec.chained] or [SyncPayloadCodecExtension.then].
abstract interface class SyncPayloadCodec {
  /// Default no-op codec that stores plain JSON strings without modification.
  const factory SyncPayloadCodec.identity() = _IdentityPayloadCodec;

  /// Creates a custom [SyncPayloadCodec] from [encode] and [decode] callbacks.
  ///
  /// Easily wraps packages like `cryptography`, `encrypt`, `pointycastle`, or
  /// `archive` (GZip/ZLib).
  const factory SyncPayloadCodec.custom({
    required FutureOr<String> Function(String plainText) encode,
    required FutureOr<String> Function(String encodedText) decode,
  }) = _CustomPayloadCodec;

  /// Built-in pure-Dart XOR + Base64 codec with a version prefix (`sp_v1:`).
  ///
  /// Useful for lightweight obfuscation, deterministic testing of encrypted
  /// storage pipelines, or custom byte-key masking without external
  /// dependencies.
  ///
  /// > Note: For high-security production encryption (AES-GCM / ChaCha20),
  /// > wrap a cryptographic library using [SyncPayloadCodec.custom].
  factory SyncPayloadCodec.xorBase64({
    required List<int> keyBytes,
  }) = _XorBase64PayloadCodec;

  /// Chains multiple [codecs] into a single pipeline.
  ///
  /// During [encode], codecs are applied forward (`codecs[0]`, `codecs[1]`, …).
  /// During [decode], codecs are applied in reverse order (`…`, `codecs[1]`,
  /// `codecs[0]`).
  factory SyncPayloadCodec.chained(List<SyncPayloadCodec> codecs) =
      _ChainedPayloadCodec;

  /// Transforms a plain-text JSON string before it is persisted to local
  /// storage or the outbox.
  FutureOr<String> encode(String plainText);

  /// Reverses [encode], restoring the original plain-text JSON string when
  /// reading from local storage or the outbox.
  FutureOr<String> decode(String encodedText);
}

/// Extension methods for composing [SyncPayloadCodec] instances.
extension SyncPayloadCodecExtension on SyncPayloadCodec {
  /// Returns a new [SyncPayloadCodec] that applies `this` codec followed by
  /// [next] during `encode`, and reverses the order (`next.decode` then
  /// `this.decode`) during `decode`.
  ///
  /// Example:
  /// ```dart
  /// final pipeline = gzipCodec.then(aesEncryptionCodec);
  /// ```
  SyncPayloadCodec then(SyncPayloadCodec next) {
    return SyncPayloadCodec.chained(<SyncPayloadCodec>[this, next]);
  }
}

final class _IdentityPayloadCodec implements SyncPayloadCodec {
  const _IdentityPayloadCodec();

  @override
  String encode(String plainText) => plainText;

  @override
  String decode(String encodedText) => encodedText;
}

final class _CustomPayloadCodec implements SyncPayloadCodec {
  const _CustomPayloadCodec({
    required FutureOr<String> Function(String plainText) encode,
    required FutureOr<String> Function(String encodedText) decode,
  })  : _encode = encode,
        _decode = decode;

  final FutureOr<String> Function(String plainText) _encode;
  final FutureOr<String> Function(String encodedText) _decode;

  @override
  FutureOr<String> encode(String plainText) => _encode(plainText);

  @override
  FutureOr<String> decode(String encodedText) => _decode(encodedText);
}

final class _XorBase64PayloadCodec implements SyncPayloadCodec {
  _XorBase64PayloadCodec({required List<int> keyBytes})
      : assert(keyBytes.isNotEmpty, 'keyBytes must not be empty'),
        _keyBytes = List<int>.unmodifiable(keyBytes);

  static const String _prefix = 'sp_v1:';
  final List<int> _keyBytes;

  @override
  String encode(String plainText) {
    final inputBytes = utf8.encode(plainText);
    final transformed = List<int>.generate(
      inputBytes.length,
      (i) => (inputBytes[i] ^ _keyBytes[i % _keyBytes.length]) & 0xFF,
      growable: false,
    );
    return '$_prefix${base64UrlEncode(transformed)}';
  }

  @override
  String decode(String encodedText) {
    if (!encodedText.startsWith(_prefix)) {
      throw const FormatException(
        'Invalid encoded payload: missing "$_prefix" header.',
      );
    }
    final rawBase64 = encodedText.substring(_prefix.length);
    final cipherBytes = base64Url.decode(rawBase64);
    final restored = List<int>.generate(
      cipherBytes.length,
      (i) => (cipherBytes[i] ^ _keyBytes[i % _keyBytes.length]) & 0xFF,
      growable: false,
    );
    return utf8.decode(restored);
  }
}

final class _ChainedPayloadCodec implements SyncPayloadCodec {
  _ChainedPayloadCodec(List<SyncPayloadCodec> codecs)
      : _codecs = List<SyncPayloadCodec>.unmodifiable(codecs);

  final List<SyncPayloadCodec> _codecs;

  @override
  Future<String> encode(String plainText) async {
    var current = plainText;
    for (final codec in _codecs) {
      current = await codec.encode(current);
    }
    return current;
  }

  @override
  Future<String> decode(String encodedText) async {
    var current = encodedText;
    for (var i = _codecs.length - 1; i >= 0; i--) {
      current = await _codecs[i].decode(current);
    }
    return current;
  }
}
