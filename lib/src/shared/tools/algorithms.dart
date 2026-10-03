import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated.dart';
import 'package:intl/intl.dart';

String formatTimestamp(PlatformInt64 timestamp) {
  // 1. Convert PlatformInt64 to Dart int
  final int rawSeconds = timestamp.toInt();

  // 2. Create DateTime object (convert seconds to milliseconds if your Rust timestamp is in seconds)
  final dateTime = DateTime.fromMillisecondsSinceEpoch(rawSeconds * 1000).toLocal();

  // 3. Format to 12-hour format with AM/PM
  return DateFormat('h:mm a').format(dateTime); // e.g. "10:10 AM"
}
