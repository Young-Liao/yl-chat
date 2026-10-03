import 'dart:ui';

import 'package:yl_chat/src/theme/abstract_theme.dart';

/// Dark theme implementation based on the UI design specs.
class DarkTheme implements AbstractTheme {
  const DarkTheme();

  @override
  Color get bgDark => const Color(0xFF121824);

  @override
  Color get bgPanel => const Color(0xFF1E2640);

  @override
  Color get bgChat => const Color(0xFF182035);

  @override
  Color get bgBubbleIncoming => const Color(0xFF263354);

  @override
  Color get bgBubbleOutgoing => const Color(0xFF2563EB);

  @override
  Color get textMain => const Color(0xFFF1F5F9);

  @override
  Color get textMuted => const Color(0xFF94A3B8);

  @override
  Color get accent => const Color(0xFF3B82F6);

  @override
  Color get accentHover => const Color(0xFF1D4ED8);

  @override
  Color get border => const Color(0xFF2E3A59);

  @override
  Color get statusOnline => const Color(0xFF10B981);

  @override
  Color get statusBusy => const Color(0xFFEF4444);

  @override
  Color get statusAway => const Color(0xFFF59E0B);

  @override
  Color get scrollbarThumb => const Color(0xFF334155);

  @override
  Color get scrollbarThumbHover => const Color(0xFF475569);

  @override
  Color get trafficClose => const Color(0xFFFF5F56);

  @override
  Color get trafficMinimize => const Color(0xFFFFBD2E);

  @override
  Color get trafficMaximize => const Color(0xFF27C93F);
}
