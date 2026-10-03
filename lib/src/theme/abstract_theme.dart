import 'dart:ui';

import 'package:flutter/cupertino.dart';
import 'package:yl_chat/src/theme/dark_theme.dart';

abstract class AbstractTheme {
  // Backgrounds & Panels
  Color get bgDark;
  Color get bgPanel;
  Color get bgChat;

  // Bubbles
  Color get bgBubbleIncoming;
  Color get bgBubbleOutgoing;

  // Text
  Color get textMain;
  Color get textMuted;

  // Accents & Actions
  Color get accent;
  Color get accentHover;
  Color get border;

  // User Status Indicators
  Color get statusOnline;
  Color get statusBusy;
  Color get statusAway;

  // Scrollbars & Input Elements
  Color get scrollbarThumb;
  Color get scrollbarThumbHover;

  // Traffic Lights (macOS Window Controls)
  Color get trafficClose;
  Color get trafficMinimize;
  Color get trafficMaximize;

  factory AbstractTheme.of(BuildContext context) {
    return DarkTheme();
  }
}

extension AppThemeExtension on BuildContext {
  AbstractTheme get appTheme => .of(this);
}

