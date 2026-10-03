import 'dart:async';
import 'package:flutter/material.dart';

class PeerUpdater extends ChangeNotifier {
  Timer? _timer;

  void startTimer() {
    // Prevent starting multiple timers if already running
    _timer?.cancel();

    // Fires every 1 minute
    _timer = Timer.periodic(const Duration(minutes: 1), (timer) {
      _onTick();
    });
  }

  void _onTick() {
    notifyListeners();
  }

  void stopTimer() {
    _timer?.cancel();
    _timer = null;
  }

  @override
  void dispose() {
    stopTimer();
    super.dispose();
  }
}
