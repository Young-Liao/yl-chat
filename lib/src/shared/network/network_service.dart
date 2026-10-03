import 'dart:async';
import 'dart:developer' as developer;
import 'package:flutter/cupertino.dart';
import 'package:yl_chat/src/rust/api/protocol.dart';

import '../../../main.dart'; // 替换为你实际生成的 rust API 路径

/// 全局事件控制器（广播流）
final StreamController<String> networkEventController = StreamController<String>.broadcast();

/// 供所有组件统一监听的广播流
Stream<String> get globalNetworkStream => networkEventController.stream;

/// 初始化网络事件总线：确保 Rust 的 StreamSink 只被绑定一次
StreamSubscription<String>? _rustSinkSubscription;

void initGlobalNetworkListener() {
  _rustSinkSubscription?.cancel();

  // networkEventStream 是 Rust FRB 导出的原始流
  _rustSinkSubscription = networkEventStream.listen(
        (event) {
      debugPrint('[Global Network Bus] Received from Rust: $event');
      // 转发给全局广播总线
      networkEventController.add(event);
    },
    onError: (error) {
      debugPrint('[Global Network Bus] Error: $error');
    },
    onDone: () {
      debugPrint('[Global Network Bus] Closed.');
    },
  );
}
