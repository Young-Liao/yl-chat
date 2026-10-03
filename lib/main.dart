import 'package:flutter/material.dart';
import 'package:path_provider/path_provider.dart';
import 'package:window_manager/window_manager.dart';
import 'package:yl_chat/src/features/chat_screen/chat_screen.dart';
import 'package:yl_chat/src/features/chat_screen/side_panel/side_panel.dart';
import 'package:yl_chat/src/rust/api/discovery.dart';
import 'package:yl_chat/src/rust/api/protocol.dart';
import 'package:yl_chat/src/rust/frb_generated.dart';
import 'package:yl_chat/src/shared/device/device_id_manager.dart';
import 'package:yl_chat/src/shared/network/network_service.dart';
import 'package:yl_chat/src/shared/tools/previous_value_notifier.dart';

PreviousValueNotifier<PeerItem?> chosenPeer = PreviousValueNotifier(null);

const servicePort = 14981;
late final NetworkEngine networkEngine;
late final Stream<String> networkEventStream;

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await RustLib.init();

  // Initialize window manager
  await windowManager.ensureInitialized();

  WindowOptions windowOptions = const WindowOptions(
    size: Size(1200, 800),          // Initial window size (width, height)
    center: true,                  // Center the window on screen
    title: 'YL Chat',
  );

  windowManager.waitUntilReadyToShow(windowOptions, () async {
    await windowManager.show();
    await windowManager.focus();
  });

  final docDir = await getApplicationDocumentsDirectory();
  final dbPath = "${docDir.path}/app_chat.db";

  // Register local mDNS service on startup
  final macAddress = await DeviceIdManager.getOrCreateDeviceId();
  await registerBonjourService(macAddress: macAddress);
  networkEngine = await NetworkEngine.newInstance(selfMac: macAddress, dbPathStr: dbPath);
  networkEventStream = networkEngine.startListener(port: servicePort);
  initGlobalNetworkListener();

  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'yl_chat P2P',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: Colors.deepPurple),
        useMaterial3: true,
      ),
      home: const ChatScreen(),
    );
  }
}
