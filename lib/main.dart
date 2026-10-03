import 'package:flutter/material.dart';
import 'package:window_manager/window_manager.dart';
import 'package:yl_chat/src/features/chat_screen/chat_screen.dart';
import 'package:yl_chat/src/features/chat_screen/side_panel/side_panel.dart';
import 'package:yl_chat/src/rust/api/network.dart';
import 'package:yl_chat/src/rust/frb_generated.dart';
import 'package:yl_chat/src/shared/tools/previous_value_notifier.dart';

PreviousValueNotifier<PeerItem?> chosenPeer = PreviousValueNotifier(null);

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

  // Register local mDNS service on startup
  await registerBonjourService();

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
