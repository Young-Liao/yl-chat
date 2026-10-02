import 'package:flutter/material.dart';
import 'package:yl_chat/src/rust/api/network.dart';
import 'package:yl_chat/src/rust/api/simple.dart';
import 'package:yl_chat/src/rust/frb_generated.dart';

Future<void> main() async
{
  WidgetsFlutterBinding.ensureInitialized();
  await RustLib.init();

  // 1. Register local service on startup
  await registerBonjourService();

  runApp(const MyApp());
}

class MyApp extends StatefulWidget
{
  const MyApp({super.key});

  @override
  State<MyApp> createState() => _MyAppState();
}

class _MyAppState extends State<MyApp>
{
  List<String> _peers = [];
  bool _isScanning = false;

  Future<void> _refreshPeers() async
  {
    setState(() => _isScanning = true);

    // Give background mDNS advertisement a moment to settle
    await Future.delayed(const Duration(seconds: 1));
    final result = await scanLanPeers();

    setState(()
    {
      _peers = result;
      _isScanning = false;
    }
    );
  }

  @override
  void initState() 
  {
    super.initState();
    _refreshPeers();
  }

  @override
  Widget build(BuildContext context) 
  {
    return MaterialApp(
      home: Scaffold(
        appBar: AppBar(
          title: const Text('mDNS Peer Discovery'),
          actions: [
            IconButton(
              icon: const Icon(Icons.refresh),
              onPressed: _isScanning ? null : _refreshPeers
            )
          ]
        ),
        body: Center(
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              Text('Greet: ${greet(name: "Tom")}'),
              const SizedBox(height: 20),
              _isScanning
                ? const CircularProgressIndicator()
                : Text('Discovered Peers:\n${_peers.join("\n")}'),
              TextButton(onPressed: _refreshPeers,
                child: const Text("Refresh Peers"))
            ]
          )
        )
      )
    );
  }
}
