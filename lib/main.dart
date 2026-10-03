import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:yl_chat/src/rust/api/network.dart';
import 'package:yl_chat/src/rust/api/protocol.dart';
import 'package:yl_chat/src/rust/api/simple.dart';
import 'package:yl_chat/src/rust/frb_generated.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await RustLib.init();

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
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: Colors.deepPurple),
        useMaterial3: true,
      ),
      home: const ChatScreen(),
    );
  }
}

class ChatScreen extends StatefulWidget {
  const ChatScreen({super.key});

  @override
  State<ChatScreen> createState() => _ChatScreenState();
}

class _ChatScreenState extends State<ChatScreen> {
  List<String> _peers = [];
  bool _isScanning = false;

  PeerSession? _activeSession;
  final List<String> _messages = [];
  final TextEditingController _textController = TextEditingController();

  @override
  void initState() {
    super.initState();
    _scanForPeers();
  }

  /// 1. Discovery: Scan LAN for mDNS peers
  Future<void> _scanForPeers() async {
    setState(() => _isScanning = true);
    try {
      final peers = await scanLanPeers();
      peers.add("192.168.1.80");
      setState(() {
        _peers = peers;
      });
    } catch (e) {
      _showSnackBar("Scan failed: $e");
    } finally {
      setState(() => _isScanning = false);
    }
  }

  /// 2. Session: Connect TCP socket via PeerSession OO wrapper
  Future<void> _connectToPeer(String ip) async {
    try {
      _showSnackBar("Connecting to $ip...");

      final session = await PeerSession.connect(
        peerId: "peer_$ip",
        peerIp: ip,
        port: 14981,
      );

      setState(() {
        _activeSession = session;
        _messages.add("System: Connected to $ip");
      });

      // Start listening for incoming frames on a background task
      _listenForIncomingMessages();
    } catch (e) {
      _showSnackBar("Connection failed: $e");
    }
  }

  /// 3. Read loop: Poll or stream incoming MessageEnvelopes
  Future<void> _listenForIncomingMessages() async {
    while (_activeSession != null) {
      try {
        final envelope = await _activeSession!.readEnvelope();

        setState(() {
          // Format based on payload
          final sender = envelope.senderName;
          final content = envelope.payload.toString();
          _messages.add("$sender: $content");
        });
      } catch (e) {
        if (_activeSession != null) {
          setState(() {
            _messages.add("System: Connection lost ($e)");
            _activeSession = null;
          });
        }
        break;
      }
    }
  }

  /// 4. Send Message: Frame and send string payload
  Future<void> _sendMessage() async {
    final text = _textController.text.trim();
    if (text.isEmpty || _activeSession == null) return;

    _textController.clear();

    try {
      await _activeSession!.sendChatMessage(
        senderId: "my_device",
        senderName: "Me",
        content: text,
      );

      setState(() {
        _messages.add("Me: $text");
      });
    } catch (e) {
      _showSnackBar("Failed to send: $e");
    }
  }

  void _showSnackBar(String text) {
    ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(text)));
  }

  @override
  void dispose() {
    _activeSession?.close();
    _textController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('yl_chat P2P'),
        actions: [
          IconButton(
            icon: const Icon(Icons.refresh),
            onPressed: _isScanning ? null : _scanForPeers,
          ),
        ],
      ),
      body: Column(
        children: [
          // Banner displaying peer discovery
          Container(
            padding: const EdgeInsets.all(8.0),
            color: Colors.grey.shade200,
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Row(
                  children: [
                    Text(
                      "Discovered Peers (${_peers.length}):",
                      style: const TextStyle(fontWeight: FontWeight.bold),
                    ),
                    if (_isScanning) ...[
                      const SizedBox(width: 8),
                      const SizedBox(
                        width: 12,
                        height: 12,
                        child: CircularProgressIndicator(strokeWidth: 2),
                      ),
                    ],
                  ],
                ),
                const SizedBox(height: 4),
                _peers.isEmpty
                    ? const Text("No peers found yet.")
                    : Wrap(
                  spacing: 8.0,
                  children: _peers.map((ip) {
                    return ActionChip(
                      avatar: const Icon(Icons.dns, size: 16),
                      label: Text(ip),
                      onPressed: () => _connectToPeer(ip),
                    );
                  }).toList(),
                ),
              ],
            ),
          ),

          // Chat messages list
          Expanded(
            child: ListView.builder(
              padding: const EdgeInsets.all(12.0),
              itemCount: _messages.length,
              itemBuilder: (context, index) {
                return Padding(
                  padding: const EdgeInsets.symmetric(vertical: 4.0),
                  child: Text(_messages[index]),
                );
              },
            ),
          ),

          // Message input bar
          if (_activeSession != null)
            SafeArea(
              child: Padding(
                padding: const EdgeInsets.all(8.0),
                child: Row(
                  children: [
                    Expanded(
                      child: TextField(
                        controller: _textController,
                        decoration: const InputDecoration(
                          hintText: 'Type a message...',
                          border: OutlineInputBorder(),
                        ),
                        onSubmitted: (_) => _sendMessage(),
                      ),
                    ),
                    const SizedBox(width: 8),
                    IconButton(
                      icon: const Icon(Icons.send),
                      onPressed: _sendMessage,
                    ),
                  ],
                ),
              ),
            ),
        ],
      ),
    );
  }
}
