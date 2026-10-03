import 'dart:async';
import 'dart:io';
import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated.dart';
import 'package:flutter_test/flutter_test.dart';

import 'package:yl_chat/src/rust/frb_generated.dart';
import 'package:yl_chat/src/rust/api/protocol.dart';

void logStep(String step) {
  print('[TEST-INFO] ${DateTime.now().toIso8601String()} ->$step');
}

void main() {
  setUpAll(() async {
    logStep('Initializing RustLib for bridge testing...');
    final dylibPath = Platform.isMacOS
        ? 'rust/target/debug/librust_lib_yl_chat.dylib'
        : 'rust/target/debug/rust_lib_yl_chat.dll';

    await RustLib.init(
      externalLibrary: ExternalLibrary.open(
        Directory.current.uri.resolve(dylibPath).toFilePath(),
      ),
    );
    logStep('RustLib initialized successfully.');
  });

  test('Full P2P Storage, Notification Stream, and Outbox Flow Test', () async {
    const serverMac = 'AA:BB:CC:DD:EE:01';
    const clientMac = 'AA:BB:CC:DD:EE:02';

    const serverPort = 14981;
    const clientPort = 14982;

    logStep('1. Instantiating NetworkEngine instances...');
    final serverEngine = await NetworkEngine.newInstance(selfMac: serverMac);
    final clientEngine = await NetworkEngine.newInstance(selfMac: clientMac);

    logStep('2. Starting TCP listeners (FRB generates Dart Streams from Rust StreamSink)...');
    // FRB converts `notify_sink: StreamSink<String>` in Rust into a `Stream<String>` return in Dart
    final Stream<String> serverNotifyStream = await serverEngine.startListener(port: serverPort);
    final Stream<String> clientNotifyStream = await clientEngine.startListener(port: clientPort);

    logStep('3. Attaching Completers to listen for specific events...');
    final serverMsgCompleter = Completer<String>();
    final serverNotifySub = serverNotifyStream.listen((event) {
      logStep('Server received event: $event');
      if (event.startsWith('NEW_MSG:') && !serverMsgCompleter.isCompleted) {
        serverMsgCompleter.complete(event);
      }
    });

    final clientAckCompleter = Completer<String>();
    final clientNotifySub = clientNotifyStream.listen((event) {
      logStep('Client received event: $event');
      if (event.startsWith('ACK:') && !clientAckCompleter.isCompleted) {
        clientAckCompleter.complete(event);
      }
    });

    logStep('4. Injecting peer discovery records with ports...');
    await clientEngine.upsertPeer(
      record: PeerRecord(
        macAddress: serverMac,
        lastKnownIp: '127.0.0.1',
        port: serverPort,
        deviceName: 'Server_Node',
        lastSeen: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      ),
    );

    await serverEngine.upsertPeer(
      record: PeerRecord(
        macAddress: clientMac,
        lastKnownIp: '127.0.0.1',
        port: clientPort,
        deviceName: 'Client_Node',
        lastSeen: DateTime.now().millisecondsSinceEpoch ~/ 1000,
      ),
    );

    logStep('5. Verifying stored peer records on client...');
    final peers = await clientEngine.getPeers();
    expect(peers.length, equals(1));
    expect(peers.first.macAddress, equals(serverMac));
    expect(peers.first.port, equals(serverPort));

    logStep('6. Sending message from client to server via Outbox...');
    const chatContent = 'Hello from Flutter P2P V2 Architecture!';
    await clientEngine.sendMessage(
      recipientMac: serverMac,
      content: chatContent,
    );

    logStep('7. Checking client local storage for pending/sent message...');
    final clientMessages = await clientEngine.getMessages(peerMac: serverMac);
    expect(clientMessages.length, equals(1));
    expect(clientMessages.first.content, equals(chatContent));
    expect(clientMessages.first.isOutgoing, isTrue);

    logStep('8. Waiting for server notification stream signal (NEW_MSG:...)...');
    final notifyEvent = await serverMsgCompleter.future
        .timeout(const Duration(seconds: 4));
    logStep('Server notified: $notifyEvent');
    expect(notifyEvent, equals('NEW_MSG:$clientMac'));

    logStep('9. Querying server storage for received chat history...');
    final serverMessages = await serverEngine.getMessages(peerMac: clientMac);
    expect(serverMessages.length, equals(1));
    expect(serverMessages.first.content, equals(chatContent));
    expect(serverMessages.first.isOutgoing, isFalse);

    logStep('10. Waiting for client ACK notification stream signal...');
    final ackEvent = await clientAckCompleter.future
        .timeout(const Duration(seconds: 4));
    logStep('Client notified of ACK: $ackEvent');
    expect(ackEvent, equals('ACK:$serverMac'));

    logStep('11. Verifying ACK delivery status in client storage...');
    final updatedClientMessages = await clientEngine.getMessages(peerMac: serverMac);
    expect(updatedClientMessages.first.status, equals(MessageStatus.acked));
    logStep('Message status successfully updated to ACKED on client.');

    logStep('12. Cleaning up stream subscriptions...');
    unawaited(serverNotifySub.cancel());
    unawaited(clientNotifySub.cancel());

    logStep('13. Test completed cleanly!');
  });
}
