import 'dart:async';
import 'dart:io';
import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated.dart';
import 'package:flutter_test/flutter_test.dart';

import 'package:yl_chat/src/rust/frb_generated.dart';
import 'package:yl_chat/src/rust/api/protocol.dart';

void logStep(String step) {
  print('[TEST-INFO] ${DateTime.now().toIso8601String()} -> $step');
}

void main() {
  const serverDbPath = './db1.db';
  const clientDbPath = './db2.db';

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


    final filesToClean = [
      serverDbPath,
      '$serverDbPath-shm',
      '$serverDbPath-wal',
      clientDbPath,
      '$clientDbPath-shm',
      '$clientDbPath-wal',
    ];

    // 2. 循环检查并强行删除
    for (final path in filesToClean) {
      final file = File(path);
      if (await file.exists()) {
        try {
          await file.delete();
          print('🧹 成功清理旧测试文件: $path');
        } catch (e) {
          print('⚠️ 删除文件失败 $path: $e (可能是由于之前的测试进程未完全释放锁)');
        }
      }
    }
  });

  test('Full P2P Storage, Notification Stream, and Outbox Flow Test', () async {
    const serverMac = 'd858e9a7-0ae6-4c58-9a8b-bb032b14a137';
    const clientMac = 'a140a591-149a-459e-b2e0-d0e435f92df9';

    const serverPort = 14982;
    const clientPort = 14983;

    logStep('1. Instantiating NetworkEngine instances...');
    final serverEngine = await NetworkEngine.newInstance(selfMac: serverMac, dbPathStr: clientDbPath);
    final clientEngine = await NetworkEngine.newInstance(selfMac: clientMac, dbPathStr: serverDbPath);

    logStep('2. Starting TCP listeners...');
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
    int clientAckCount = 0;
    final clientNotifySub = clientNotifyStream.listen((event) {
      logStep('Client received event: $event');
      if (event.startsWith('ACK:')) {
        clientAckCount++;
        if (!clientAckCompleter.isCompleted) {
          clientAckCompleter.complete(event);
        }
      }
    });

    logStep('4. Injecting peer discovery records with ports...');
    await clientEngine.upsertPeer(
      record: PeerRecord(
        macAddress: serverMac,
        lastKnownIp: '127.0.0.1',
        port: serverPort,
        deviceName: 'Server_Node',
        lastSeen: DateTime.now().millisecondsSinceEpoch ~/ 1000, isOnline: true,
      ),
    );

    await serverEngine.upsertPeer(
      record: PeerRecord(
        macAddress: clientMac,
        lastKnownIp: '127.0.0.1',
        port: clientPort,
        deviceName: 'Client_Node',
        lastSeen: DateTime.now().millisecondsSinceEpoch ~/ 1000, isOnline: true,
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

    logStep('7. Checking client local storage for initial pending message...');
    final clientMessagesInitial = await clientEngine.getMessages(peerMac: serverMac);
    expect(clientMessagesInitial.length, equals(1));
    expect(clientMessagesInitial.first.content, equals(chatContent));
    expect(clientMessagesInitial.first.isOutgoing, isTrue);

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

    // EXPLICIT BUG CATCHER #1: Ensure ACK is attributed to the server, not receiver's self MAC
    expect(
      ackEvent,
      equals('ACK:$serverMac'),
      reason: 'ACK event MAC must match target peer conversation ID ($serverMac), not internal receiver MAC',
    );

    logStep('11. Verifying ACK delivery status in client storage...');
    final updatedClientMessages = await clientEngine.getMessages(peerMac: serverMac);
    expect(updatedClientMessages.length, equals(1));

    // EXPLICIT BUG CATCHER #2: Ensure storage state actually transitions to Acked
    expect(
      updatedClientMessages.first.status,
      equals(MessageStatus.acked),
      reason: 'Message status failed to transition to MessageStatus.acked due to MAC lookup mismatch',
    );
    logStep('Message status successfully updated to ACKED on client.');

    logStep('12. Testing Outbox Re-transmission Prevention...');
    // EXPLICIT BUG CATCHER #3: Simulate mDNS background flush cycle
    await clientEngine.flushOutbox(peerMac: serverMac);

    // Allow brief async event processing window
    await Future.delayed(const Duration(milliseconds: 200));

    final postFlushMessages = await clientEngine.getMessages(peerMac: serverMac);
    expect(postFlushMessages.length, equals(1));
    expect(postFlushMessages.first.status, equals(MessageStatus.acked));
    expect(clientAckCount, equals(1), reason: 'Flushing an acked outbox should not trigger duplicate re-transmissions or extra ACKs');

    logStep('13. Cleaning up stream subscriptions...');
    unawaited(serverNotifySub.cancel());
    unawaited(clientNotifySub.cancel());

    logStep('14. Test completed cleanly!');
  });
}
