import 'dart:async';
import 'dart:io';
import 'package:flutter_rust_bridge/flutter_rust_bridge_for_generated.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:yl_chat/src/rust/api/engine.dart';
import 'package:yl_chat/src/rust/api/models.dart';

import 'package:yl_chat/src/rust/frb_generated.dart';

void logStep(String step) {
  print('[TEST-INFO] ${DateTime.now().toIso8601String()} -> $step');
}

void main() {
  const serverDbPath = './db1.db';
  const clientDbPath = './db2.db';

  // Constant path to the image file used for testing
  const testImagePath = '/Users/young/Pictures/Bridge.jpeg';

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
      './offline_resume_server.db',
      './offline_resume_server.db-shm',
      './offline_resume_server.db-wal',
      './offline_resume_client.db',
      './offline_resume_client.db-shm',
      './offline_resume_client.db-wal',
      './img_db1.db',
      './img_db1.db-shm',
      './img_db1.db-wal',
      './img_db2.db',
      './img_db2.db-shm',
      './img_db2.db-wal',
    ];

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
    final serverEngine = await NetworkEngineHandle.newInstance(selfMac: serverMac, dbPathStr: clientDbPath);
    final clientEngine = await NetworkEngineHandle.newInstance(selfMac: clientMac, dbPathStr: serverDbPath);

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

  test('P2P Image Transfer Flow Test', () async {
    const serverMac = 'd858e9a7-0ae6-4c58-9a8b-bb032b14a137';
    const clientMac = 'a140a591-149a-459e-b2e0-d0e435f92df9';

    const serverPort = 14984;
    const clientPort = 14985;

    // Verify image file exists before running test
    final imageFile = File(testImagePath);
    expect(await imageFile.exists(), isTrue, reason: 'Test image does not exist at $testImagePath');

    logStep('1. Instantiating NetworkEngine instances for image transfer...');
    final serverEngine = await NetworkEngineHandle.newInstance(selfMac: serverMac, dbPathStr: './img_db1.db');
    final clientEngine = await NetworkEngineHandle.newInstance(selfMac: clientMac, dbPathStr: './img_db2.db');

    logStep('2. Starting TCP listeners...');
    final Stream<String> serverNotifyStream = await serverEngine.startListener(port: serverPort);
    final Stream<String> clientNotifyStream = await clientEngine.startListener(port: clientPort);

    // Set up listeners BEFORE sending to avoid race conditions
    final serverFileCompleter = Completer<String>();
    final serverNotifySub = serverNotifyStream.listen((event) {
      logStep('Server image test event: $event');
      // Match FILE_COMPLETE: or NEW_FILE: depending on engine output
      if (event.startsWith('FILE_COMPLETE:') && !serverFileCompleter.isCompleted) {
        serverFileCompleter.complete(event);
      }
    });

    final clientSentCompleter = Completer<String>();
    final clientNotifySub = clientNotifyStream.listen((event) {
      logStep('Client image test event: $event');
      if (event.startsWith('FILE_SENT:') && !clientSentCompleter.isCompleted) {
        clientSentCompleter.complete(event);
      }
    });

    logStep('3. Upserting peer records...');
    await clientEngine.upsertPeer(
      record: PeerRecord(
        macAddress: serverMac,
        lastKnownIp: '127.0.0.1',
        port: serverPort,
        deviceName: 'Server_Node',
        lastSeen: DateTime.now().millisecondsSinceEpoch ~/ 1000,
        isOnline: true,
      ),
    );

    await serverEngine.upsertPeer(
      record: PeerRecord(
        macAddress: clientMac,
        lastKnownIp: '127.0.0.1',
        port: clientPort,
        deviceName: 'Client_Node',
        lastSeen: DateTime.now().millisecondsSinceEpoch ~/ 1000,
        isOnline: true,
      ),
    );

    logStep('4. Sending image file from client to server...');
    final transferId = await clientEngine.sendFile(
      recipientMac: serverMac,
      filePathStr: testImagePath,
    );

    logStep('5. Waiting for server file transfer completion signal...');
    final serverEvent = await serverFileCompleter.future.timeout(const Duration(seconds: 5));
    logStep('Server received file notification: $serverEvent');
    expect(serverEvent, contains(transferId));

    logStep('6. Waiting for client send completion signal...');
    final clientSentEvent = await clientSentCompleter.future.timeout(const Duration(seconds: 5));
    expect(clientSentEvent, equals('FILE_SENT:$transferId'));

    logStep('7. Cleaning up image test resources...');
    unawaited(serverNotifySub.cancel());
    unawaited(clientNotifySub.cancel());

    // Clean up temporary databases created for this test
    for (final dbPath in ['./img_db1.db', './img_db2.db']) {
      final f = File(dbPath);
      if (await f.exists()) {
        try { await f.delete(); } catch (_) {}
      }
    }

    logStep('8. Image transfer test passed successfully!');
  });


  test('P2P File Transfer Offline Pending and Resume Test', () async {
    const serverMac = 'd858e9a7-0ae6-4c58-9a8b-bb032b14a137';
    const clientMac = 'a140a591-149a-459e-b2e0-d0e435f92df9';

    const serverPort = 14990;
    const clientPort = 14991;

    // 校验测试文件
    final imageFile = File(testImagePath);
    expect(await imageFile.exists(), isTrue, reason: 'Test image missing at $testImagePath');

    const serverDbPath = './offline_resume_server.db';
    const clientDbPath = './offline_resume_client.db';

    logStep('1. Instantiating NetworkEngine instances without active network listeners...');
    final serverEngine = await NetworkEngineHandle.newInstance(
      selfMac: serverMac,
      dbPathStr: serverDbPath,
    );
    final clientEngine = await NetworkEngineHandle.newInstance(
      selfMac: clientMac,
      dbPathStr: clientDbPath,
    );

    logStep('2. Injecting offline peer record into client...');
    // 设置 Peer 离线或指向无效/未开启监听的端口，确保无 TCP 连接
    await clientEngine.upsertPeer(
      record: PeerRecord(
        macAddress: serverMac,
        lastKnownIp: '127.0.0.1',
        port: serverPort,
        deviceName: 'Server_Node',
        lastSeen: DateTime.now().millisecondsSinceEpoch ~/ 1000,
        isOnline: false, // 离线状态
      ),
    );

    logStep('3. Initiating sendFile while offline (Outbox queuing)...');
    final transferId = await clientEngine.sendFile(
      recipientMac: serverMac,
      filePathStr: testImagePath,
    );
    logStep('File transfer queued with transferId: $transferId');

    logStep('4. Verifying client local database records message as pending/sending...');
    final clientPendingMsgs = await clientEngine.getMessages(peerMac: serverMac);
    expect(clientPendingMsgs.length, equals(1));
    expect(clientPendingMsgs.first.msgId, equals(transferId));
    // 尚未建立连接并完成 Ack，状态不应为 acked
    expect(clientPendingMsgs.first.status, isNot(equals(MessageStatus.acked)));

    logStep('5. [NETWORK RESTORED] Starting TCP listeners and updating peer online status...');
    final Stream<String> serverNotifyStream = await serverEngine.startListener(port: serverPort);
    final Stream<String> clientNotifyStream = await clientEngine.startListener(port: clientPort);

    final serverFileCompleter = Completer<String>();
    final serverSub = serverNotifyStream.listen((event) {
      logStep('Server received event: $event');
      if (event.startsWith('FILE_COMPLETE:') && !serverFileCompleter.isCompleted) {
        serverFileCompleter.complete(event);
      }
    });

    final clientAckCompleter = Completer<String>();
    final clientSub = clientNotifyStream.listen((event) {
      logStep('Client received event: $event');
      if (event.startsWith('FILE_SENT:') || event.startsWith('ACK:')) {
        if (!clientAckCompleter.isCompleted) {
          clientAckCompleter.complete(event);
        }
      }
    });

    // 恢复 Peer 在线状态
    await clientEngine.upsertPeer(
      record: PeerRecord(
        macAddress: serverMac,
        lastKnownIp: '127.0.0.1',
        port: serverPort,
        deviceName: 'Server_Node',
        lastSeen: DateTime.now().millisecondsSinceEpoch ~/ 1000,
        isOnline: true,
      ),
    );
    await serverEngine.upsertPeer(
      record: PeerRecord(
        macAddress: clientMac,
        lastKnownIp: '127.0.0.1',
        port: clientPort,
        deviceName: 'Client_Node',
        lastSeen: DateTime.now().millisecondsSinceEpoch ~/ 1000,
        isOnline: true,
      ),
    );

    logStep('6. Flushing outbox to trigger resumption and missing chunk retransmission...');
    await clientEngine.flushOutbox(peerMac: serverMac);

    logStep('7. Waiting for completion signals on both ends...');
    final serverEvent = await serverFileCompleter.future.timeout(const Duration(seconds: 5));
    expect(serverEvent, contains(transferId));

    final clientAckEvent = await clientAckCompleter.future.timeout(const Duration(seconds: 5));
    logStep('Client received completion event: $clientAckEvent');

    logStep('8. Verifying local databases after successful resume...');
    final clientMsgsFinal = await clientEngine.getMessages(peerMac: serverMac);
    expect(clientMsgsFinal.first.status, equals(MessageStatus.acked));

    final serverMsgsFinal = await serverEngine.getMessages(peerMac: clientMac);
    expect(serverMsgsFinal.any((m) => m.msgId == transferId), isTrue);

    logStep('9. Cleaning up test resources...');
    unawaited(serverSub.cancel());
    unawaited(clientSub.cancel());

    for (final dbPath in [serverDbPath, clientDbPath]) {
      final f = File(dbPath);
      if (await f.exists()) {
        try { await f.delete(); } catch (_) {}
      }
    }

    logStep('10. Offline Pending & Resume 断点续传测试通过！');
  });
}
