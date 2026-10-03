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
  setUpAll(() async {
    logStep('Initializing RustLib...');
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

  test('Full P2P Server-Client Handshake and Messaging Flow', () async {
    const serverId = 'server_node_1';
    const clientId = 'client_node_1';

    logStep('1. Creating PeerManagers...');
    final serverManager = await PeerManager.default_();
    final clientManager = await PeerManager.default_();

    logStep('2. Starting server listener...');
    final Stream<String> serverConnectedStream = await serverManager.createPeerListener(
      senderId: serverId,
    );

    final serverConnectedCompleter = Completer<String>();
    final serverConnectSub = serverConnectedStream.listen((id) {
      logStep('Event received on serverConnectedStream: $id');
      if (!serverConnectedCompleter.isCompleted) {
        serverConnectedCompleter.complete(id);
      }
    });

    await Future.delayed(const Duration(milliseconds: 100));

    logStep('3. Connecting client to peer (127.0.0.1:14981)...');
    final remoteServerPeerId = await clientManager.connectPeer(
      peerIp: '127.0.0.1',
      senderId: clientId,
      port: 14981,
    );
    logStep('Client connected. Returned remoteServerPeerId: $remoteServerPeerId');
    expect(remoteServerPeerId, equals(serverId));

    logStep('4. Waiting for server connected completer...');
    final connectedClientId = await serverConnectedCompleter.future
        .timeout(const Duration(seconds: 3));
    logStep('Server confirmed connected client ID: $connectedClientId');
    expect(connectedClientId, equals(clientId));

    logStep('5. Adding reception handlers for server and client...');
    final Stream<MessageEnvelope> serverMsgStream =
    await serverManager.addReceptionHandlerFor(peerId: clientId);
    final Stream<MessageEnvelope> clientMsgStream =
    await clientManager.addReceptionHandlerFor(peerId: serverId);

    final serverMsgCompleter = Completer<MessageEnvelope>();
    final serverMsgSub = serverMsgStream.listen((env) {
      logStep('Server received envelope: msg_id=${env.msgId}, payload=${env.payload.runtimeType}');
      if (!serverMsgCompleter.isCompleted) {
        serverMsgCompleter.complete(env);
      }
    });

    final clientAckCompleter = Completer<MessageEnvelope>();
    final clientAckSub = clientMsgStream.listen((env) {
      logStep('Client received envelope: msg_id=${env.msgId}, payload=${env.payload.runtimeType}');
      if (!clientAckCompleter.isCompleted) {
        clientAckCompleter.complete(env);
      }
    });

    logStep('6. Sending chat message from client to server...');
    final sentMsgId = await clientManager.sendChatMessage(
      peerId: serverId,
      content: 'Hello from Flutter integration test!',
    );
    logStep('Chat message sent with id: $sentMsgId');

    logStep('7. Awaiting server message completer...');
    final serverEnvelope = await serverMsgCompleter.future
        .timeout(const Duration(seconds: 3));
    logStep('Server completer finished.');
    expect(serverEnvelope.senderId, equals(clientId));

    logStep('8. Awaiting client ACK completer...');
    final clientAckEnvelope = await clientAckCompleter.future
        .timeout(const Duration(seconds: 3));
    logStep('Client ACK completer finished.');
    expect(clientAckEnvelope.senderId, equals(serverId));

    logStep('9. Disconnecting peers first...');
    await clientManager.disconnectPeer(peerId: serverId);
    logStep('Client disconnected from server.');
    await serverManager.disconnectPeer(peerId: clientId);
    logStep('Server disconnected from client.');

    logStep('10. Cancelling Dart stream subscriptions...');
    unawaited(serverConnectSub.cancel());
    logStep('serverConnectSub cancelled.');
    unawaited(serverMsgSub.cancel());
    logStep('serverMsgSub cancelled.');
    unawaited(clientAckSub.cancel());
    logStep('clientAckSub cancelled.');

    logStep('11. Test execution reached the very end!');
  });
}
