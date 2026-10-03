import 'package:flutter/material.dart';
import 'package:yl_chat/main.dart';
import 'package:yl_chat/src/features/chat_screen/chat_box/title_bar.dart';
import 'package:yl_chat/src/rust/api/protocol.dart';
import 'package:yl_chat/src/shared/tools/algorithms.dart';
import 'package:yl_chat/src/theme/abstract_theme.dart';

import '../side_panel/side_panel.dart';

class ChatBox extends StatefulWidget
{
  const ChatBox({super.key});

  @override
  State<StatefulWidget> createState() => _ChatBoxState();
}

class _ChatBoxState extends State<ChatBox>
{
  late final PeerManager peerManager;
  late Stream<String> connectStream;
  late Stream<MessageEnvelope> currentMsgStream;
  List<ChatMessageBubble> messages = [];

  late final TextEditingController _messageController;

  bool fromListener = false;

  @override
  void initState() {
    super.initState();
    _initPeerManager();

    _messageController = TextEditingController();
  }
 
  void _onPeerChanged() async {
    if (chosenPeer.previousValue != null) {
      peerManager.disconnectPeer(peerId: chosenPeer.previousValue!.name);
    }
    if (chosenPeer.value != null) {
      messages.clear();
      if (fromListener) {
        fromListener = false;
        return;
      }
      try {
        final peerId = await peerManager.connectPeer(
            peerIp: chosenPeer.value!.ip,
            senderId: await getSenderId(),
            port: await getServicePort()
        );
        if (peerId != chosenPeer.value!.name) {
          throw Exception("The peerId isn't equal to the peerName. Fatal.");
        }

        currentMsgStream =
            peerManager.addReceptionHandlerFor(peerId: chosenPeer.value!.name);
        currentMsgStream.listen((envelope) {
          switch (envelope.payload) {
            case MessagePayload_Handshake(:final clientVersion, :final publicKey, :final peerId):
              debugPrint('Handshake from $peerId (v$clientVersion)');

            case MessagePayload_ChatMessage(:final content):
              debugPrint('Chat message: $content');
              messages.add( ChatMessageBubble(
                  senderInitials: 'PC',
                  text: content,
                  timestamp: formatTimestamp(envelope.timestamp),
                  isMe: false
                ),
              );

            case MessagePayload_Ack(:final targetMsgId, :final status):
              debugPrint('ACK for $targetMsgId: $status');

            case MessagePayload_Ping():
              debugPrint('Received Ping');

            case MessagePayload_Pong():
              debugPrint('Received Pong');
          }
        });
      } catch (e) {
        debugPrint("Failed to connect to peer ${chosenPeer.value!.name} :$e");
        chosenPeer.value = null;
      }
    }
  }

  void _initPeerManager() async {
    peerManager = await PeerManager.default_();
    connectStream = peerManager.createPeerListener(
      senderId: await getSenderId(),
    );
    connectStream.listen((id) {
      fromListener = true;
      contactListKey.currentState?.selectByName(id);
    });

    chosenPeer.addListener(_onPeerChanged);
  }
  
  @override
  void dispose() {
    super.dispose();
    
    chosenPeer.removeListener(_onPeerChanged);
  }

  Widget _buildChatInputField({
    required BuildContext context,
    required TextEditingController controller,
    required VoidCallback onSend,
    required VoidCallback onAttach,
  }) {
    final theme = context.appTheme;

    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 25, vertical: 15),
      decoration: BoxDecoration(
        color: theme.bgPanel,
        border: Border(
          top: BorderSide(
            color: theme.border
          )
        ),
        borderRadius: BorderRadius.only(
          bottomRight: Radius.circular(15.0)
        )
      ),
      child: Container(
        decoration: BoxDecoration(
          color: theme.bgDark,
          borderRadius: BorderRadius.all(Radius.circular(15.0)),
          border: Border.all(
            color: theme.border
          ),
        ),
        padding: EdgeInsets.symmetric(vertical: 10.0, horizontal: 10.0),
        child: Row(
          children: [
            IconButton(
              icon: const Icon(Icons.attach_file, color: Color(0xFF8E9BAE)),
              onPressed: onAttach,
              constraints: const BoxConstraints(),
              padding: EdgeInsets.zero,
            ),
            const SizedBox(width: 12),
            Expanded(
              child: TextField(
                controller: controller,
                style: const TextStyle(color: Colors.white, fontSize: 14),
                decoration: const InputDecoration(
                  hintText: 'Type a message or drop files here...',
                  hintStyle: TextStyle(color: Color(0xFF64748B), fontSize: 14),
                  border: InputBorder.none,
                  isDense: true,
                  contentPadding: EdgeInsets.zero,
                ),
              ),
            ),
            const SizedBox(width: 12),
            ElevatedButton.icon(
              onPressed: onSend,
              icon: const Text(
                'Send',
                style: TextStyle(
                  color: Colors.white,
                  fontWeight: FontWeight.bold,
                  fontSize: 14,
                ),
              ),
              label: const Icon(
                Icons.send_outlined,
                color: Colors.white,
                size: 16,
              ),
              style: ElevatedButton.styleFrom(
                backgroundColor: const Color(0xFF3B82F6),
                elevation: 0,
                padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
                shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(8),
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }

  void _handleSendMessage() {
    // 4. Read text using .text.trim()
    final text = _messageController.text.trim();
    if (text.isEmpty) return;

    // Call your PeerManager / Rust API here
    // clientManager.sendChatMessage(peerId: targetPeerId, content: text);
    if (chosenPeer.value != null) {
      peerManager.sendChatMessage(peerId: chosenPeer.value!.name, content: text);
    }

    // 5. Clear the text input after sending
    _messageController.clear();
  }

  @override
  Widget build(BuildContext context)
  {
    final theme = context.appTheme;
    return Column(
      crossAxisAlignment: CrossAxisAlignment.stretch,
      children: [
        TitleBar(
          name: chosenPeer.value?.name ?? "",
          ip: chosenPeer.value?.ip ?? "",
          status: chosenPeer.value != null ? "Online" : "",
        ),
        Expanded(
          child: Container(
            decoration: BoxDecoration(
              color: theme.bgChat
            ),
            child: ListView(
              padding: const EdgeInsets.all(16.0),
              children: messages,
            )
          )
        ),
        _buildChatInputField(context: context, controller: _messageController,
            onSend: _handleSendMessage,
            onAttach: () {})
      ]
    );
  }
}

class ChatMessageBubble extends StatelessWidget
{
  final String senderInitials;
  final String? text;
  final String? imageUrl;
  final String timestamp;
  final bool isMe;

  const ChatMessageBubble({
    super.key,
    required this.senderInitials,
    this.text,
    this.imageUrl,
    required this.timestamp,
    this.isMe = false
  });

  @override
  Widget build(BuildContext context)
  {
    final theme = context.appTheme;

    // 气泡背景颜色
    final bubbleColor = isMe ? theme.bgBubbleOutgoing : theme.bgBubbleIncoming;
    final textColor = theme.textMain;

    // 头像 Widget
    final avatar = CircleAvatar(
      radius: 18,
      backgroundColor: const Color(0xFF334155),
      child: Text(
        senderInitials,
        style: const TextStyle(
          color: Colors.white,
          fontSize: 12,
          fontWeight: FontWeight.bold
        )
      )
    );

    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 8.0, horizontal: 12.0),
      child: Row(
        mainAxisAlignment: isMe ? MainAxisAlignment.end : MainAxisAlignment.start,
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          // 如果是对端发来的消息，头像在左侧
          if (!isMe) ...[
            avatar,
            const SizedBox(width: 8)
          ],

          // 气泡和时间戳主主体
          Flexible(
            child: Column(
              crossAxisAlignment:
              isMe ? CrossAxisAlignment.end : CrossAxisAlignment.start,
              children: [
                // 图片展示
                if (imageUrl != null) ...[
                  Container(
                    constraints: const BoxConstraints(maxWidth: 320),
                    child: ClipRRect(
                      borderRadius: BorderRadius.circular(12),
                      child: Image.network(
                        imageUrl!,
                        fit: BoxFit.cover,
                        width: double.infinity
                      )
                    ),
                  )
                ],

                // 普通文本消息
                if (text != null) ...[
                  // 消息气泡容器
                  Container(
                    constraints: const BoxConstraints(maxWidth: 320),
                    padding: imageUrl != null
                      ? const EdgeInsets.all(8.0)
                      : const EdgeInsets.symmetric(horizontal: 16.0, vertical: 12.0),
                    decoration: BoxDecoration(
                      color: bubbleColor,
                      borderRadius: BorderRadius.only(
                        bottomLeft: const Radius.circular(16),
                        bottomRight: const Radius.circular(16),
                        topLeft: Radius.circular(isMe ? 16 : 4),
                        topRight: Radius.circular(isMe ? 4 : 16)
                      )
                    ),
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        Text(
                          text!,
                          style: TextStyle(
                            color: textColor,
                            fontSize: 14,
                            height: 1.4
                          )
                        )
                      ]
                    )
                  )],

                const SizedBox(height: 4),

                // 时间戳
                Text(
                  timestamp,
                  style: TextStyle(
                    color: theme.textMuted,
                    fontSize: 11
                  )
                )
              ]
            )
          ),

          // 如果是自己发出的消息，头像在右侧
          if (isMe) ...[
            const SizedBox(width: 8),
            avatar
          ]
        ]
      )
    );
  }
}
