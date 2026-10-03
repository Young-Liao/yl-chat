import 'dart:async';
import 'package:flutter/material.dart';
import 'package:yl_chat/main.dart';
import 'package:yl_chat/src/features/chat_screen/chat_box/title_bar.dart';
import 'package:yl_chat/src/rust/api/protocol.dart';
import 'package:yl_chat/src/shared/device/device_id_manager.dart';
import 'package:yl_chat/src/shared/tools/algorithms.dart';
import 'package:yl_chat/src/theme/abstract_theme.dart';

class ChatBox extends StatefulWidget {
  const ChatBox({super.key});

  @override
  State<ChatBox> createState() => _ChatBoxState();
}

class _ChatBoxState extends State<ChatBox> {
  StreamSubscription<String>? _notifySubscription;

  List<PersistentMessage> _messages = [];
  final TextEditingController _messageController = TextEditingController();
  final ScrollController _scrollController = ScrollController();

  @override
  void initState() {
    super.initState();
    _initEngineAndListener();
    chosenPeer.addListener(_onPeerChanged);
  }

  /// Initialize NetworkEngine, start TCP listener, and listen to notification stream
  Future<void> _initEngineAndListener() async {
    _notifySubscription = networkEventStream.listen((event) {
      debugPrint('[NetworkEngine Event] $event');

      if (chosenPeer.value == null) return;
      final currentPeerMac = chosenPeer.value!.ip; // Using chosen peer identifier

      if (event == 'NEW_MSG:$currentPeerMac' || event == 'ACK:$currentPeerMac') {
        _refreshMessages();
      }
    });
  }

  /// Triggered whenever user switches selected contact in side panel
  void _onPeerChanged() {
    if (chosenPeer.value != null) {
      _refreshMessages();
    } else {
      setState(() {
        _messages = [];
      });
    }
  }

  /// Fetch chat history from Rust local storage
  Future<void> _refreshMessages() async {
    if (chosenPeer.value == null) return;
    final peerMac = chosenPeer.value!.ip;

    final history = await networkEngine.getMessages(peerMac: peerMac);
    setState(() {
      _messages = history;
    });

    _scrollToBottom();
  }

  /// Send message via NetworkEngine outbox
  Future<void> _handleSendMessage() async {
    final text = _messageController.text.trim();
    if (text.isEmpty || chosenPeer.value == null) return;

    final recipientMac = chosenPeer.value!.ip;
    _messageController.clear();

    try {
      // 1. Append to outbox and attempt TCP delivery in Rust
      await networkEngine.sendMessage(
        recipientMac: recipientMac,
        content: text,
      );

      // 2. Immediately update UI state from local storage
      await _refreshMessages();
    } catch (e) {
      debugPrint('Failed to send message: $e');
    }
  }

  void _scrollToBottom() {
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (_scrollController.hasClients) {
        _scrollController.animateTo(
          _scrollController.position.maxScrollExtent,
          duration: const Duration(milliseconds: 250),
          curve: Curves.easeOut,
        );
      }
    });
  }

  @override
  void dispose() {
    chosenPeer.removeListener(_onPeerChanged);
    _notifySubscription?.cancel();
    _messageController.dispose();
    _scrollController.dispose();
    super.dispose();
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
        border: Border(top: BorderSide(color: theme.border)),
        borderRadius: const BorderRadius.only(bottomRight: Radius.circular(15.0)),
      ),
      child: Container(
        decoration: BoxDecoration(
          color: theme.bgDark,
          borderRadius: const BorderRadius.all(Radius.circular(15.0)),
          border: Border.all(color: theme.border),
        ),
        padding: const EdgeInsets.symmetric(vertical: 10.0, horizontal: 10.0),
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
                onSubmitted: (_) => onSend(),
                decoration: const InputDecoration(
                  hintText: 'Type a message...',
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

  @override
  Widget build(BuildContext context) {
    final theme = context.appTheme;
    final currentPeer = chosenPeer.value;

    return Column(
      crossAxisAlignment: CrossAxisAlignment.stretch,
      children: [
        TitleBar(
          name: currentPeer?.name ?? "Select a Contact",
          ip: currentPeer?.ip ?? "",
          status: currentPeer != null ? "Online" : "",
        ),
        Expanded(
          child: Container(
            color: theme.bgChat,
            child: _messages.isEmpty
                ? Center(
              child: Text(
                currentPeer == null
                    ? "Select a host from side panel to chat"
                    : "No messages yet. Say hello!",
                style: TextStyle(color: theme.textMuted),
              ),
            )
                : ListView.builder(
              controller: _scrollController,
              padding: const EdgeInsets.all(16.0),
              itemCount: _messages.length,
              itemBuilder: (context, index) {
                final msg = _messages[index];
                return ChatMessageBubble(
                  key: ValueKey(msg.msgId),
                  senderInitials: msg.isOutgoing ? 'ME' : 'PC',
                  text: msg.content,
                  timestamp: formatTimestamp(msg.timestamp),
                  isMe: msg.isOutgoing,
                  isAcked: msg.status == MessageStatus.acked,
                );
              },
            ),
          ),
        ),
        _buildChatInputField(
          context: context,
          controller: _messageController,
          onSend: _handleSendMessage,
          onAttach: () {},
        ),
      ],
    );
  }
}

class ChatMessageBubble extends StatelessWidget {
  final String senderInitials;
  final String? text;
  final String? imageUrl;
  final String timestamp;
  final bool isMe;
  final bool isAcked;

  const ChatMessageBubble({
    super.key,
    required this.senderInitials,
    this.text,
    this.imageUrl,
    required this.timestamp,
    this.isMe = false,
    this.isAcked = false,
  });

  @override
  Widget build(BuildContext context) {
    final theme = context.appTheme;

    final bubbleColor = isMe ? theme.bgBubbleOutgoing : theme.bgBubbleIncoming;
    final textColor = theme.textMain;

    final avatar = CircleAvatar(
      radius: 18,
      backgroundColor: const Color(0xFF334155),
      child: Text(
        senderInitials,
        style: const TextStyle(
          color: Colors.white,
          fontSize: 12,
          fontWeight: FontWeight.bold,
        ),
      ),
    );

    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 8.0, horizontal: 12.0),
      child: Row(
        mainAxisAlignment: isMe ? MainAxisAlignment.end : MainAxisAlignment.start,
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          if (!isMe) ...[
            avatar,
            const SizedBox(width: 8),
          ],
          Flexible(
            child: Column(
              crossAxisAlignment:
              isMe ? CrossAxisAlignment.end : CrossAxisAlignment.start,
              children: [
                if (text != null) ...[
                  Container(
                    constraints: const BoxConstraints(maxWidth: 320),
                    padding: const EdgeInsets.symmetric(
                      horizontal: 16.0,
                      vertical: 12.0,
                    ),
                    decoration: BoxDecoration(
                      color: bubbleColor,
                      borderRadius: BorderRadius.only(
                        bottomLeft: const Radius.circular(16),
                        bottomRight: const Radius.circular(16),
                        topLeft: Radius.circular(isMe ? 16 : 4),
                        topRight: Radius.circular(isMe ? 4 : 16),
                      ),
                    ),
                    child: Text(
                      text!,
                      style: TextStyle(
                        color: textColor,
                        fontSize: 14,
                        height: 1.4,
                      ),
                    ),
                  ),
                ],
                const SizedBox(height: 4),
                Row(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    Text(
                      timestamp,
                      style: TextStyle(
                        color: theme.textMuted,
                        fontSize: 11,
                      ),
                    ),
                    if (isMe) ...[
                      const SizedBox(width: 4),
                      Icon(
                        isAcked ? Icons.done_all : Icons.access_time,
                        size: 13,
                        color: isAcked ? theme.textMain : theme.textMuted,
                      ),
                    ],
                  ],
                ),
              ],
            ),
          ),
          if (isMe) ...[
            const SizedBox(width: 8),
            avatar,
          ],
        ],
      ),
    );
  }
}
