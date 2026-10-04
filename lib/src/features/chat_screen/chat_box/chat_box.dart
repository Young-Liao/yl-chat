import 'dart:async';
import 'dart:developer' as developer;
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:yl_chat/main.dart';
import 'package:yl_chat/src/features/chat_screen/chat_box/title_bar.dart';
import 'package:yl_chat/src/rust/api/protocol.dart';
import 'package:yl_chat/src/shared/tools/algorithms.dart';
import 'package:yl_chat/src/theme/abstract_theme.dart';

import '../../../shared/network/network_service.dart';

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

  final FocusNode _focusNode = FocusNode();

  @override
  void initState() {
    super.initState();
    _initEngineAndListener();
    chosenPeer.addListener(_onPeerChanged);

    // 如果初始化时已经选择了联系人，主动拉取一次历史记录
    if (chosenPeer.value != null) {
      _refreshMessages();
    }
  }

  /// 订阅 Rust 底层网络事件通知
  void _initEngineAndListener() {
    _notifySubscription?.cancel();
    _notifySubscription = globalNetworkStream.listen(
          (event) {
        debugPrint('[NetworkEngine Event] $event');

        final currentPeer = chosenPeer.value;
        if (currentPeer == null) return;
        final currentPeerMac = currentPeer.macAddress;

        // 捕获新消息、对端ACK回复、或本地发送完成事件，强行刷新UI
        if (event == 'NEW_MSG:$currentPeerMac' ||
            event == 'ACK:$currentPeerMac' ||
            event == 'MSG_SENT:$currentPeerMac' ||
            event == 'PEER_LIST_UPDATED') {
          _refreshMessages();
        }
      },
      onError: (error) {
        debugPrint('[NetworkEngine Event Error] $error');
      },
    );
  }

  /// 切换联系人时的回调
  void _onPeerChanged() {
    if (!mounted) return;

    // 切换联系人时先清空列表，防止 UI 闪烁显示上一个人的聊天记录
    setState(() {
      _messages = [];
    });

    if (chosenPeer.value != null) {
      _refreshMessages();
    }
  }

  /// 从 Rust LocalStorage 中主动刷新消息历史记录
  Future<void> _refreshMessages() async {
    final currentPeer = chosenPeer.value;
    if (currentPeer == null) return;

    try {
      final history = await networkEngine.getMessages(peerMac: currentPeer.macAddress);
      if (mounted) {
        setState(() {
          _messages = history;
        });
        _scrollToBottom();
      }
    } catch (e) {
      debugPrint('Failed to fetch messages: $e');
    }
  }

  /// 发送消息逻辑
  Future<void> _handleSendMessage() async {
    final text = _messageController.text;
    _focusNode.requestFocus();
    final currentPeer = chosenPeer.value;

    if (text.isEmpty || currentPeer == null) return;

    final recipientMac = currentPeer.macAddress;
    _messageController.clear();

    try {
      debugPrint("Sending message to $recipientMac: $text");

      // 1. 调用 Rust API 追加进 Outbox 并尝试 TCP 直连发送
      await networkEngine.sendMessage(
        recipientMac: recipientMac,
        content: text,
      );

      // 2. 发送完成后立刻重新拉取 Storage，保证 UI 第一时间渲染该消息
      await _refreshMessages();
    } catch (e) {
      debugPrint('Failed to send message: $e');
    }
  }

  /// 自动滚动到底部
  void _scrollToBottom() {
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (_scrollController.hasClients) {
        _scrollController.animateTo(
          _scrollController.position.maxScrollExtent,
          duration: const Duration(milliseconds: 200),
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
              child: Focus(
                onKeyEvent: (FocusNode node, KeyEvent event) {
                  // 1. 监听 Enter 键按下（排除 Shift + Enter 换行）
                  if (event is KeyDownEvent &&
                      (event.logicalKey == LogicalKeyboardKey.enter ||
                       event.logicalKey == LogicalKeyboardKey.numpadEnter) &&
                      !HardwareKeyboard.instance.isShiftPressed) {

                    // 2. 执行你的发送逻辑
                    _handleSendMessage();

                    // 3. 核心：返回 handled，告诉系统“这个 Enter 已经被我处理了，不要再传给 TextField 了”
                    return KeyEventResult.handled;
                  }

                  // 其他按键（如 Shift + Enter 或普通字符）正常放行
                  return KeyEventResult.ignored;
                },
                child: TextField(
                  focusNode: _focusNode,
                  controller: controller,
                  style: TextStyle(color: theme.textMain, fontSize: 14),
                  maxLines: null,
                  decoration: InputDecoration(
                    hintText: 'Type a message...',
                    hintStyle: TextStyle(color: theme.textMuted, fontSize: 14),
                    border: InputBorder.none,
                    isDense: true,
                    contentPadding: EdgeInsets.zero,
                  ),
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

    // 使用 ValueListenableBuilder 实时响应 chosenPeer 的变更
    return ValueListenableBuilder(
      valueListenable: chosenPeer,
      builder: (context, currentPeer, child) {
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
                child: currentPeer == null
                    ? Center(
                  child: Text(
                    "Select a host from side panel to chat",
                    style: TextStyle(color: theme.textMuted),
                  ),
                )
                    : _messages.isEmpty
                    ? Center(
                  child: Text(
                    "No messages yet. Say hello!",
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
      },
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
