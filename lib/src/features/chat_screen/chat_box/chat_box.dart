import 'dart:async';
import 'dart:io';
import 'dart:ui';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:path/path.dart' as p;
import 'package:yl_chat/main.dart';
import 'package:yl_chat/src/features/chat_screen/chat_box/title_bar.dart';
import 'package:yl_chat/src/shared/tools/file_picker_util.dart';
import 'package:yl_chat/src/theme/abstract_theme.dart';

import '../../../rust/api/models.dart';
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

  final List<PickedFileData> _pickedFiles = [];

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

        // 捕获新消息、对端ACK回复或本地发送完成事件，刷新UI
        if (event == 'NEW_MSG:$currentPeerMac' ||
            event == 'ACK:$currentPeerMac' ||
            event == 'MSG_SENT:$currentPeerMac') {
          _refreshMessages(autoScroll: true);
        } else if (event.startsWith('FILE_PROGRESS')) {
          // 文件传输进度更新时刷新列表，保持滚动条稳定
          _refreshMessages(autoScroll: false);
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
      _refreshMessages(autoScroll: true);
    }
  }

  /// 从 Rust LocalStorage 中主动刷新消息历史记录
  Future<void> _refreshMessages({bool autoScroll = false}) async {
    final currentPeer = chosenPeer.value;
    if (currentPeer == null) return;

    try {
      final history = await networkEngine.getMessages(peerMac: currentPeer.macAddress);
      if (mounted) {
        setState(() {
          _messages = history;
        });
        if (autoScroll) {
          _scrollToBottom();
        }
      }
    } catch (e) {
      debugPrint('Failed to fetch messages: $e');
    }
  }

  /// 发送消息逻辑
  Future<void> _handleSendMessage() async {
    final text = _messageController.text;
    final files = [..._pickedFiles];
    _focusNode.requestFocus();
    final currentPeer = chosenPeer.value;

    if (currentPeer == null) return;

    _messageController.clear();
    _pickedFiles.clear();
    if (mounted) {
      setState(() {});
    }

    final recipientMac = currentPeer.macAddress;

    if (files.isNotEmpty) {
      for (final attachment in files) {
        try {
          await networkEngine.sendFile(
            recipientMac: recipientMac,
            filePathStr: attachment.file.path,
          );

          await _refreshMessages(autoScroll: true);
        } catch (e) {
          debugPrint('Failed to send file: $e');
        }
      }
    }

    if (text.isNotEmpty) {
      try {
        debugPrint("Sending message to $recipientMac: $text");

        // 1. 调用 Rust API 追加进 Outbox 并尝试 TCP 直连发送
        await networkEngine.sendMessage(
          recipientMac: recipientMac,
          content: text,
        );

        // 2. 发送完成后立刻重新拉取 Storage，保证 UI 第一时间渲染该消息
        await _refreshMessages(autoScroll: true);
      } catch (e) {
        debugPrint('Failed to send message: $e');
      }
    }
  }

  /// 自动滚动到底部（因为 ListView 设置了 reverse: true，0.0 偏移量即为底部最新消息）
  void _scrollToBottom() {
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (_scrollController.hasClients) {
        _scrollController.animateTo(
          0.0,
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
    _focusNode.dispose();
    super.dispose();
  }

  void _onAttachFiles() async {
    // 选文件接口调用（可按需要解锁）
  }

  void _onAttachImages() async {
    final images = await FilePickerUtil.pickMultipleImages();
    if (images.isNotEmpty && mounted) {
      setState(() {
        _pickedFiles.addAll(images);
      });
    }
  }

  Widget _buildChatInputField({
    required BuildContext context,
    required TextEditingController controller,
    required VoidCallback onSend,
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
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            // 1. 附件列表区域（仅在有文件时渲染）
            if (_pickedFiles.isNotEmpty)
              ConstrainedBox(
                constraints: const BoxConstraints(
                  maxHeight: 180,
                ),
                child: Scrollbar(
                  child: ListView.builder(
                    shrinkWrap: true,
                    itemCount: _pickedFiles.length,
                    itemBuilder: (context, index) {
                      final item = _pickedFiles[index];
                      return AttachmentItem(
                        fileName: item.name,
                        fileSize: item.sizeFormatted,
                        file: item.file,
                        onDelete: () {
                          setState(() {
                            _pickedFiles.removeAt(index);
                          });
                        },
                      );
                    },
                  ),
                ),
              ),

            const SizedBox(height: 10.0),

            // 2. 底部输入与操作按钮区域
            Row(
              children: [
                const SizedBox(width: 10.0),
                IconButton(
                  icon: const Icon(Icons.attach_file),
                  onPressed: _onAttachFiles,
                  constraints: const BoxConstraints(),
                  padding: EdgeInsets.zero,
                  style: ButtonStyle(
                    iconColor: WidgetStateProperty.resolveWith<Color>((states) {
                      if (states.contains(WidgetState.hovered)) {
                        return theme.textMain;
                      }
                      return theme.textMuted;
                    }),
                  ),
                ),
                const SizedBox(width: 10.0),
                IconButton(
                  icon: const Icon(Icons.image),
                  onPressed: _onAttachImages,
                  constraints: const BoxConstraints(),
                  padding: EdgeInsets.zero,
                  style: ButtonStyle(
                    iconColor: WidgetStateProperty.resolveWith<Color>((states) {
                      if (states.contains(WidgetState.hovered)) {
                        return theme.textMain;
                      }
                      return theme.textMuted;
                    }),
                  ),
                ),
                const SizedBox(width: 12),
                Expanded(
                  child: Focus(
                    onKeyEvent: (FocusNode node, KeyEvent event) {
                      if (event is KeyDownEvent &&
                          (event.logicalKey == LogicalKeyboardKey.enter ||
                              event.logicalKey == LogicalKeyboardKey.numpadEnter) &&
                          !HardwareKeyboard.instance.isShiftPressed) {
                        _handleSendMessage();
                        return KeyEventResult.handled;
                      }
                      return KeyEventResult.ignored;
                    },
                    child: TextField(
                      focusNode: _focusNode,
                      controller: controller,
                      style: TextStyle(color: theme.textMain, fontSize: 14),
                      maxLines: null,
                      decoration: InputDecoration(
                        hintText: 'Type a message or drop files here...',
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
                const SizedBox(width: 10.0),
              ],
            ),
            const SizedBox(height: 10.0),
          ],
        ),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final theme = context.appTheme;

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
                  reverse: true,
                  controller: _scrollController,
                  padding: const EdgeInsets.all(16.0),
                  itemCount: _messages.length,
                  itemBuilder: (context, index) {
                    final reversedIndex = _messages.length - 1 - index;
                    final message = _messages[reversedIndex];

                    return message.toBubble(
                      myInitials: "ME",
                      peerInitials: "PC",
                    );
                  },
                ),
              ),
            ),
            _buildChatInputField(
              context: context,
              controller: _messageController,
              onSend: _handleSendMessage,
            ),
          ],
        );
      },
    );
  }
}

class AttachmentItem extends StatelessWidget {
  final String fileName;
  final String fileSize;
  final File? file;
  final VoidCallback? onDelete;

  const AttachmentItem({
    super.key,
    required this.fileName,
    required this.fileSize,
    this.file,
    this.onDelete,
  });

  bool get _isImage {
    final ext = fileName.split('.').last.toLowerCase();
    return ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp', 'heic'].contains(ext);
  }

  Widget _buildThumbnail() {
    final typeConfig = FilePickerUtil.getFileTypeConfig(fileName);

    if (_isImage && file != null && file!.existsSync()) {
      return Image.file(
        file!,
        fit: BoxFit.cover,
        errorBuilder: (context, error, stackTrace) {
          return FilePickerUtil.buildFileIcon(typeConfig);
        },
      );
    }

    return FilePickerUtil.buildFileIcon(typeConfig);
  }

  @override
  Widget build(BuildContext context) {
    final theme = context.appTheme;

    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
      decoration: BoxDecoration(
        border: Border(
          bottom: BorderSide(
            color: theme.border,
          ),
        ),
      ),
      child: Row(
        children: [
          const SizedBox(width: 10),
          ClipRRect(
            borderRadius: BorderRadius.circular(8),
            child: SizedBox(
              width: 48,
              height: 48,
              child: _buildThumbnail(),
            ),
          ),
          const SizedBox(width: 12),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              mainAxisSize: MainAxisSize.min,
              children: [
                Text(
                  fileName,
                  style: TextStyle(
                    color: theme.textMain,
                    fontWeight: FontWeight.bold,
                    fontSize: 14,
                  ),
                  maxLines: 1,
                  overflow: TextOverflow.ellipsis,
                ),
                const SizedBox(height: 2),
                Text(
                  fileSize,
                  style: TextStyle(
                    color: theme.textMuted,
                    fontSize: 11,
                  ),
                  maxLines: 1,
                  overflow: TextOverflow.ellipsis,
                ),
              ],
            ),
          ),
          IconButton(
            icon: const Icon(Icons.close, color: Colors.grey, size: 18),
            onPressed: onDelete,
            padding: EdgeInsets.zero,
            constraints: const BoxConstraints(),
            splashRadius: 18,
          ),
          const SizedBox(width: 10),
        ],
      ),
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

  final bool isTransferring;
  final String? fileName;
  final double progress;
  final String? currentSizeText;
  final String? totalSizeText;
  final String? speedText;
  final VoidCallback? onCancelTransfer;

  const ChatMessageBubble({
    super.key,
    required this.senderInitials,
    this.text,
    this.imageUrl,
    required this.timestamp,
    this.isMe = false,
    this.isAcked = false,
    this.isTransferring = false,
    this.fileName,
    this.progress = 0.0,
    this.currentSizeText,
    this.totalSizeText,
    this.speedText,
    this.onCancelTransfer,
  });

  Widget _buildImageTransferCard(AbstractTheme theme) {
    final percentageInt = (progress.clamp(0.0, 1.0) * 100).toInt();

    String sizeInfo = '$percentageInt%';
    if (currentSizeText != null && totalSizeText != null) {
      sizeInfo += ' • $currentSizeText / $totalSizeText';
    }

    final progressOverlay = Column(
      mainAxisAlignment: MainAxisAlignment.center,
      children: [
        Stack(
          alignment: Alignment.center,
          children: [
            SizedBox(
              width: 54,
              height: 54,
              child: CircularProgressIndicator(
                value: progress.clamp(0.0, 1.0),
                strokeWidth: 4,
                backgroundColor: const Color(0xFF334155),
                valueColor: const AlwaysStoppedAnimation<Color>(Color(0xFF3B82F6)),
              ),
            ),
            IconButton(
              icon: const Icon(Icons.close, color: Colors.white, size: 20),
              onPressed: onCancelTransfer,
              padding: EdgeInsets.zero,
              constraints: const BoxConstraints(),
            ),
          ],
        ),
        const SizedBox(height: 16),
        Text(
          fileName ?? 'image.png',
          maxLines: 1,
          overflow: TextOverflow.ellipsis,
          style: const TextStyle(
            color: Colors.white,
            fontSize: 15,
            fontWeight: FontWeight.bold,
          ),
        ),
        const SizedBox(height: 4),
        Text(
          sizeInfo,
          style: const TextStyle(
            color: Color(0xFF94A3B8),
            fontSize: 12,
          ),
        ),
        if (speedText != null) ...[
          const SizedBox(height: 4),
          Text(
            speedText!,
            style: const TextStyle(
              color: Color(0xFF3B82F6),
              fontSize: 12,
              fontWeight: FontWeight.w500,
            ),
          ),
        ],
      ],
    );

    return Container(
      width: 260,
      height: 200,
      clipBehavior: Clip.antiAlias,
      decoration: BoxDecoration(
        color: const Color(0xFF1E293B),
        borderRadius: BorderRadius.circular(16),
        border: Border.all(
          color: theme.border,
          width: 1,
        ),
      ),
      child: Stack(
        children: [
          if (isMe && imageUrl != null && imageUrl!.isNotEmpty) ...[
            Positioned.fill(
              child: imageUrl!.startsWith('http')
                  ? Image.network(imageUrl!, fit: BoxFit.cover)
                  : Image.file(File(imageUrl!), fit: BoxFit.cover),
            ),
            Positioned.fill(
              child: BackdropFilter(
                filter: ImageFilter.blur(sigmaX: 8, sigmaY: 8),
                child: Container(
                  color: Colors.black.withValues(alpha: 0.55),
                ),
              ),
            ),
          ],
          Center(
            child: Padding(
              padding: const EdgeInsets.all(16.0),
              child: progressOverlay,
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildImageWidget(String path) {
    final bool isNetwork = path.startsWith('http://') || path.startsWith('https://');

    return ClipRRect(
      borderRadius: BorderRadius.circular(12),
      child: ConstrainedBox(
        constraints: const BoxConstraints(
          maxWidth: 240,
          maxHeight: 300,
        ),
        child: isNetwork
            ? Image.network(
          path,
          fit: BoxFit.cover,
          errorBuilder: (context, error, stackTrace) =>
          const Icon(Icons.broken_image, color: Colors.grey),
        )
            : Image.file(
          File(path),
          fit: BoxFit.cover,
          errorBuilder: (context, error, stackTrace) =>
          const Icon(Icons.broken_image, color: Colors.grey),
        ),
      ),
    );
  }

  Widget _buildTextBubble(Color bubbleColor, Color textColor) {
    return Container(
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
        text ?? '',
        style: TextStyle(
          color: textColor,
          fontSize: 14,
          height: 1.4,
        ),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final theme = context.appTheme;

    final bubbleColor = isMe ? const Color(0xFF2563EB) : const Color(0xFF263354);
    final textColor = Colors.white;

    Widget contentWidget;
    if (isTransferring) {
      contentWidget = _buildImageTransferCard(theme);
    } else if (imageUrl != null && imageUrl!.isNotEmpty) {
      contentWidget = _buildImageWidget(imageUrl!);
    } else {
      contentWidget = _buildTextBubble(bubbleColor, textColor);
    }

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
                contentWidget,
                const SizedBox(height: 4),
                Row(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    Text(
                      timestamp,
                      style: const TextStyle(
                        color: Color(0xFF94A3B8),
                        fontSize: 11,
                      ),
                    ),
                    if (isMe) ...[
                      const SizedBox(width: 4),
                      Icon(
                        isAcked ? Icons.done_all : Icons.access_time,
                        size: 13,
                        color: isAcked ? Colors.white : const Color(0xFF94A3B8),
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

extension PersistentMessageX on PersistentMessage {
  bool get isImageMessage => fileType == FileType.image;

  /// 计算传输进度 (0.0 ~ 1.0)
  double get transferProgress {
    if (totalChunks == 0) return 0.0;
    return (sentChunks / totalChunks).clamp(0.0, 1.0);
  }

  /// 判断当前消息是否处于传输/未完成状态
  bool get isTransferring {
    if (totalChunks == 0) return status == MessageStatus.pending;
    return status == MessageStatus.pending || sentChunks < totalChunks;
  }

  ChatMessageBubble toBubble({
    required String myInitials,
    required String peerInitials,
    BigInt? transferredBytes,
    String? speedText,
  }) {
    final DateTime dt = DateTime.fromMillisecondsSinceEpoch(timestamp.toInt());
    final String timeStr =
        "${dt.hour.toString().padLeft(2, '0')}:${dt.minute.toString().padLeft(2, '0')}";

    String? currentSizeStr;
    String? totalSizeStr;

    final progressValue = transferProgress;

    if (fileSize != null && fileSize! > BigInt.zero) {
      final int totalInt = fileSize!.toInt();
      totalSizeStr = FilePickerUtil.formatBytes(totalInt);

      if (transferredBytes != null) {
        currentSizeStr = FilePickerUtil.formatBytes(transferredBytes.toInt());
      } else {
        final currentInt = (totalInt * progressValue).toInt();
        currentSizeStr = FilePickerUtil.formatBytes(currentInt);
      }
    }

    return ChatMessageBubble(
      key: ValueKey(msgId),
      senderInitials: isOutgoing ? myInitials : peerInitials,
      text: isImageMessage ? null : content,
      imageUrl: filePath,
      timestamp: timeStr,
      isMe: isOutgoing,
      isAcked: status == MessageStatus.acked,
      isTransferring: isImageMessage && isTransferring,
      fileName: fileName ?? (filePath != null ? p.basename(filePath!) : 'image.png'),
      progress: progressValue,
      currentSizeText: currentSizeStr,
      totalSizeText: totalSizeStr,
      speedText: speedText,
    );
  }
}
