import 'dart:async';
import 'dart:io';
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
    final files = [..._pickedFiles];
    _focusNode.requestFocus();
    final currentPeer = chosenPeer.value;

    if (currentPeer == null) return;

    _messageController.clear();
    _pickedFiles.clear();
    final recipientMac = currentPeer.macAddress;

    if (files.isNotEmpty) {

      for (final attachment in files) {
        try {
          await networkEngine.sendFile(
              recipientMac: recipientMac,
              filePathStr: attachment.file.path
          );

          await _refreshMessages();
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
        await _refreshMessages();
      } catch (e) {
        debugPrint('Failed to send message: $e');
      }
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

  void _onAttachFiles() {
    // final files = FilePickerUtil.pickMultipleFiles();
  }


  void _onAttachImages() async {
    final images = await FilePickerUtil.pickMultipleImages();
    _pickedFiles.addAll(images);
    if (mounted) {
      setState(() { });
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
          mainAxisSize: MainAxisSize.min, // 适应内容高度
          children: [
            // 1. 附件列表区域（仅在有文件时渲染）
            if (_pickedFiles.isNotEmpty)
              ConstrainedBox(
                constraints: const BoxConstraints(
                  maxHeight: 180, // 限制最大高度，约可容纳 2-3 项，多出部分自动可滚动
                ),
                child: Scrollbar( // 加上滚动条支持
                  child: ListView.builder(
                    shrinkWrap: true, // 内容少时不占用多余高度
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
                    return _messages[index].toBubble(
                        myInitials: "ME",
                        peerInitials: "PC"
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

class ChatMessageBubble extends StatelessWidget {
  final String senderInitials;
  final String? text;
  final String? imageUrl;
  final String timestamp;
  final bool isMe;
  final bool isAcked;

  // 新增文件传输进度相关参数
  final bool isTransferring;
  final String? fileName;
  final double progress; // 0.0 ~ 1.0 (例如 0.45 代表 45%)
  final String? currentSizeText; // 例如 "2.1 MB"
  final String? totalSizeText; // 例如 "4.8 MB"

  const ChatMessageBubble({
    super.key,
    required this.senderInitials,
    this.text,
    this.imageUrl,
    required this.timestamp,
    this.isMe = false,
    this.isAcked = false,
    // 新增参数默认值
    this.isTransferring = false,
    this.fileName,
    this.progress = 0.0,
    this.currentSizeText,
    this.totalSizeText,
  });

  /// 构建传输进度卡片 Widget (根据图片完美还原 UI)
  Widget _buildTransferProgressCard(AbstractTheme theme) {
    final percentageInt = (progress.clamp(0.0, 1.0) * 100).toInt();

    return Container(
      width: 280,
      padding: const EdgeInsets.all(12.0),
      decoration: BoxDecoration(
        color: theme.bgDark, // 深色卡片背景
        borderRadius: BorderRadius.circular(12),
        border: Border.all(
          color: theme.border, // 边框颜色
          width: 1,
        ),
      ),
      child: Column(
        mainAxisSize: MainAxisSize.min,
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              // 左侧蓝底图标
              FilePickerUtil.buildFileIcon(FilePickerUtil.getFileTypeConfig(fileName ?? "")),
              const SizedBox(width: 10),
              // 右侧文件名与大小信息
              Expanded(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    // 文件名
                    Text(
                      fileName ?? 'File',
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                      style: const TextStyle(
                        color: Colors.white,
                        fontSize: 14,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                    const SizedBox(height: 4),
                    // 45% 与 2.1 MB / 4.8 MB 左右分布
                    Row(
                      mainAxisAlignment: MainAxisAlignment.spaceBetween,
                      children: [
                        Text(
                          '$percentageInt%',
                          style: const TextStyle(
                            color: Color(0xFF94A3B8),
                            fontSize: 12,
                          ),
                        ),
                        if (currentSizeText != null && totalSizeText != null)
                          Text(
                            '$currentSizeText / $totalSizeText',
                            style: const TextStyle(
                              color: Color(0xFF94A3B8),
                              fontSize: 12,
                            ),
                          ),
                      ],
                    ),
                  ],
                ),
              ),
            ],
          ),
          const SizedBox(height: 10),
          // 蓝色进度条
          ClipRRect(
            borderRadius: BorderRadius.circular(3),
            child: LinearProgressIndicator(
              value: progress.clamp(0.0, 1.0),
              minHeight: 6,
              backgroundColor: const Color(0xFF0F172A), // 进度条未完成背景
              valueColor: const AlwaysStoppedAnimation<Color>(
                Color(0xFF3B82F6), // 蓝色进度
              ),
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

    // 假设 context.appTheme 的逻辑
    final bubbleColor = isMe ? const Color(0xFF2563EB) : const Color(0xFF263354);
    final textColor = Colors.white;

    // 内容逻辑控制
    Widget contentWidget;
    if (isTransferring) {
      // 显示传输进度卡片
      contentWidget = _buildTransferProgressCard(theme);
    } else if (imageUrl != null && imageUrl!.isNotEmpty) {
      // 显示完工图片
      contentWidget = _buildImageWidget(imageUrl!);
    } else {
      // 显示普通文本
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

  // 判断是否为图片类型
  bool get _isImage {
    final ext = fileName.split('.').last.toLowerCase();
    return ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp', 'heic'].contains(ext);
  }

  // 根据文件后缀获取对应的图标与背景色

  // 渲染左侧预览图或文件图标
  Widget _buildThumbnail() {
    final typeConfig = FilePickerUtil.getFileTypeConfig(fileName);

    // 如果是图片，且本地文件可用，尝试加载图片
    if (_isImage && file != null && file!.existsSync()) {
      return Image.file(
        file!,
        fit: BoxFit.cover,
        errorBuilder: (context, error, stackTrace) {
          return FilePickerUtil.buildFileIcon(typeConfig);
        },
      );
    }

    // 非图片或文件不存在时，渲染对应文件类型的图标
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
          // Rounded Thumbnail Container
          ClipRRect(
            borderRadius: BorderRadius.circular(8),
            child: SizedBox(
              width: 48,
              height: 48,
              child: _buildThumbnail(),
            ),
          ),
          const SizedBox(width: 12),

          // File Information
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

          // Remove Button
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

extension PersistentMessageX on PersistentMessage {
  bool get isImageMessage => fileType == FileType.image;

  /// 判断当前消息是否处于传输/发送中
  bool get isTransferring => status == MessageStatus.pending;

  ChatMessageBubble toBubble({
    required String myInitials,
    required String peerInitials,
    double transferProgress = 0.0, // 支持外部传入实时传输进度 (0.0 ~ 1.0)
    BigInt? transferredBytes,       // 支持外部传入已传输字节数
  }) {
    final DateTime dt = DateTime.fromMillisecondsSinceEpoch(timestamp.toInt());
    final String timeStr =
        "${dt.hour.toString().padLeft(2, '0')}:${dt.minute.toString().padLeft(2, '0')}";

    // 计算字节文本 (例如: "2.1 MB / 4.8 MB")
    String? currentSizeStr;
    String? totalSizeStr;

    if (fileSize != null && fileSize! > BigInt.zero) {
      final int totalInt = fileSize!.toInt();
      totalSizeStr = FilePickerUtil.formatBytes(totalInt);

      if (transferredBytes != null) {
        currentSizeStr = FilePickerUtil.formatBytes(transferredBytes.toInt());
      } else {
        // 若未传入已传输字节，则根据 progress 比例换算
        final currentInt = (totalInt * transferProgress.clamp(0.0, 1.0)).toInt();
        currentSizeStr = FilePickerUtil.formatBytes(currentInt);
      }
    }

    return ChatMessageBubble(
      key: ValueKey(msgId),
      senderInitials: isOutgoing ? myInitials : peerInitials,
      text: isImageMessage ? null : content,
      // 未传输完成时不用直接显示图片，由进度卡片展示
      imageUrl: (isImageMessage && !isTransferring) ? filePath : null,
      timestamp: timeStr,
      isMe: isOutgoing,
      isAcked: status == MessageStatus.acked,
      // 传输卡片所需参数
      isTransferring: isTransferring && isImageMessage,
      fileName: fileName ?? (filePath != null ? p.basename(filePath!) : 'image.png'),
      progress: transferProgress,
      currentSizeText: currentSizeStr,
      totalSizeText: totalSizeStr,
    );
  }
}
