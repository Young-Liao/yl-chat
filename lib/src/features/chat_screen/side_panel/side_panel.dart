import 'dart:async';
import 'dart:developer' as developer;
import 'package:flutter/material.dart';
import 'package:yl_chat/main.dart';
import 'package:yl_chat/src/rust/api/protocol.dart';
import 'package:yl_chat/src/theme/abstract_theme.dart';

import '../../../shared/network/network_service.dart';

class PeerItem {
  final String initials;
  final String name;
  final String macAddress; // Added hardware ID key
  final String ip;
  final String lastMessage;
  final Color statusColor;

  PeerItem({
    required this.initials,
    required this.name,
    required this.macAddress,
    required this.ip,
    required this.lastMessage,
    required this.statusColor,
  });
}

class SidePanel extends StatefulWidget {
  final AbstractTheme theme;

  const SidePanel({
    super.key,
    required this.theme,
  });

  @override
  State<StatefulWidget> createState() => _SidePanelState();
}

class _SidePanelState extends State<SidePanel> {
  StreamSubscription<String>? _notifySubscription;
  Timer? _scanTimer;
  List<PeerItem> peerItems = [];

  @override
  void initState() {
    super.initState();
    // 1. 初始加载：主动拉取一次本地存储中的 Peer 列表
    _reloadPeersFromStorage();

    // 2. 监听 Rust 事件
    _subscribeToEngineEvents();

    // 3. 定期触发 mDNS 扫描并刷新 Outbox
    _scanTimer = Timer.periodic(const Duration(seconds: 10), (_) {
      _triggerScanCycle();
    });
  }

  void _subscribeToEngineEvents() {
    _notifySubscription?.cancel();
    _notifySubscription = globalNetworkStream.listen(
          (event) {
        debugPrint("Dart UI Received Network Event: $event");
        if (event == 'PEER_LIST_UPDATED' || event.startsWith('ACK:')) {
          _reloadPeersFromStorage();
        }
      },
      onError: (error) {
        debugPrint("Network event stream error: $error");
      },
      onDone: () {
        debugPrint("Network event stream completed.");
      },
    );
  }

  /// 触发扫描并更新 UI
  Future<void> _triggerScanCycle() async {
    try {
      await networkEngine.runScanAndFlushCycle();
      // 强制触发一次 UI 刷新，防止事件通道丢失导致界面卡住
      await _reloadPeersFromStorage();
    } catch (e) {
      debugPrint("Error running scan cycle: $e");
    }
  }

  /// Fetches cached peer records directly from NetworkEngine storage
  Future<void> _reloadPeersFromStorage() async {
    try {
      final records = await networkEngine.getPeers();
      final updatedItems = records.map((record) {
        return PeerItem(
          initials: record.deviceName.isNotEmpty
              ? record.deviceName.substring(0, 1).toUpperCase()
              : "PC",
          name: record.deviceName,
          macAddress: record.macAddress,
          ip: record.lastKnownIp,
          lastMessage: "Tap to view conversation...",
          statusColor: record.isOnline ? widget.theme.statusOnline : widget.theme.statusAway,
        );
      }).toList();

      if (mounted) {
        setState(() {
          peerItems = updatedItems;
        });
      }
    } catch (e) {
      debugPrint("Error reading stored peers: $e");
    }
  }

  @override
  void dispose() {
    _scanTimer?.cancel();
    _notifySubscription?.cancel();
    super.dispose();
  }

  Widget _buildSearchBar(BuildContext context) {
    final theme = context.appTheme;

    return Container(
      margin: const EdgeInsets.symmetric(horizontal: 20.0),
      child: TextField(
        style: TextStyle(
          color: theme.textMain,
          fontSize: 13,
        ),
        cursorColor: theme.accent,
        decoration: InputDecoration(
          hintText: 'Search hosts or IP addresses...',
          hintStyle: TextStyle(
            color: theme.textMuted,
            fontSize: 13,
            fontWeight: FontWeight.w400,
          ),
          filled: true,
          fillColor: theme.bgDark,
          contentPadding: const EdgeInsets.symmetric(
            horizontal: 14,
            vertical: 15,
          ),
          isDense: true,
          enabledBorder: OutlineInputBorder(
            borderRadius: BorderRadius.circular(8),
            borderSide: BorderSide(
              color: theme.border,
              width: 1,
            ),
          ),
          focusedBorder: OutlineInputBorder(
            borderRadius: BorderRadius.circular(8),
            borderSide: BorderSide(
              color: theme.accent,
              width: 1,
            ),
          ),
        ),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final theme = context.appTheme;
    return SizedBox(
      width: 320.0,
      child: Container(
        decoration: BoxDecoration(
            color: theme.bgPanel,
            borderRadius: const BorderRadius.only(
              topLeft: Radius.circular(15.0),
              bottomLeft: Radius.circular(15.0),
            ),
            border: Border(
                right: BorderSide(color: theme.border)
            )
        ),
        child: Column(
          children: [
            Container(
              padding: const EdgeInsets.symmetric(vertical: 20, horizontal: 25),
              child: Row(
                children: [
                  CircleAvatar(
                    radius: 20,
                    backgroundColor: theme.bgPanel,
                    child: Icon(
                      Icons.chat,
                      color: theme.textMain,
                    ),
                  ),
                  const SizedBox(width: 10),
                  Text(
                    "YL Chat",
                    style: TextStyle(
                      fontSize: 20,
                      fontFamily: "monospace",
                      fontWeight: FontWeight.bold,
                      color: theme.textMain,
                    ),
                  )
                ],
              ),
            ),
            _buildSearchBar(context),
            Divider(
              color: theme.border,
            ),
            Expanded(
              child: ContactListWidget(
                key: contactListKey,
                contacts: peerItems,
              ),
            )
          ],
        ),
      ),
    );
  }
}

class ContactCard extends StatefulWidget {
  final String initials;
  final String name;
  final String ipAddress;
  final String lastMessage;
  final Color statusColor;
  final bool isSelected;
  final VoidCallback onTap;

  const ContactCard({
    super.key,
    required this.initials,
    required this.name,
    required this.ipAddress,
    required this.lastMessage,
    required this.statusColor,
    this.isSelected = false,
    required this.onTap,
  });

  @override
  State<ContactCard> createState() => _ContactCardState();
}

class _ContactCardState extends State<ContactCard> {
  bool _isHovered = false;

  @override
  Widget build(BuildContext context) {
    final theme = context.appTheme;

    Color getBackgroundColor() {
      if (widget.isSelected) {
        return theme.bgBubbleIncoming;
      }
      if (_isHovered) {
        return theme.bgBubbleIncoming.withOpacity(0.5);
      }
      return Colors.transparent;
    }

    return MouseRegion(
      cursor: SystemMouseCursors.click,
      onEnter: (_) => setState(() => _isHovered = true),
      onExit: (_) => setState(() => _isHovered = false),
      child: GestureDetector(
        onTap: widget.onTap,
        child: Container(
          padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 12),
          decoration: BoxDecoration(
            color: getBackgroundColor(),
            borderRadius: BorderRadius.circular(10),
            border: widget.isSelected
                ? Border.all(color: theme.border.withOpacity(0.6), width: 1)
                : Border.all(color: Colors.transparent, width: 1),
          ),
          child: Row(
            crossAxisAlignment: CrossAxisAlignment.center,
            children: [
              Stack(
                children: [
                  CircleAvatar(
                    radius: 22,
                    backgroundColor: const Color(0xFF2E3A59),
                    child: Text(
                      widget.initials,
                      style: TextStyle(
                        color: theme.textMain,
                        fontSize: 13,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                  ),
                  Positioned(
                    bottom: 1,
                    right: 1,
                    child: Container(
                      width: 10,
                      height: 10,
                      decoration: BoxDecoration(
                        color: widget.statusColor,
                        shape: BoxShape.circle,
                        border: Border.all(
                          color: widget.isSelected
                              ? theme.bgBubbleIncoming
                              : theme.bgPanel,
                          width: 2,
                        ),
                      ),
                    ),
                  ),
                ],
              ),
              const SizedBox(width: 12),
              Expanded(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    Text(
                      widget.name,
                      style: TextStyle(
                        color: theme.textMain,
                        fontSize: 14,
                        fontWeight: FontWeight.bold,
                      ),
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                    ),
                    const SizedBox(height: 2),
                    Text(
                      widget.ipAddress,
                      style: TextStyle(
                        color: theme.textMuted.withOpacity(0.8),
                        fontSize: 11,
                        fontFamily: 'monospace',
                      ),
                    ),
                    const SizedBox(height: 2),
                    Text(
                      widget.lastMessage,
                      style: TextStyle(
                        color: theme.textMuted,
                        fontSize: 12,
                      ),
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                    ),
                  ],
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }
}

class ContactListWidget extends StatefulWidget {
  final List<PeerItem> contacts;

  const ContactListWidget({super.key, required this.contacts});

  @override
  State<ContactListWidget> createState() => ContactListWidgetState();
}

class ContactListWidgetState extends State<ContactListWidget> {
  int selectedIndex = 0;

  bool selectByName(String name) {
    final index = widget.contacts.indexWhere((item) => item.name == name);
    if (index != -1) {
      chosenPeer.value = widget.contacts[index];
      setState(() {
        selectedIndex = index;
      });
      return true;
    }
    return false;
  }

  @override
  Widget build(BuildContext context) {
    return ListView.separated(
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
      itemCount: widget.contacts.length,
      separatorBuilder: (context, index) => const SizedBox(height: 6),
      itemBuilder: (context, index) {
        final contact = widget.contacts[index];
        final isSelected = selectedIndex == index;

        return ContactCard(
          initials: contact.initials,
          name: contact.name,
          ipAddress: contact.ip,
          lastMessage: contact.lastMessage,
          statusColor: contact.statusColor,
          isSelected: isSelected,
          onTap: () {
            chosenPeer.value = widget.contacts[index];
            setState(() {
              selectedIndex = index;
            });
          },
        );
      },
    );
  }
}

final GlobalKey<ContactListWidgetState> contactListKey = GlobalKey<ContactListWidgetState>();
