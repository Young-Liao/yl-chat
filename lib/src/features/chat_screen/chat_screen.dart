import 'package:flutter/cupertino.dart';
import 'package:flutter/material.dart';
import 'package:yl_chat/main.dart';
import 'package:yl_chat/src/features/chat_screen/chat_box/chat_box.dart';
import 'package:yl_chat/src/features/chat_screen/chat_box/title_bar.dart';
import 'package:yl_chat/src/features/chat_screen/side_panel/side_panel.dart';
import 'package:yl_chat/src/theme/abstract_theme.dart';

class ChatScreen extends StatelessWidget {
  const ChatScreen({
    super.key,
  });

  @override
  Widget build(BuildContext context) {
    final theme = context.appTheme;

    return Scaffold(
      backgroundColor: theme.bgDark,
      body: SafeArea(
        child: Container(
          margin: EdgeInsets.all(15.0),
          decoration: BoxDecoration(
            borderRadius: BorderRadius.all(Radius.circular(15.0)),
            border: Border.all(
              color: theme.border
            )
          ),
          child: Row(children: [
            SidePanel(theme: theme),
            const Expanded(child: ChatBox()),
          ],),
        )
      ),
    );
  }
}