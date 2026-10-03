import 'package:flutter/material.dart';
import 'package:yl_chat/src/theme/abstract_theme.dart';

class TitleBar extends StatelessWidget
{
  final String name;
  final String ip;
  final String status;

  const TitleBar({super.key, required this.name, required this.ip, required this.status});

  @override
  Widget build(BuildContext context) 
  {
    final theme = context.appTheme;

    return Container(
      padding: EdgeInsets.symmetric(vertical: 20, horizontal: 25),
      decoration: BoxDecoration(
        color: theme.bgPanel,
        border: Border(
          bottom: BorderSide(
            color: theme.border
          )
        ),
        borderRadius: BorderRadius.only(
          topRight: Radius.circular(12.0)
        )
      ),
      // height: 75.0,
      child: Row(
        children: [
          Stack(
            children: [CircleAvatar(
                radius: 20,
                backgroundColor: const Color(0xFF334155),
                child: Text(
                  "PC",
                  style: TextStyle(
                    color: theme.textMain,
                    fontSize: 14,
                    fontWeight: FontWeight.bold
                  )
                )
              ),
              Positioned(
                bottom: 0,
                right: 0,
                child: Container(
                  width: 10,
                  height: 10,
                  decoration: BoxDecoration(
                    color: theme.statusOnline,
                    shape: BoxShape.circle,
                    border: Border.all(
                      color: theme.bgPanel,
                      width: 2
                    )
                  )
                )
              )
            ]
          ),
          const SizedBox(width: 14),
          Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                name,
                style: TextStyle(
                  color: theme.textMain,
                  fontSize: 16,
                  fontWeight: FontWeight.bold
                )
              ),
              SizedBox(height: 5.0),
              Text(
                "$ip · $status",
                style: TextStyle(
                  color: theme.textMuted,
                  fontSize: 12,
                  fontFamily: "monospace"
                )
              )
            ]
          )
        ]
      )
    );
  }
}
