import 'package:flutter/cupertino.dart';
import 'package:flutter_secure_storage/flutter_secure_storage.dart';
import 'package:uuid/uuid.dart';

class DeviceIdManager {
  static const _storage = FlutterSecureStorage();
  static const _keyDeviceId = 'unique_device_id';

  /// 获取或创建唯一的设备 ID
  static Future<String> getOrCreateDeviceId() async {
    try {
      // 1. 尝试从加密存储（Keychain/Keystore）中读取
      String? deviceId = await _storage.read(key: _keyDeviceId);

      // 2. 如果不存在，则生成并持久化
      if (deviceId == null || deviceId.isEmpty) {
        deviceId = const Uuid().v4(); // 生成 UUIDv4，例如: "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
        await _storage.write(key: _keyDeviceId, value: deviceId);
      }

      return deviceId;
    } catch (e) {
      debugPrint("ERROR when getting deviceId: $e");
      // 容错处理：如果硬件或权限异常，返回一个临时的 UUID（这次启动有效）
      return const Uuid().v4();
    }
  }
}
