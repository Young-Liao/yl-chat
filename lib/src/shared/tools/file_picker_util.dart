import 'dart:io';
import 'package:file_picker/file_picker.dart';
import 'package:flutter/foundation.dart';
import 'package:image_picker/image_picker.dart';
import 'package:path/path.dart' as p;
import 'package:path_provider/path_provider.dart';

/// 通用文件选择器数据模型
class PickedFileData {
  final String name;
  final String sizeFormatted;
  final int sizeInBytes;
  final File file;
  final String extension;

  PickedFileData({
    required this.name,
    required this.sizeFormatted,
    required this.sizeInBytes,
    required this.file,
    required this.extension,
  });
}

/// FilePicker 工具类
class FilePickerUtil {
  FilePickerUtil._();

  static final ImagePicker _imagePicker = ImagePicker();

  /// 拷贝文件至本地 App 沙盒 Documents 目录
  static Future<File> _copyToSandbox(File sourceFile, String originalName) async {
    final docsDir = await getApplicationDocumentsDirectory();
    final attachmentDir = Directory(p.join(docsDir.path, 'attachments'));
    if (!await attachmentDir.exists()) {
      await attachmentDir.create(recursive: true);
    }

    final String timePrefix = DateTime.now().millisecondsSinceEpoch.toString();
    final String targetPath = p.join(attachmentDir.path, '${timePrefix}_$originalName');
    return await sourceFile.copy(targetPath);
  }

  /// 选择单个文件
  static Future<PickedFileData?> pickSingleFile({
    FileType type = FileType.any,
    List<String>? allowedExtensions,
  }) async {
    try {
      final result = await FilePicker.pickFile(
        type: type,
        allowedExtensions: allowedExtensions,
      );

      if (result != null && result.path != null) {
        return await _convertAndSavePlatformFile(result);
      }
    } catch (e) {
      debugPrint('选择文件出错: $e');
    }
    return null;
  }

  /// 选择多个文件
  static Future<List<PickedFileData>> pickMultipleFiles({
    FileType type = FileType.any,
    List<String>? allowedExtensions,
  }) async {
    try {
      final result = await FilePicker.pickFiles(
        type: type,
        allowedExtensions: allowedExtensions,
      );

      if (result.isNotEmpty) {
        List<PickedFileData> list = [];
        for (var platformFile in result) {
          if (platformFile.path != null) {
            list.add(await _convertAndSavePlatformFile(platformFile));
          }
        }
        return list;
      }
    } catch (e) {
      debugPrint('选择多文件出错: $e');
    }
    return [];
  }

  /// 仅选择图片文件（单选）：iOS/Android 调起系统相册，桌面端调起文件选择器
  static Future<PickedFileData?> pickImage() async {
    if (!kIsWeb && (Platform.isIOS || Platform.isAndroid)) {
      try {
        final XFile? xFile = await _imagePicker.pickImage(source: ImageSource.gallery);
        if (xFile != null) {
          return await _convertAndSaveXFile(xFile);
        }
      } catch (e) {
        debugPrint('调起相册失败: $e');
      }
      return null;
    }

    return pickSingleFile(type: FileType.image);
  }

  /// 仅选择图片文件（多选）
  static Future<List<PickedFileData>> pickMultipleImages() async {
    if (!kIsWeb && (Platform.isIOS || Platform.isAndroid)) {
      try {
        final List<XFile> xFiles = await _imagePicker.pickMultiImage();
        if (xFiles.isNotEmpty) {
          List<PickedFileData> result = [];
          for (var xFile in xFiles) {
            result.add(await _convertAndSaveXFile(xFile));
          }
          return result;
        }
      } catch (e) {
        debugPrint('调起相册多选失败: $e');
      }
      return [];
    }

    return pickMultipleFiles(type: FileType.image);
  }

  /// 选择文件夹
  static Future<String?> pickDirectory() async {
    try {
      return await FilePicker.getDirectoryPath();
    } catch (e) {
      debugPrint('选择文件夹出错: $e');
      return null;
    }
  }

  /// 转换 PlatformFile 并保存至沙盒
  static Future<PickedFileData> _convertAndSavePlatformFile(PlatformFile platformFile) async {
    final originalFile = File(platformFile.path!);
    final savedFile = await _copyToSandbox(originalFile, platformFile.name);
    final int sizeInBytes = savedFile.lengthSync();
    final String ext = platformFile.extension ?? savedFile.path.split('.').last;

    return PickedFileData(
      name: platformFile.name,
      sizeFormatted: formatBytes(sizeInBytes),
      sizeInBytes: sizeInBytes,
      file: savedFile,
      extension: ext.toLowerCase(),
    );
  }

  /// 转换 XFile 并保存至沙盒
  static Future<PickedFileData> _convertAndSaveXFile(XFile xFile) async {
    final tempFile = File(xFile.path);
    final savedFile = await _copyToSandbox(tempFile, xFile.name);
    final int sizeInBytes = savedFile.lengthSync();
    final String ext = savedFile.path.split('.').last;

    return PickedFileData(
      name: xFile.name,
      sizeFormatted: formatBytes(sizeInBytes),
      sizeInBytes: sizeInBytes,
      file: savedFile,
      extension: ext.toLowerCase(),
    );
  }

  /// 字节大小格式化工具方法
  static String formatBytes(int bytes, [int decimals = 1]) {
    if (bytes <= 0) return "0 B";
    const suffixes = ["B", "KB", "MB", "GB", "TB"];
    var i = (bytes.toString().length - 1) ~/ 3;
    if (i >= suffixes.length) i = suffixes.length - 1;
    double num = bytes / (1 << (10 * i));
    return '${num.toStringAsFixed(decimals)} ${suffixes[i]}';
  }
}
