// GENERATED CODE - DO NOT MODIFY BY HAND
// coverage:ignore-file
// ignore_for_file: type=lint
// ignore_for_file: unused_element, deprecated_member_use, deprecated_member_use_from_same_package, use_function_type_syntax_for_parameters, unnecessary_const, avoid_init_to_null, invalid_override_different_default_values_named, prefer_expression_function_bodies, annotate_overrides, invalid_annotation_target, unnecessary_question_mark

part of 'models.dart';

// **************************************************************************
// FreezedGenerator
// **************************************************************************

// dart format off
T _$identity<T>(T value) => value;
/// @nodoc
mixin _$MessagePayload {





@override
bool operator ==(Object other) {
  return identical(this, other) || (other.runtimeType == runtimeType&&other is MessagePayload);
}


@override
int get hashCode => runtimeType.hashCode;

@override
String toString() {
  return 'MessagePayload()';
}


}

/// @nodoc
class $MessagePayloadCopyWith<$Res>  {
$MessagePayloadCopyWith(MessagePayload _, $Res Function(MessagePayload) __);
}


/// Adds pattern-matching-related methods to [MessagePayload].
extension MessagePayloadPatterns on MessagePayload {
/// A variant of `map` that fallback to returning `orElse`.
///
/// It is equivalent to doing:
/// ```dart
/// switch (sealedClass) {
///   case final Subclass value:
///     return ...;
///   case _:
///     return orElse();
/// }
/// ```

@optionalTypeArgs TResult maybeMap<TResult extends Object?>({TResult Function( MessagePayload_Handshake value)?  handshake,TResult Function( MessagePayload_ChatMessage value)?  chatMessage,TResult Function( MessagePayload_FileTransferInit value)?  fileTransferInit,TResult Function( MessagePayload_FileChunk value)?  fileChunk,TResult Function( MessagePayload_ChunkAck value)?  chunkAck,TResult Function( MessagePayload_FileCompleteAck value)?  fileCompleteAck,TResult Function( MessagePayload_Ack value)?  ack,required TResult orElse(),}){
final _that = this;
switch (_that) {
case MessagePayload_Handshake() when handshake != null:
return handshake(_that);case MessagePayload_ChatMessage() when chatMessage != null:
return chatMessage(_that);case MessagePayload_FileTransferInit() when fileTransferInit != null:
return fileTransferInit(_that);case MessagePayload_FileChunk() when fileChunk != null:
return fileChunk(_that);case MessagePayload_ChunkAck() when chunkAck != null:
return chunkAck(_that);case MessagePayload_FileCompleteAck() when fileCompleteAck != null:
return fileCompleteAck(_that);case MessagePayload_Ack() when ack != null:
return ack(_that);case _:
  return orElse();

}
}
/// A `switch`-like method, using callbacks.
///
/// Callbacks receives the raw object, upcasted.
/// It is equivalent to doing:
/// ```dart
/// switch (sealedClass) {
///   case final Subclass value:
///     return ...;
///   case final Subclass2 value:
///     return ...;
/// }
/// ```

@optionalTypeArgs TResult map<TResult extends Object?>({required TResult Function( MessagePayload_Handshake value)  handshake,required TResult Function( MessagePayload_ChatMessage value)  chatMessage,required TResult Function( MessagePayload_FileTransferInit value)  fileTransferInit,required TResult Function( MessagePayload_FileChunk value)  fileChunk,required TResult Function( MessagePayload_ChunkAck value)  chunkAck,required TResult Function( MessagePayload_FileCompleteAck value)  fileCompleteAck,required TResult Function( MessagePayload_Ack value)  ack,}){
final _that = this;
switch (_that) {
case MessagePayload_Handshake():
return handshake(_that);case MessagePayload_ChatMessage():
return chatMessage(_that);case MessagePayload_FileTransferInit():
return fileTransferInit(_that);case MessagePayload_FileChunk():
return fileChunk(_that);case MessagePayload_ChunkAck():
return chunkAck(_that);case MessagePayload_FileCompleteAck():
return fileCompleteAck(_that);case MessagePayload_Ack():
return ack(_that);}
}
/// A variant of `map` that fallback to returning `null`.
///
/// It is equivalent to doing:
/// ```dart
/// switch (sealedClass) {
///   case final Subclass value:
///     return ...;
///   case _:
///     return null;
/// }
/// ```

@optionalTypeArgs TResult? mapOrNull<TResult extends Object?>({TResult? Function( MessagePayload_Handshake value)?  handshake,TResult? Function( MessagePayload_ChatMessage value)?  chatMessage,TResult? Function( MessagePayload_FileTransferInit value)?  fileTransferInit,TResult? Function( MessagePayload_FileChunk value)?  fileChunk,TResult? Function( MessagePayload_ChunkAck value)?  chunkAck,TResult? Function( MessagePayload_FileCompleteAck value)?  fileCompleteAck,TResult? Function( MessagePayload_Ack value)?  ack,}){
final _that = this;
switch (_that) {
case MessagePayload_Handshake() when handshake != null:
return handshake(_that);case MessagePayload_ChatMessage() when chatMessage != null:
return chatMessage(_that);case MessagePayload_FileTransferInit() when fileTransferInit != null:
return fileTransferInit(_that);case MessagePayload_FileChunk() when fileChunk != null:
return fileChunk(_that);case MessagePayload_ChunkAck() when chunkAck != null:
return chunkAck(_that);case MessagePayload_FileCompleteAck() when fileCompleteAck != null:
return fileCompleteAck(_that);case MessagePayload_Ack() when ack != null:
return ack(_that);case _:
  return null;

}
}
/// A variant of `when` that fallback to an `orElse` callback.
///
/// It is equivalent to doing:
/// ```dart
/// switch (sealedClass) {
///   case Subclass(:final field):
///     return ...;
///   case _:
///     return orElse();
/// }
/// ```

@optionalTypeArgs TResult maybeWhen<TResult extends Object?>({TResult Function( String senderMac)?  handshake,TResult Function( String content)?  chatMessage,TResult Function( String transferId,  FileType fileType,  String fileName,  BigInt fileSize,  int totalChunks,  String fileHash)?  fileTransferInit,TResult Function( ChunkHeader header,  Uint8List data)?  fileChunk,TResult Function( String transferId,  int chunkIndex)?  chunkAck,TResult Function( String transferId)?  fileCompleteAck,TResult Function( String targetMsgId)?  ack,required TResult orElse(),}) {final _that = this;
switch (_that) {
case MessagePayload_Handshake() when handshake != null:
return handshake(_that.senderMac);case MessagePayload_ChatMessage() when chatMessage != null:
return chatMessage(_that.content);case MessagePayload_FileTransferInit() when fileTransferInit != null:
return fileTransferInit(_that.transferId,_that.fileType,_that.fileName,_that.fileSize,_that.totalChunks,_that.fileHash);case MessagePayload_FileChunk() when fileChunk != null:
return fileChunk(_that.header,_that.data);case MessagePayload_ChunkAck() when chunkAck != null:
return chunkAck(_that.transferId,_that.chunkIndex);case MessagePayload_FileCompleteAck() when fileCompleteAck != null:
return fileCompleteAck(_that.transferId);case MessagePayload_Ack() when ack != null:
return ack(_that.targetMsgId);case _:
  return orElse();

}
}
/// A `switch`-like method, using callbacks.
///
/// As opposed to `map`, this offers destructuring.
/// It is equivalent to doing:
/// ```dart
/// switch (sealedClass) {
///   case Subclass(:final field):
///     return ...;
///   case Subclass2(:final field2):
///     return ...;
/// }
/// ```

@optionalTypeArgs TResult when<TResult extends Object?>({required TResult Function( String senderMac)  handshake,required TResult Function( String content)  chatMessage,required TResult Function( String transferId,  FileType fileType,  String fileName,  BigInt fileSize,  int totalChunks,  String fileHash)  fileTransferInit,required TResult Function( ChunkHeader header,  Uint8List data)  fileChunk,required TResult Function( String transferId,  int chunkIndex)  chunkAck,required TResult Function( String transferId)  fileCompleteAck,required TResult Function( String targetMsgId)  ack,}) {final _that = this;
switch (_that) {
case MessagePayload_Handshake():
return handshake(_that.senderMac);case MessagePayload_ChatMessage():
return chatMessage(_that.content);case MessagePayload_FileTransferInit():
return fileTransferInit(_that.transferId,_that.fileType,_that.fileName,_that.fileSize,_that.totalChunks,_that.fileHash);case MessagePayload_FileChunk():
return fileChunk(_that.header,_that.data);case MessagePayload_ChunkAck():
return chunkAck(_that.transferId,_that.chunkIndex);case MessagePayload_FileCompleteAck():
return fileCompleteAck(_that.transferId);case MessagePayload_Ack():
return ack(_that.targetMsgId);}
}
/// A variant of `when` that fallback to returning `null`
///
/// It is equivalent to doing:
/// ```dart
/// switch (sealedClass) {
///   case Subclass(:final field):
///     return ...;
///   case _:
///     return null;
/// }
/// ```

@optionalTypeArgs TResult? whenOrNull<TResult extends Object?>({TResult? Function( String senderMac)?  handshake,TResult? Function( String content)?  chatMessage,TResult? Function( String transferId,  FileType fileType,  String fileName,  BigInt fileSize,  int totalChunks,  String fileHash)?  fileTransferInit,TResult? Function( ChunkHeader header,  Uint8List data)?  fileChunk,TResult? Function( String transferId,  int chunkIndex)?  chunkAck,TResult? Function( String transferId)?  fileCompleteAck,TResult? Function( String targetMsgId)?  ack,}) {final _that = this;
switch (_that) {
case MessagePayload_Handshake() when handshake != null:
return handshake(_that.senderMac);case MessagePayload_ChatMessage() when chatMessage != null:
return chatMessage(_that.content);case MessagePayload_FileTransferInit() when fileTransferInit != null:
return fileTransferInit(_that.transferId,_that.fileType,_that.fileName,_that.fileSize,_that.totalChunks,_that.fileHash);case MessagePayload_FileChunk() when fileChunk != null:
return fileChunk(_that.header,_that.data);case MessagePayload_ChunkAck() when chunkAck != null:
return chunkAck(_that.transferId,_that.chunkIndex);case MessagePayload_FileCompleteAck() when fileCompleteAck != null:
return fileCompleteAck(_that.transferId);case MessagePayload_Ack() when ack != null:
return ack(_that.targetMsgId);case _:
  return null;

}
}

}

/// @nodoc


class MessagePayload_Handshake extends MessagePayload {
  const MessagePayload_Handshake({required this.senderMac}): super._();
  

 final  String senderMac;

/// Create a copy of MessagePayload
/// with the given fields replaced by the non-null parameter values.
@JsonKey(includeFromJson: false, includeToJson: false)
@pragma('vm:prefer-inline')
$MessagePayload_HandshakeCopyWith<MessagePayload_Handshake> get copyWith => _$MessagePayload_HandshakeCopyWithImpl<MessagePayload_Handshake>(this, _$identity);



@override
bool operator ==(Object other) {
  return identical(this, other) || (other.runtimeType == runtimeType&&other is MessagePayload_Handshake&&(identical(other.senderMac, senderMac) || other.senderMac == senderMac));
}


@override
int get hashCode => Object.hash(runtimeType,senderMac);

@override
String toString() {
  return 'MessagePayload.handshake(senderMac: $senderMac)';
}


}

/// @nodoc
abstract mixin class $MessagePayload_HandshakeCopyWith<$Res> implements $MessagePayloadCopyWith<$Res> {
  factory $MessagePayload_HandshakeCopyWith(MessagePayload_Handshake value, $Res Function(MessagePayload_Handshake) _then) = _$MessagePayload_HandshakeCopyWithImpl;
@useResult
$Res call({
 String senderMac
});




}
/// @nodoc
class _$MessagePayload_HandshakeCopyWithImpl<$Res>
    implements $MessagePayload_HandshakeCopyWith<$Res> {
  _$MessagePayload_HandshakeCopyWithImpl(this._self, this._then);

  final MessagePayload_Handshake _self;
  final $Res Function(MessagePayload_Handshake) _then;

/// Create a copy of MessagePayload
/// with the given fields replaced by the non-null parameter values.
@pragma('vm:prefer-inline') $Res call({Object? senderMac = null,}) {
  return _then(MessagePayload_Handshake(
senderMac: null == senderMac ? _self.senderMac : senderMac // ignore: cast_nullable_to_non_nullable
as String,
  ));
}


}

/// @nodoc


class MessagePayload_ChatMessage extends MessagePayload {
  const MessagePayload_ChatMessage({required this.content}): super._();
  

 final  String content;

/// Create a copy of MessagePayload
/// with the given fields replaced by the non-null parameter values.
@JsonKey(includeFromJson: false, includeToJson: false)
@pragma('vm:prefer-inline')
$MessagePayload_ChatMessageCopyWith<MessagePayload_ChatMessage> get copyWith => _$MessagePayload_ChatMessageCopyWithImpl<MessagePayload_ChatMessage>(this, _$identity);



@override
bool operator ==(Object other) {
  return identical(this, other) || (other.runtimeType == runtimeType&&other is MessagePayload_ChatMessage&&(identical(other.content, content) || other.content == content));
}


@override
int get hashCode => Object.hash(runtimeType,content);

@override
String toString() {
  return 'MessagePayload.chatMessage(content: $content)';
}


}

/// @nodoc
abstract mixin class $MessagePayload_ChatMessageCopyWith<$Res> implements $MessagePayloadCopyWith<$Res> {
  factory $MessagePayload_ChatMessageCopyWith(MessagePayload_ChatMessage value, $Res Function(MessagePayload_ChatMessage) _then) = _$MessagePayload_ChatMessageCopyWithImpl;
@useResult
$Res call({
 String content
});




}
/// @nodoc
class _$MessagePayload_ChatMessageCopyWithImpl<$Res>
    implements $MessagePayload_ChatMessageCopyWith<$Res> {
  _$MessagePayload_ChatMessageCopyWithImpl(this._self, this._then);

  final MessagePayload_ChatMessage _self;
  final $Res Function(MessagePayload_ChatMessage) _then;

/// Create a copy of MessagePayload
/// with the given fields replaced by the non-null parameter values.
@pragma('vm:prefer-inline') $Res call({Object? content = null,}) {
  return _then(MessagePayload_ChatMessage(
content: null == content ? _self.content : content // ignore: cast_nullable_to_non_nullable
as String,
  ));
}


}

/// @nodoc


class MessagePayload_FileTransferInit extends MessagePayload {
  const MessagePayload_FileTransferInit({required this.transferId, required this.fileType, required this.fileName, required this.fileSize, required this.totalChunks, required this.fileHash}): super._();
  

 final  String transferId;
 final  FileType fileType;
 final  String fileName;
 final  BigInt fileSize;
 final  int totalChunks;
 final  String fileHash;

/// Create a copy of MessagePayload
/// with the given fields replaced by the non-null parameter values.
@JsonKey(includeFromJson: false, includeToJson: false)
@pragma('vm:prefer-inline')
$MessagePayload_FileTransferInitCopyWith<MessagePayload_FileTransferInit> get copyWith => _$MessagePayload_FileTransferInitCopyWithImpl<MessagePayload_FileTransferInit>(this, _$identity);



@override
bool operator ==(Object other) {
  return identical(this, other) || (other.runtimeType == runtimeType&&other is MessagePayload_FileTransferInit&&(identical(other.transferId, transferId) || other.transferId == transferId)&&(identical(other.fileType, fileType) || other.fileType == fileType)&&(identical(other.fileName, fileName) || other.fileName == fileName)&&(identical(other.fileSize, fileSize) || other.fileSize == fileSize)&&(identical(other.totalChunks, totalChunks) || other.totalChunks == totalChunks)&&(identical(other.fileHash, fileHash) || other.fileHash == fileHash));
}


@override
int get hashCode => Object.hash(runtimeType,transferId,fileType,fileName,fileSize,totalChunks,fileHash);

@override
String toString() {
  return 'MessagePayload.fileTransferInit(transferId: $transferId, fileType: $fileType, fileName: $fileName, fileSize: $fileSize, totalChunks: $totalChunks, fileHash: $fileHash)';
}


}

/// @nodoc
abstract mixin class $MessagePayload_FileTransferInitCopyWith<$Res> implements $MessagePayloadCopyWith<$Res> {
  factory $MessagePayload_FileTransferInitCopyWith(MessagePayload_FileTransferInit value, $Res Function(MessagePayload_FileTransferInit) _then) = _$MessagePayload_FileTransferInitCopyWithImpl;
@useResult
$Res call({
 String transferId, FileType fileType, String fileName, BigInt fileSize, int totalChunks, String fileHash
});




}
/// @nodoc
class _$MessagePayload_FileTransferInitCopyWithImpl<$Res>
    implements $MessagePayload_FileTransferInitCopyWith<$Res> {
  _$MessagePayload_FileTransferInitCopyWithImpl(this._self, this._then);

  final MessagePayload_FileTransferInit _self;
  final $Res Function(MessagePayload_FileTransferInit) _then;

/// Create a copy of MessagePayload
/// with the given fields replaced by the non-null parameter values.
@pragma('vm:prefer-inline') $Res call({Object? transferId = null,Object? fileType = null,Object? fileName = null,Object? fileSize = null,Object? totalChunks = null,Object? fileHash = null,}) {
  return _then(MessagePayload_FileTransferInit(
transferId: null == transferId ? _self.transferId : transferId // ignore: cast_nullable_to_non_nullable
as String,fileType: null == fileType ? _self.fileType : fileType // ignore: cast_nullable_to_non_nullable
as FileType,fileName: null == fileName ? _self.fileName : fileName // ignore: cast_nullable_to_non_nullable
as String,fileSize: null == fileSize ? _self.fileSize : fileSize // ignore: cast_nullable_to_non_nullable
as BigInt,totalChunks: null == totalChunks ? _self.totalChunks : totalChunks // ignore: cast_nullable_to_non_nullable
as int,fileHash: null == fileHash ? _self.fileHash : fileHash // ignore: cast_nullable_to_non_nullable
as String,
  ));
}


}

/// @nodoc


class MessagePayload_FileChunk extends MessagePayload {
  const MessagePayload_FileChunk({required this.header, required this.data}): super._();
  

 final  ChunkHeader header;
 final  Uint8List data;

/// Create a copy of MessagePayload
/// with the given fields replaced by the non-null parameter values.
@JsonKey(includeFromJson: false, includeToJson: false)
@pragma('vm:prefer-inline')
$MessagePayload_FileChunkCopyWith<MessagePayload_FileChunk> get copyWith => _$MessagePayload_FileChunkCopyWithImpl<MessagePayload_FileChunk>(this, _$identity);



@override
bool operator ==(Object other) {
  return identical(this, other) || (other.runtimeType == runtimeType&&other is MessagePayload_FileChunk&&(identical(other.header, header) || other.header == header)&&const DeepCollectionEquality().equals(other.data, data));
}


@override
int get hashCode => Object.hash(runtimeType,header,const DeepCollectionEquality().hash(data));

@override
String toString() {
  return 'MessagePayload.fileChunk(header: $header, data: $data)';
}


}

/// @nodoc
abstract mixin class $MessagePayload_FileChunkCopyWith<$Res> implements $MessagePayloadCopyWith<$Res> {
  factory $MessagePayload_FileChunkCopyWith(MessagePayload_FileChunk value, $Res Function(MessagePayload_FileChunk) _then) = _$MessagePayload_FileChunkCopyWithImpl;
@useResult
$Res call({
 ChunkHeader header, Uint8List data
});




}
/// @nodoc
class _$MessagePayload_FileChunkCopyWithImpl<$Res>
    implements $MessagePayload_FileChunkCopyWith<$Res> {
  _$MessagePayload_FileChunkCopyWithImpl(this._self, this._then);

  final MessagePayload_FileChunk _self;
  final $Res Function(MessagePayload_FileChunk) _then;

/// Create a copy of MessagePayload
/// with the given fields replaced by the non-null parameter values.
@pragma('vm:prefer-inline') $Res call({Object? header = null,Object? data = null,}) {
  return _then(MessagePayload_FileChunk(
header: null == header ? _self.header : header // ignore: cast_nullable_to_non_nullable
as ChunkHeader,data: null == data ? _self.data : data // ignore: cast_nullable_to_non_nullable
as Uint8List,
  ));
}


}

/// @nodoc


class MessagePayload_ChunkAck extends MessagePayload {
  const MessagePayload_ChunkAck({required this.transferId, required this.chunkIndex}): super._();
  

 final  String transferId;
 final  int chunkIndex;

/// Create a copy of MessagePayload
/// with the given fields replaced by the non-null parameter values.
@JsonKey(includeFromJson: false, includeToJson: false)
@pragma('vm:prefer-inline')
$MessagePayload_ChunkAckCopyWith<MessagePayload_ChunkAck> get copyWith => _$MessagePayload_ChunkAckCopyWithImpl<MessagePayload_ChunkAck>(this, _$identity);



@override
bool operator ==(Object other) {
  return identical(this, other) || (other.runtimeType == runtimeType&&other is MessagePayload_ChunkAck&&(identical(other.transferId, transferId) || other.transferId == transferId)&&(identical(other.chunkIndex, chunkIndex) || other.chunkIndex == chunkIndex));
}


@override
int get hashCode => Object.hash(runtimeType,transferId,chunkIndex);

@override
String toString() {
  return 'MessagePayload.chunkAck(transferId: $transferId, chunkIndex: $chunkIndex)';
}


}

/// @nodoc
abstract mixin class $MessagePayload_ChunkAckCopyWith<$Res> implements $MessagePayloadCopyWith<$Res> {
  factory $MessagePayload_ChunkAckCopyWith(MessagePayload_ChunkAck value, $Res Function(MessagePayload_ChunkAck) _then) = _$MessagePayload_ChunkAckCopyWithImpl;
@useResult
$Res call({
 String transferId, int chunkIndex
});




}
/// @nodoc
class _$MessagePayload_ChunkAckCopyWithImpl<$Res>
    implements $MessagePayload_ChunkAckCopyWith<$Res> {
  _$MessagePayload_ChunkAckCopyWithImpl(this._self, this._then);

  final MessagePayload_ChunkAck _self;
  final $Res Function(MessagePayload_ChunkAck) _then;

/// Create a copy of MessagePayload
/// with the given fields replaced by the non-null parameter values.
@pragma('vm:prefer-inline') $Res call({Object? transferId = null,Object? chunkIndex = null,}) {
  return _then(MessagePayload_ChunkAck(
transferId: null == transferId ? _self.transferId : transferId // ignore: cast_nullable_to_non_nullable
as String,chunkIndex: null == chunkIndex ? _self.chunkIndex : chunkIndex // ignore: cast_nullable_to_non_nullable
as int,
  ));
}


}

/// @nodoc


class MessagePayload_FileCompleteAck extends MessagePayload {
  const MessagePayload_FileCompleteAck({required this.transferId}): super._();
  

 final  String transferId;

/// Create a copy of MessagePayload
/// with the given fields replaced by the non-null parameter values.
@JsonKey(includeFromJson: false, includeToJson: false)
@pragma('vm:prefer-inline')
$MessagePayload_FileCompleteAckCopyWith<MessagePayload_FileCompleteAck> get copyWith => _$MessagePayload_FileCompleteAckCopyWithImpl<MessagePayload_FileCompleteAck>(this, _$identity);



@override
bool operator ==(Object other) {
  return identical(this, other) || (other.runtimeType == runtimeType&&other is MessagePayload_FileCompleteAck&&(identical(other.transferId, transferId) || other.transferId == transferId));
}


@override
int get hashCode => Object.hash(runtimeType,transferId);

@override
String toString() {
  return 'MessagePayload.fileCompleteAck(transferId: $transferId)';
}


}

/// @nodoc
abstract mixin class $MessagePayload_FileCompleteAckCopyWith<$Res> implements $MessagePayloadCopyWith<$Res> {
  factory $MessagePayload_FileCompleteAckCopyWith(MessagePayload_FileCompleteAck value, $Res Function(MessagePayload_FileCompleteAck) _then) = _$MessagePayload_FileCompleteAckCopyWithImpl;
@useResult
$Res call({
 String transferId
});




}
/// @nodoc
class _$MessagePayload_FileCompleteAckCopyWithImpl<$Res>
    implements $MessagePayload_FileCompleteAckCopyWith<$Res> {
  _$MessagePayload_FileCompleteAckCopyWithImpl(this._self, this._then);

  final MessagePayload_FileCompleteAck _self;
  final $Res Function(MessagePayload_FileCompleteAck) _then;

/// Create a copy of MessagePayload
/// with the given fields replaced by the non-null parameter values.
@pragma('vm:prefer-inline') $Res call({Object? transferId = null,}) {
  return _then(MessagePayload_FileCompleteAck(
transferId: null == transferId ? _self.transferId : transferId // ignore: cast_nullable_to_non_nullable
as String,
  ));
}


}

/// @nodoc


class MessagePayload_Ack extends MessagePayload {
  const MessagePayload_Ack({required this.targetMsgId}): super._();
  

 final  String targetMsgId;

/// Create a copy of MessagePayload
/// with the given fields replaced by the non-null parameter values.
@JsonKey(includeFromJson: false, includeToJson: false)
@pragma('vm:prefer-inline')
$MessagePayload_AckCopyWith<MessagePayload_Ack> get copyWith => _$MessagePayload_AckCopyWithImpl<MessagePayload_Ack>(this, _$identity);



@override
bool operator ==(Object other) {
  return identical(this, other) || (other.runtimeType == runtimeType&&other is MessagePayload_Ack&&(identical(other.targetMsgId, targetMsgId) || other.targetMsgId == targetMsgId));
}


@override
int get hashCode => Object.hash(runtimeType,targetMsgId);

@override
String toString() {
  return 'MessagePayload.ack(targetMsgId: $targetMsgId)';
}


}

/// @nodoc
abstract mixin class $MessagePayload_AckCopyWith<$Res> implements $MessagePayloadCopyWith<$Res> {
  factory $MessagePayload_AckCopyWith(MessagePayload_Ack value, $Res Function(MessagePayload_Ack) _then) = _$MessagePayload_AckCopyWithImpl;
@useResult
$Res call({
 String targetMsgId
});




}
/// @nodoc
class _$MessagePayload_AckCopyWithImpl<$Res>
    implements $MessagePayload_AckCopyWith<$Res> {
  _$MessagePayload_AckCopyWithImpl(this._self, this._then);

  final MessagePayload_Ack _self;
  final $Res Function(MessagePayload_Ack) _then;

/// Create a copy of MessagePayload
/// with the given fields replaced by the non-null parameter values.
@pragma('vm:prefer-inline') $Res call({Object? targetMsgId = null,}) {
  return _then(MessagePayload_Ack(
targetMsgId: null == targetMsgId ? _self.targetMsgId : targetMsgId // ignore: cast_nullable_to_non_nullable
as String,
  ));
}


}

// dart format on
