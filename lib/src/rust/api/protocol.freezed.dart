// GENERATED CODE - DO NOT MODIFY BY HAND
// coverage:ignore-file
// ignore_for_file: type=lint
// ignore_for_file: unused_element, deprecated_member_use, deprecated_member_use_from_same_package, use_function_type_syntax_for_parameters, unnecessary_const, avoid_init_to_null, invalid_override_different_default_values_named, prefer_expression_function_bodies, annotate_overrides, invalid_annotation_target, unnecessary_question_mark

part of 'protocol.dart';

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

@optionalTypeArgs TResult maybeMap<TResult extends Object?>({TResult Function( MessagePayload_Handshake value)?  handshake,TResult Function( MessagePayload_ChatMessage value)?  chatMessage,TResult Function( MessagePayload_Ack value)?  ack,TResult Function( MessagePayload_Ping value)?  ping,TResult Function( MessagePayload_Pong value)?  pong,required TResult orElse(),}){
final _that = this;
switch (_that) {
case MessagePayload_Handshake() when handshake != null:
return handshake(_that);case MessagePayload_ChatMessage() when chatMessage != null:
return chatMessage(_that);case MessagePayload_Ack() when ack != null:
return ack(_that);case MessagePayload_Ping() when ping != null:
return ping(_that);case MessagePayload_Pong() when pong != null:
return pong(_that);case _:
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

@optionalTypeArgs TResult map<TResult extends Object?>({required TResult Function( MessagePayload_Handshake value)  handshake,required TResult Function( MessagePayload_ChatMessage value)  chatMessage,required TResult Function( MessagePayload_Ack value)  ack,required TResult Function( MessagePayload_Ping value)  ping,required TResult Function( MessagePayload_Pong value)  pong,}){
final _that = this;
switch (_that) {
case MessagePayload_Handshake():
return handshake(_that);case MessagePayload_ChatMessage():
return chatMessage(_that);case MessagePayload_Ack():
return ack(_that);case MessagePayload_Ping():
return ping(_that);case MessagePayload_Pong():
return pong(_that);}
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

@optionalTypeArgs TResult? mapOrNull<TResult extends Object?>({TResult? Function( MessagePayload_Handshake value)?  handshake,TResult? Function( MessagePayload_ChatMessage value)?  chatMessage,TResult? Function( MessagePayload_Ack value)?  ack,TResult? Function( MessagePayload_Ping value)?  ping,TResult? Function( MessagePayload_Pong value)?  pong,}){
final _that = this;
switch (_that) {
case MessagePayload_Handshake() when handshake != null:
return handshake(_that);case MessagePayload_ChatMessage() when chatMessage != null:
return chatMessage(_that);case MessagePayload_Ack() when ack != null:
return ack(_that);case MessagePayload_Ping() when ping != null:
return ping(_that);case MessagePayload_Pong() when pong != null:
return pong(_that);case _:
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

@optionalTypeArgs TResult maybeWhen<TResult extends Object?>({TResult Function( String clientVersion,  String? publicKey)?  handshake,TResult Function( String content)?  chatMessage,TResult Function( String targetMsgId,  String status)?  ack,TResult Function()?  ping,TResult Function()?  pong,required TResult orElse(),}) {final _that = this;
switch (_that) {
case MessagePayload_Handshake() when handshake != null:
return handshake(_that.clientVersion,_that.publicKey);case MessagePayload_ChatMessage() when chatMessage != null:
return chatMessage(_that.content);case MessagePayload_Ack() when ack != null:
return ack(_that.targetMsgId,_that.status);case MessagePayload_Ping() when ping != null:
return ping();case MessagePayload_Pong() when pong != null:
return pong();case _:
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

@optionalTypeArgs TResult when<TResult extends Object?>({required TResult Function( String clientVersion,  String? publicKey)  handshake,required TResult Function( String content)  chatMessage,required TResult Function( String targetMsgId,  String status)  ack,required TResult Function()  ping,required TResult Function()  pong,}) {final _that = this;
switch (_that) {
case MessagePayload_Handshake():
return handshake(_that.clientVersion,_that.publicKey);case MessagePayload_ChatMessage():
return chatMessage(_that.content);case MessagePayload_Ack():
return ack(_that.targetMsgId,_that.status);case MessagePayload_Ping():
return ping();case MessagePayload_Pong():
return pong();}
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

@optionalTypeArgs TResult? whenOrNull<TResult extends Object?>({TResult? Function( String clientVersion,  String? publicKey)?  handshake,TResult? Function( String content)?  chatMessage,TResult? Function( String targetMsgId,  String status)?  ack,TResult? Function()?  ping,TResult? Function()?  pong,}) {final _that = this;
switch (_that) {
case MessagePayload_Handshake() when handshake != null:
return handshake(_that.clientVersion,_that.publicKey);case MessagePayload_ChatMessage() when chatMessage != null:
return chatMessage(_that.content);case MessagePayload_Ack() when ack != null:
return ack(_that.targetMsgId,_that.status);case MessagePayload_Ping() when ping != null:
return ping();case MessagePayload_Pong() when pong != null:
return pong();case _:
  return null;

}
}

}

/// @nodoc


class MessagePayload_Handshake extends MessagePayload {
  const MessagePayload_Handshake({required this.clientVersion, this.publicKey}): super._();
  

 final  String clientVersion;
 final  String? publicKey;

/// Create a copy of MessagePayload
/// with the given fields replaced by the non-null parameter values.
@JsonKey(includeFromJson: false, includeToJson: false)
@pragma('vm:prefer-inline')
$MessagePayload_HandshakeCopyWith<MessagePayload_Handshake> get copyWith => _$MessagePayload_HandshakeCopyWithImpl<MessagePayload_Handshake>(this, _$identity);



@override
bool operator ==(Object other) {
  return identical(this, other) || (other.runtimeType == runtimeType&&other is MessagePayload_Handshake&&(identical(other.clientVersion, clientVersion) || other.clientVersion == clientVersion)&&(identical(other.publicKey, publicKey) || other.publicKey == publicKey));
}


@override
int get hashCode => Object.hash(runtimeType,clientVersion,publicKey);

@override
String toString() {
  return 'MessagePayload.handshake(clientVersion: $clientVersion, publicKey: $publicKey)';
}


}

/// @nodoc
abstract mixin class $MessagePayload_HandshakeCopyWith<$Res> implements $MessagePayloadCopyWith<$Res> {
  factory $MessagePayload_HandshakeCopyWith(MessagePayload_Handshake value, $Res Function(MessagePayload_Handshake) _then) = _$MessagePayload_HandshakeCopyWithImpl;
@useResult
$Res call({
 String clientVersion, String? publicKey
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
@pragma('vm:prefer-inline') $Res call({Object? clientVersion = null,Object? publicKey = freezed,}) {
  return _then(MessagePayload_Handshake(
clientVersion: null == clientVersion ? _self.clientVersion : clientVersion // ignore: cast_nullable_to_non_nullable
as String,publicKey: freezed == publicKey ? _self.publicKey : publicKey // ignore: cast_nullable_to_non_nullable
as String?,
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


class MessagePayload_Ack extends MessagePayload {
  const MessagePayload_Ack({required this.targetMsgId, required this.status}): super._();
  

 final  String targetMsgId;
 final  String status;

/// Create a copy of MessagePayload
/// with the given fields replaced by the non-null parameter values.
@JsonKey(includeFromJson: false, includeToJson: false)
@pragma('vm:prefer-inline')
$MessagePayload_AckCopyWith<MessagePayload_Ack> get copyWith => _$MessagePayload_AckCopyWithImpl<MessagePayload_Ack>(this, _$identity);



@override
bool operator ==(Object other) {
  return identical(this, other) || (other.runtimeType == runtimeType&&other is MessagePayload_Ack&&(identical(other.targetMsgId, targetMsgId) || other.targetMsgId == targetMsgId)&&(identical(other.status, status) || other.status == status));
}


@override
int get hashCode => Object.hash(runtimeType,targetMsgId,status);

@override
String toString() {
  return 'MessagePayload.ack(targetMsgId: $targetMsgId, status: $status)';
}


}

/// @nodoc
abstract mixin class $MessagePayload_AckCopyWith<$Res> implements $MessagePayloadCopyWith<$Res> {
  factory $MessagePayload_AckCopyWith(MessagePayload_Ack value, $Res Function(MessagePayload_Ack) _then) = _$MessagePayload_AckCopyWithImpl;
@useResult
$Res call({
 String targetMsgId, String status
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
@pragma('vm:prefer-inline') $Res call({Object? targetMsgId = null,Object? status = null,}) {
  return _then(MessagePayload_Ack(
targetMsgId: null == targetMsgId ? _self.targetMsgId : targetMsgId // ignore: cast_nullable_to_non_nullable
as String,status: null == status ? _self.status : status // ignore: cast_nullable_to_non_nullable
as String,
  ));
}


}

/// @nodoc


class MessagePayload_Ping extends MessagePayload {
  const MessagePayload_Ping(): super._();
  






@override
bool operator ==(Object other) {
  return identical(this, other) || (other.runtimeType == runtimeType&&other is MessagePayload_Ping);
}


@override
int get hashCode => runtimeType.hashCode;

@override
String toString() {
  return 'MessagePayload.ping()';
}


}




/// @nodoc


class MessagePayload_Pong extends MessagePayload {
  const MessagePayload_Pong(): super._();
  






@override
bool operator ==(Object other) {
  return identical(this, other) || (other.runtimeType == runtimeType&&other is MessagePayload_Pong);
}


@override
int get hashCode => runtimeType.hashCode;

@override
String toString() {
  return 'MessagePayload.pong()';
}


}




// dart format on
