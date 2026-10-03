import 'package:flutter/foundation.dart';

class PreviousValueNotifier<T> extends ValueNotifier<T> {
  T _previousValue;

  PreviousValueNotifier(T value)
      : _previousValue = value,
        super(value);

  /// Gets the value before the last mutation
  T get previousValue => _previousValue;

  @override
  set value(T newValue) {
    if (value == newValue) return;
    _previousValue = value; // Save old value before updating
    super.value = newValue;
  }
}

// Usage:
void main() {
  final counter = PreviousValueNotifier<int>(0);

  counter.addListener(() {
    print('Old: ${counter.previousValue}, New: ${counter.value}');
  });

  counter.value = 5; // Output: Old: 0, New: 5
  counter.value = 12; // Output: Old: 5, New: 12
}
