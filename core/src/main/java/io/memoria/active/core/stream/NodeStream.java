package io.memoria.active.core.stream;

import io.vavr.collection.Stream;
import io.vavr.control.Try;

public interface NodeStream<T> {
  void append(T t);

  Stream<Try<T>> stream();

  static <T> NodeStream<T> inMemory() {
    return new MemNodeStream<>();
  }
}
