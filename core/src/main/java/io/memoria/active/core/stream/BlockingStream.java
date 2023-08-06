package io.memoria.active.core.stream;

import io.vavr.collection.Stream;
import io.vavr.control.Try;

public interface BlockingStream<T> {
  Try<T> append(T t);

  Stream<Try<T>> stream();

  static <T> BlockingStream<T> inMemory() {
    return new MemBlockingStream<>();
  }
}
