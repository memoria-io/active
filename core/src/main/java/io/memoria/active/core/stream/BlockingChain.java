package io.memoria.active.core.stream;

import io.vavr.collection.Stream;
import io.vavr.control.Try;

public interface BlockingChain<T> {
  Try<T> append(T t);

  Stream<Try<T>> stream();

  static <T> BlockingChain<T> inMemory() {
    return new MemBlockingChain<>();
  }
}
