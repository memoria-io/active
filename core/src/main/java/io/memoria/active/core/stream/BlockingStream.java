package io.memoria.active.core.stream;

import io.vavr.collection.Stream;
import io.vavr.control.Try;

public interface BlockingStream extends AutoCloseable {
  Try<Msg> publish(String topic, int partition, Msg msg);

  Try<Stream<Msg>> fetch(String topic, int partition);

  static BlockingStream inMemory() {
    return new MemBlockingStream();
  }
}
