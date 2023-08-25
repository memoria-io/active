package io.memoria.active.core.stream;

import io.vavr.collection.Stream;
import io.vavr.control.Try;

public interface BlockingStream extends AutoCloseable {
  Try<Msg> publish(String topic, int partition, Msg msg);

  Try<Stream<MsgResult>> fetch(String topic, int partition, boolean fromStart);

  default Try<Stream<MsgResult>> fetch(String topic, int partition) {
    return fetch(topic, partition, true);
  }

  static BlockingStream inMemory() {
    return new MemBlockingStream();
  }
}
