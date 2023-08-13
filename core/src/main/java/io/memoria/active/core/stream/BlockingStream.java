package io.memoria.active.core.stream;

import io.vavr.collection.Stream;
import io.vavr.control.Try;

public interface BlockingStream extends AutoCloseable {
  Try<Msg> publish(String topic, int partition, Msg msg);

  Stream<Try<MsgResult>> stream(String topic, int partition, boolean fromStart);

  default Stream<Try<MsgResult>> stream(String topic, int partition) {
    return stream(topic, partition, true);
  }
}
