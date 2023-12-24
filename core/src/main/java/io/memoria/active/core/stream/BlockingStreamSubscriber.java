package io.memoria.active.core.stream;

import io.vavr.collection.Stream;
import io.vavr.control.Try;

public interface BlockingStreamSubscriber {
  Stream<Try<Msg>> fetch(String topic, int partition);
}
