package io.memoria.active.core.stream;

import io.vavr.collection.Stream;
import io.vavr.control.Try;

public interface BlockingStream {
  Try<String> append(String topic, int partition, String msg);

  Stream<Try<String>> stream(String topic, int partition);
}
