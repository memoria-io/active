package io.memoria.active.core.stream;

import io.vavr.control.Try;

public interface BlockingStreamPublisher {
  Try<Msg> publish(String topic, int partition, Msg msg);
}
