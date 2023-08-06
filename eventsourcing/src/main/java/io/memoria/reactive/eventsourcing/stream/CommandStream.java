package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.eventsourcing.Command;
import io.vavr.collection.Stream;
import io.vavr.control.Try;

public interface CommandStream<C extends Command> {
  Try<C> append(String topic, int partition, C cmd);

  Stream<Try<C>> stream(String topic, int partition);

  static <C extends Command> CommandStream<C> inMemory() {
    return new MemCommandStream<>();
  }
}

