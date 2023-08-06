package io.memoria.reactive.eventsourcing.stream;

import io.memoria.active.core.stream.BlockingStream;
import io.memoria.atom.eventsourcing.Command;
import io.vavr.collection.Stream;
import io.vavr.control.Try;

import java.util.HashMap;
import java.util.Map;

class MemCommandStream<C extends Command> implements CommandStream<C> {
  private final Map<String, Map<Integer, BlockingStream<C>>> topics;

  MemCommandStream() {
    topics = new HashMap<>();
  }

  @Override
  public Try<C> append(String topic, int partition, C cmd) {
    createTopic(topic, partition);
    topics.get(topic).get(partition).append(cmd);
    return Try.success(cmd);
  }

  private void createTopic(String topic, int partition) {
    topics.computeIfAbsent(topic, tp -> new HashMap<>());
    topics.computeIfPresent(topic, (tp, v) -> {
      v.computeIfAbsent(partition, p -> BlockingStream.inMemory());
      return v;
    });
  }

  @Override
  public Stream<Try<C>> stream(String topic, int partition) {
    createTopic(topic, partition);
    return topics.get(topic).get(partition).stream();
  }
}

