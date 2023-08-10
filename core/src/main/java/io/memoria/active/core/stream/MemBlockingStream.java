package io.memoria.active.core.stream;

import io.vavr.collection.Stream;
import io.vavr.control.Try;

import java.util.HashMap;
import java.util.Map;

class MemBlockingStream implements BlockingStream {
  private final Map<String, Map<Integer, BlockingChain<Msg>>> topics;

  public MemBlockingStream() {
    topics = new HashMap<>();
  }

  @Override
  public Try<Msg> append(String topic, int partition, Msg msg) {
    addTopic(topic, partition);
    return this.topics.get(topic).get(partition).append(msg);
  }

  @Override
  public Stream<Try<Msg>> stream(String topic, int partition) {
    addTopic(topic, partition);
    return this.topics.get(topic).get(partition).stream();
  }

  @Override
  public void close() {
    // Silence is golden
  }

  private void addTopic(String topic, int partition) {
    this.topics.computeIfAbsent(topic, k -> new HashMap<>());
    this.topics.get(topic).computeIfAbsent(partition, k -> BlockingChain.inMemory());
  }
}
