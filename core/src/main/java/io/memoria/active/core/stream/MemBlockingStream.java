package io.memoria.active.core.stream;

import io.vavr.collection.Stream;
import io.vavr.control.Try;

import java.util.HashMap;
import java.util.Map;

class MemBlockingStream implements BlockingStream {
  private final Map<String, Map<Integer, BlockingChain<String>>> topics;

  public MemBlockingStream() {
    topics = new HashMap<>();
  }

  @Override
  public Try<String> append(String topic, int partition, String msg) {
    addTopic(topic, partition);
    return this.topics.get(topic).get(partition).append(msg);
  }


  @Override
  public Stream<Try<String>> stream(String topic, int partition) {
    addTopic(topic,partition);
    return this.topics.get(topic).get(partition).stream();
  }
  private void addTopic(String topic, int partition) {
    this.topics.computeIfAbsent(topic, k -> new HashMap<>());
    this.topics.get(topic).computeIfAbsent(partition, k -> BlockingChain.inMemory());
  }
}
