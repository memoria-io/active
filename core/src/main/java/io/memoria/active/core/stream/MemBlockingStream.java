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
  public Try<Msg> publish(String topic, int partition, Msg msg) {
    addTopic(topic, partition);
    return this.topics.get(topic).get(partition).append(msg);
  }

  @Override
  public Stream<Try<MsgResult>> stream(String topic, int partition, boolean fromStart) {
    addTopic(topic, partition);
    return this.topics.get(topic).get(partition).stream().map(tr -> tr.map(MemBlockingStream::toMsgResult));
  }

  @Override
  public void close() {
    // Silence is golden
  }

  private static MsgResult toMsgResult(Msg msg) {
    return new MsgResult(msg, () -> {});
  }

  private void addTopic(String topic, int partition) {
    this.topics.computeIfAbsent(topic, k -> new HashMap<>());
    this.topics.get(topic).computeIfAbsent(partition, k -> BlockingChain.inMemory());
  }
}
