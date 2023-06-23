package io.memoria.active.core.stream;

import io.vavr.collection.Stream;
import io.vavr.control.Try;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class MemESMsgStream implements ESMsgStream {
  private final Map<String, Map<Integer, MemStream<ESMsg>>> topics = new ConcurrentHashMap<>();

  @Override
  public Try<ESMsg> pub(ESMsg msg) {
    addPartitionSink(msg.topic(), msg.partition());
    return Try.success(this.publishFn(msg));
  }

  @Override
  public Stream<ESMsg> sub(String topic, int partition) {
    addPartitionSink(topic, partition);
    return this.topics.get(topic).get(partition).stream();
  }

  private void addPartitionSink(String topic, int partition) {
    this.topics.computeIfAbsent(topic, x -> new ConcurrentHashMap<>());
    this.topics.computeIfPresent(topic, (k, v) -> {
      var sink = new MemStream<ESMsg>();
      v.computeIfAbsent(partition, x -> sink);
      return v;
    });
  }

  private ESMsg publishFn(ESMsg msg) {
    String topic = msg.topic();
    int partition = msg.partition();
    this.topics.get(topic).get(partition).add(msg);
    return msg;
  }
}
