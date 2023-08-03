package io.memoria.reactive.eventsourcing.repo;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.Many;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class MemMsgRepo implements MsgRepo {
  private final int historySize;
  private final Map<String, Map<Integer, Many<Msg>>> topics = new ConcurrentHashMap<>();
  private final Map<String, Map<Integer, Msg>> lastMsg = new ConcurrentHashMap<>();

  public MemMsgRepo() {
    this(Integer.MAX_VALUE);
  }

  public MemMsgRepo(int historySize) {
    this.historySize = historySize;
  }

  @Override
  public Mono<Msg> last(String topic, int partition) {
    return Mono.defer(() -> Mono.justOrEmpty(lastMsg.get(topic))).flatMap(tp -> Mono.justOrEmpty(tp.get(partition)));
  }

  @Override
  public Mono<Msg> pub(String topic, int partition, Msg msg) {
    return Mono.fromCallable(() -> addPartitionSink(topic, partition))
               .flatMap(i -> Mono.fromCallable(() -> this.publishFn(topic, partition, msg)));
  }

  @Override
  public Flux<Msg> sub(String topic, int partition) {
    return Mono.fromCallable(() -> addPartitionSink(topic, partition))
               .flatMapMany(i -> this.topics.get(topic).get(partition).asFlux());
  }

  @Override
  public void close() {
    // Silence is golden
  }

  private int addPartitionSink(String topic, int partition) {
    this.topics.computeIfAbsent(topic, x -> new ConcurrentHashMap<>());
    this.topics.computeIfPresent(topic, (i, partitions) -> {
      var sink = Sinks.many().replay().<Msg>limit(historySize);
      partitions.computeIfAbsent(partition, x -> sink);
      return partitions;
    });
    this.lastMsg.computeIfAbsent(topic, x -> new ConcurrentHashMap<>());
    return partition;
  }

  private Msg publishFn(String topic, int partition, Msg msg) {
    this.topics.get(topic).get(partition).tryEmitNext(msg);
    this.lastMsg.get(topic).put(partition, msg);
    return msg;
  }
}
