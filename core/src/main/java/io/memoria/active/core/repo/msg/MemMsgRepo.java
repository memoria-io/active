package io.memoria.active.core.repo.msg;

import io.vavr.collection.List;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class MemMsgRepo implements MsgRepo {
  private final Map<String, Map<String, ArrayList<Msg>>> topics = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Msg>> lastMsg = new ConcurrentHashMap<>();

  @Override
  public Try<Msg> append(String topic, Msg msg) {
    addPartitionSink(topic, msg.aggId());
    int size = this.topics.get(topic).get(msg.aggId()).size();
    if (size != msg.seqId()) {
      throw new IllegalArgumentException("Invalid msg seqId:%d for a list size of:%d".formatted(msg.seqId(), size));
    }
    this.topics.get(topic).get(msg.aggId()).add(msg);
    this.lastMsg.get(topic).put(msg.aggId(), msg);
    return Try.success(msg);
  }

  @Override
  public Try<List<Msg>> fetch(String topic, String aggId, int fromSeqId, int toSeqId) {
    var list = getAgg(topic, aggId).map(List::ofAll)
                                   .map(l -> l.subSequence(fromSeqId, toSeqId))
                                   .getOrElse(List.empty());
    return Try.success(list);
  }

  @Override
  public Try<Integer> size(String topic, String aggId) {
    int size = getAgg(topic, aggId).map(ArrayList::size).getOrElse(0);
    return Try.success(size);
  }

  @Override
  public void close() {
    // Silence is golden
  }

  private Option<ArrayList<Msg>> getAgg(String topic, String aggId) {
    return Option.of(topics.get(topic)).flatMap(tp -> Option.of(tp.get(aggId)));
  }

  private void addPartitionSink(String topic, String aggId) {
    this.topics.computeIfAbsent(topic, x -> new ConcurrentHashMap<>());
    this.topics.computeIfPresent(topic, (i, partitions) -> {
      var sink = new ArrayList<Msg>();
      partitions.computeIfAbsent(aggId, x -> sink);
      return partitions;
    });
    this.lastMsg.computeIfAbsent(topic, x -> new ConcurrentHashMap<>());
  }
}
