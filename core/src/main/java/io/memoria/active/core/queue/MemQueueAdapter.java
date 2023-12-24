package io.memoria.active.core.queue;

import io.vavr.collection.List;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class MemQueueAdapter implements QueueRepo {
  private final Map<QueueId, java.util.List<QueueItem>> aggregates = new ConcurrentHashMap<>();

  @Override
  public Try<QueueItem> append(QueueItem msg) {
    this.aggregates.computeIfAbsent(msg.queueId(), _ -> new ArrayList<>());
    int size = this.aggregates.get(msg.queueId()).size();
    if (size != msg.itemIndex()) {
      throw new IllegalArgumentException("Invalid msg seqId:%d for a list size of:%d".formatted(msg.itemIndex(), size));
    }
    this.aggregates.get(msg.queueId()).add(msg);
    return Try.success(msg);
  }

  @Override
  public Try<List<QueueItem>> fetch(QueueId queueId) {
    var list = Option.of(this.aggregates.get(queueId)).map(List::ofAll).getOrElse(List.of());
    return Try.success(list);
  }

  @Override
  public Try<Integer> size(QueueId queueId) {
    int size = Option.of(this.aggregates.get(queueId)).map(java.util.List::size).getOrElse(0);
    return Try.success(size);
  }
}
