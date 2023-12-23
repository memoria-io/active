package io.memoria.active.core.repo.stack;

import io.vavr.collection.List;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class MemStackAdapter implements StackRepo {
  private final Map<StackId, java.util.List<StackItem>> aggregates = new ConcurrentHashMap<>();

  @Override
  public Try<StackItem> append(StackItem msg) {
    this.aggregates.computeIfAbsent(msg.stackId(), _ -> new ArrayList<>());
    int size = this.aggregates.get(msg.stackId()).size();
    if (size != msg.index()) {
      throw new IllegalArgumentException("Invalid msg seqId:%d for a list size of:%d".formatted(msg.index(), size));
    }
    this.aggregates.get(msg.stackId()).add(msg);
    return Try.success(msg);
  }

  @Override
  public Try<List<StackItem>> fetch(StackId stackId) {
    var list = Option.of(this.aggregates.get(stackId)).map(List::ofAll).getOrElse(List.of());
    return Try.success(list);
  }

  @Override
  public Try<Integer> size(StackId stackId) {
    int size = Option.of(this.aggregates.get(stackId)).map(java.util.List::size).getOrElse(0);
    return Try.success(size);
  }

  @Override
  public void close() {
    // Silence is golden
  }
}
