package io.memoria.active.core.repo.seq;

import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class MemMsgRepo implements SeqRowRepo {
  private final Map<String, ArrayList<SeqRow>> aggregates = new ConcurrentHashMap<>();

  @Override
  public Try<SeqRow> append(SeqRow msg) {
    this.aggregates.computeIfAbsent(msg.aggId(), x -> new ArrayList<>());
    int size = this.aggregates.get(msg.aggId()).size();
    if (size != msg.seqId()) {
      throw new IllegalArgumentException("Invalid msg seqId:%d for a list size of:%d".formatted(msg.seqId(), size));
    }
    this.aggregates.get(msg.aggId()).add(msg);
    return Try.success(msg);
  }

  @Override
  public Try<Stream<SeqRow>> fetch(String aggId) {
    var stream = Stream.ofAll(getAgg(aggId).getOrElse(new ArrayList<>()));
    return Try.success(stream);
  }

  @Override
  public Try<Integer> size(String aggId) {
    int size = getAgg(aggId).map(ArrayList::size).getOrElse(0);
    return Try.success(size);
  }

  @Override
  public void close() {
    // Silence is golden
  }

  private Option<ArrayList<SeqRow>> getAgg(String aggId) {
    return Option.of(aggregates.get(aggId));
  }
}
