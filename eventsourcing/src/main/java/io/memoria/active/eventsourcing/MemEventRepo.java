package io.memoria.active.eventsourcing;

import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.StateId;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class MemEventRepo implements EventRepo {
  private final Map<StateId, java.util.List<Event>> map;

  MemEventRepo() {
    this(new ConcurrentHashMap<>());
  }

  MemEventRepo(Map<StateId, java.util.List<Event>> map) {
    this.map = map;
  }

  @Override
  public Try<Event> append(Event event) {
    map.computeIfAbsent(event.meta().stateId(), _ -> new ArrayList<>());
    map.computeIfPresent(event.meta().stateId(), (_, v) -> {
      v.add(event);
      return v;
    });
    return Try.success(event);
  }

  @Override
  public Try<List<Event>> fetch(StateId stateId) {
    var list = Option.of(map.get(stateId)).map(List::ofAll).getOrElse(List.of());
    return Try.success(list);
  }

  @Override
  public Try<Long> size(StateId stateId) {
    var size = (long) Option.of(map.get(stateId)).map(java.util.List::size).getOrElse(0);
    return Try.success(size);
  }
}
