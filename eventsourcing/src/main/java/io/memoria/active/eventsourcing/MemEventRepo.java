package io.memoria.active.eventsourcing;

import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.StateId;
import io.vavr.collection.List;
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
  public List<Try<Event>> fetch(StateId stateId) {
    return Option.of(map.get(stateId)).map(List::ofAll).getOrElse(List.of()).map(Try::success);
  }

  @Override
  public Try<Long> size(StateId stateId) {
    return Try.success((long) fetch(stateId).size());
  }
}
