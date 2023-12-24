package io.memoria.active.eventsourcing;

import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.StateId;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.control.Try;

public class MemEventRepo implements EventRepo{
  @Override
  public Try<Event> append(Event e) {
    return null;
  }

  @Override
  public Try<List<Event>> fetch(StateId stateId) {
    return null;
  }

  @Override
  public Try<Stream<Event>> stream(StateId stateId) {
    return null;
  }

  @Override
  public Try<Long> size(StateId stateId) {
    return null;
  }
}
