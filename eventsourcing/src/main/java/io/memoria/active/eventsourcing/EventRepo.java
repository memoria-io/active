package io.memoria.active.eventsourcing;

import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.StateId;
import io.vavr.collection.List;
import io.vavr.control.Try;

public interface EventRepo {

  Try<Event> append(Event e);

  Try<List<Event>> fetch(StateId stateId);
}

