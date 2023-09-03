package io.memoria.active.testsuite;

import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.StateId;
import io.vavr.collection.List;
import io.vavr.control.Option;
import io.vavr.control.Try;

public interface ESScenario<E extends Event> {
  int expectedCommandsCount();

  int expectedEventsCount();

  List<Option<Try<E>>> handleCommands();

  boolean verify(StateId stateId);
}
