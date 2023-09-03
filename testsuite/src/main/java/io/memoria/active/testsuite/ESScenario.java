package io.memoria.active.testsuite;

import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.StateId;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Try;

public interface ESScenario<C extends Command, E extends Event> {
  int expectedCommandsCount();

  int expectedEventsCount();

  Stream<Option<Try<E>>> handleCommands();

  boolean verify(StateId stateId);
}
