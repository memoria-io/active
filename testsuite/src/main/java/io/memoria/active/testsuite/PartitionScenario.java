package io.memoria.active.testsuite;

import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.Event;
import io.vavr.collection.Stream;
import io.vavr.control.Try;

public interface PartitionScenario<C extends Command, E extends Event> {
  int expectedCommandsCount();

  int expectedEventsCount();

  Stream<C> publishCommands();

  Stream<E> handleCommands();

  Try<Boolean> verify();
}
