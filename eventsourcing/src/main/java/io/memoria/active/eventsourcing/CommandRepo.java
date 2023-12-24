package io.memoria.active.eventsourcing;

import io.memoria.atom.eventsourcing.Command;
import io.vavr.collection.Stream;
import io.vavr.control.Try;

public interface CommandRepo {
  Try<Command> publish(Command command);

  Stream<Try<Command>> stream();

  static CommandRepo inMemory() {
    return new MemCommandRepo();
  }
}
