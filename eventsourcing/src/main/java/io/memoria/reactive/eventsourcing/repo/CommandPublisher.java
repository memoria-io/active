package io.memoria.reactive.eventsourcing.repo;

import io.memoria.atom.eventsourcing.Command;
import io.vavr.control.Try;

public interface CommandPublisher<C extends Command> {
  Try<C> publish(C cmd);
}
