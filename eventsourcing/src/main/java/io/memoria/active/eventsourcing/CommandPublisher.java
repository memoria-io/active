package io.memoria.active.eventsourcing;

import io.memoria.atom.eventsourcing.Command;
import io.vavr.control.Try;

public interface CommandPublisher {
  Try<Command> publish(Command cmd);
}
