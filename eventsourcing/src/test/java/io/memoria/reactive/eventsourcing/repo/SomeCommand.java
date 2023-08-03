package io.memoria.reactive.eventsourcing.repo;

import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.EventId;
import io.memoria.atom.eventsourcing.StateId;
import io.vavr.control.Option;

record SomeCommand(EventId eventId, StateId stateId, CommandId commandId, Option<EventId> sagaEventId)
        implements Command {
  public SomeCommand(EventId eventId, StateId stateId, CommandId commandId) {
    this(eventId, stateId, commandId, Option.none());
  }

  @Override
  public long timestamp() {
    return 0;
  }
}
