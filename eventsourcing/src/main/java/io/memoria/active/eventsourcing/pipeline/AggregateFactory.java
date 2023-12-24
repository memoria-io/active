package io.memoria.active.eventsourcing.pipeline;

import io.memoria.active.eventsourcing.CommandPublisher;
import io.memoria.active.eventsourcing.EventRepo;
import io.memoria.atom.actor.Actor;
import io.memoria.atom.actor.ActorFactory;
import io.memoria.atom.actor.ActorId;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.StateId;

public class AggregateFactory implements ActorFactory {
  private final Domain domain;
  private final EventRepo eventRepo;
  private final CommandPublisher publisher;

  public AggregateFactory(Domain domain, EventRepo eventRepo, CommandPublisher publisher) {
    this.domain = domain;
    this.eventRepo = eventRepo;
    this.publisher = publisher;
  }

  @Override
  public Actor create(ActorId id) {
    return new Aggregate(StateId.of(id), domain, eventRepo, publisher);
  }
}
