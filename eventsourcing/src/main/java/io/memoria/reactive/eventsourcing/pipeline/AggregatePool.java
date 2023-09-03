package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.core.caching.KVCache;
import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.State;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.reactive.eventsourcing.repo.CommandPublisher;
import io.memoria.reactive.eventsourcing.repo.EventRepo;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregatePool<S extends State, C extends Command, E extends Event> {
  private static final Logger log = LoggerFactory.getLogger(AggregatePool.class.getName());

  public final Domain<S, C, E> domain;
  private final EventRepo<E> eventRepo;
  private final CommandPublisher<C> commandPublisher;
  private final KVCache<StateId, Aggregate<S, C, E>> aggMap;

  public AggregatePool(Domain<S, C, E> domain,
                       EventRepo<E> eventRepo,
                       CommandPublisher<C> commandPublisher,
                       KVCache<StateId, Aggregate<S, C, E>> aggMap) {
    this.domain = domain;
    this.eventRepo = eventRepo;
    this.commandPublisher = commandPublisher;
    this.aggMap = aggMap;
  }

  public Option<Try<E>> handle(C cmd) {
    var stateId = cmd.meta().stateId();
    aggMap.putIfAbsent(stateId, k -> initAggregate(stateId));
    return aggMap.get(stateId).get().handle(cmd);
  }

  public Try<Stream<E>> fetchEvents(StateId stateId) {
    return eventRepo.fetch(stateId);
  }

  /**
   * @return initialized aggregate
   */
  Aggregate<S, C, E> initAggregate(StateId stateId) {
    var aggregate = new Aggregate<>(stateId, domain, eventRepo, commandPublisher);
    aggregate.initialize().get().forEach(e -> log.info("Initializing with %s ".formatted(e.meta())));
    return aggregate;
  }
}
