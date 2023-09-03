package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.core.caching.KVCache;
import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.State;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.reactive.eventsourcing.repo.EventRepo;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.vavr.collection.Stream;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.function.Function;

public class PartitionPipeline<S extends State, C extends Command, E extends Event> {
  private static final Logger log = LoggerFactory.getLogger(PartitionPipeline.class.getName());

  public final Domain<S, C, E> domain;
  private final EventRepo<E> eventRepo;
  private final CommandRoute commandRoute;
  private final CommandStream<C> commandStream;
  private final KVCache<StateId, Aggregate<S, C, E>> aggMap;

  public PartitionPipeline(Domain<S, C, E> domain,
                           EventRepo<E> eventRepo,
                           CommandRoute commandRoute,
                           CommandStream<C> commandStream,
                           KVCache<StateId, Aggregate<S, C, E>> aggMap) {
    this.domain = domain;
    this.eventRepo = eventRepo;
    this.commandRoute = commandRoute;
    this.commandStream = commandStream;
    this.aggMap = aggMap;
  }

  public Stream<Try<E>> handle() {
    return commandStream.stream(commandRoute.name(), commandRoute.partition())
                        .map(tr -> tr.flatMap(this::handle))
                        .filter(this::isValid);
  }

  public Try<C> pubCommand(C cmd) {
    var newPartition = cmd.meta().partition(commandRoute.totalPartitions());
    return commandStream.append(commandRoute.name(), newPartition, cmd);
  }

  public Try<Stream<E>> fetchEvents(StateId stateId) {
    return eventRepo.fetch(stateId);
  }

  Try<E> handle(C cmd) {
    StateId stateId = cmd.meta().stateId();
    return Try.of(() -> {
      aggMap.putIfAbsent(stateId, k -> initAggregate(stateId));
      return aggMap.get(stateId).get().handle(cmd).toTry().flatMap(Function.identity());
    }).flatMap(Function.identity());
  }

  /**
   * @return initialized aggregate
   */
  Aggregate<S, C, E> initAggregate(StateId stateId) {
    var aggregate = new Aggregate<>(stateId, domain, eventRepo, commandRoute, commandStream);
    aggregate.initialize().get().forEach(e -> log.info("Initializing with %s ".formatted(e.meta())));
    return aggregate;
  }

  private boolean isValid(Try<E> tr) {
    return tr.isSuccess() || !(tr.getCause() instanceof NoSuchElementException);
  }
}
