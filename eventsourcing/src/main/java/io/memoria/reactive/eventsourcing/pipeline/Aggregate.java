package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.EventId;
import io.memoria.atom.eventsourcing.State;
import io.memoria.reactive.eventsourcing.repo.EventRepo;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.vavr.collection.Stream;
import io.vavr.control.Try;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Aggregate<S extends State, C extends Command, E extends Event> {
  private final Domain<S, C, E> domain;
  private final AtomicReference<S> state;
  private final AtomicInteger eventSeqId;
  private final EventRepo<E> eventRepo;
  private final CommandRoute commandRoute;
  private final CommandStream<C> commandStream;
  private final Set<CommandId> processedCommands;
  private final Set<EventId> processedSagaEvents;

  public Aggregate(Domain<S, C, E> domain,
                   EventRepo<E> eventRepo,
                   CommandRoute commandRoute,
                   CommandStream<C> commandStream) {
    this.domain = domain;
    this.state = new AtomicReference<>();
    this.eventSeqId = new AtomicInteger();
    this.eventRepo = eventRepo;
    this.commandRoute = commandRoute;
    this.commandStream = commandStream;
    this.processedCommands = new HashSet<>();
    this.processedSagaEvents = new HashSet<>();
  }

  public Stream<Try<E>> init(String aggId) {
    return eventRepo.fetch(aggId).map(tr -> tr.flatMap(this::evolve));
  }

  public Try<E> handle(C cmd) {
    if (isDuplicateCommand(cmd) || isDuplicateSagaCommand(cmd)) {
      return Try.failure(new NoSuchElementException());
    } else {
      return decide(cmd).flatMap(this::publish).flatMap(this::evolve).flatMap(this::saga);
    }
  }

  Try<E> decide(C cmd) {
    S currentState = state.get();
    if (currentState == null) {
      return domain.decider().apply(cmd);
    } else {
      return domain.decider().apply(currentState, cmd);
    }
  }

  Try<E> evolve(E e) {
    S currentState = state.get();
    var isSuccess = false;
    if (currentState == null) {
      var newState = domain.evolver().apply(e);
      isSuccess = state.compareAndSet(null, newState);
    } else {
      var newState = domain.evolver().apply(currentState, e);
      isSuccess = state.compareAndSet(currentState, newState);
    }
    if (isSuccess) {
      eventSeqId.getAndIncrement();
      processedCommands.add(e.commandId());
      e.sagaEventId().forEach(processedSagaEvents::add);
      return Try.success(e);
    } else {
      return Try.failure(new IllegalArgumentException("Corrupted current State" + state));
    }
  }

  Try<E> saga(E e) {
    return domain.saga().apply(e).map(this::publish).getOrElse(Try.failure(new NoSuchElementException())).map(c -> e);
  }

  Try<E> publish(E event) {
    return eventRepo.append(event.stateId().id().value(), eventSeqId.get(), event);
  }

  Try<C> publish(C cmd) {
    var partition = cmd.partition(commandRoute.totalPartitions());
    return commandStream.append(commandRoute.topic(), partition, cmd);
  }

  private boolean isDuplicateCommand(C cmd) {
    return processedCommands.contains(cmd.commandId());
  }

  private boolean isDuplicateSagaCommand(C cmd) {
    return cmd.sagaEventId().map(processedSagaEvents::contains).getOrElse(false);
  }
}
