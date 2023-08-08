package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.State;
import io.memoria.reactive.eventsourcing.repo.EventRepo;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.HashSet;
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
  }

  public Stream<Try<E>> init(String aggId) {
    return eventRepo.fetch(aggId).peek(tr -> tr.peek(this::evolve));
  }

  public Try<Option<E>> handle(C cmd) {
    if (processedCommands.contains(cmd.meta().commandId())) {
      return Try.of(Option::none);
    } else {
      return decide(cmd).peek(this::evolve).flatMap(this::saga).flatMap(this::append).map(Option::some);
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

  void evolve(E e) {
    S currentState = state.get();
    S newState;
    if (currentState == null) {
      newState = domain.evolver().apply(e);
    } else {
      newState = domain.evolver().apply(currentState, e);
    }
    state.set(newState);
    eventSeqId.getAndIncrement();
    processedCommands.add(e.meta().commandId());
  }

  Try<E> saga(E e) {
    return domain.saga().apply(e).map(this::publish).map(tr -> tr.map(c -> e)).getOrElse(Try.success(e));
  }

  Try<E> append(E event) {
    return eventRepo.append(event.meta().stateId().id().value(), eventSeqId.get(), event);
  }

  Try<C> publish(C cmd) {
    var partition = cmd.meta().partition(commandRoute.totalPartitions());
    return commandStream.append(commandRoute.topic(), partition, cmd);
  }
}
