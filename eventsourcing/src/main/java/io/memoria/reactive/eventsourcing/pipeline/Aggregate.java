package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.State;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.reactive.eventsourcing.repo.CommandPublisher;
import io.memoria.reactive.eventsourcing.repo.EventRepo;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Aggregate<S extends State, C extends Command, E extends Event> {
  public final StateId stateId;
  public final Domain<S, C, E> domain;
  private final AtomicReference<S> state;
  private final AtomicInteger eventSeqId;
  private final EventRepo<E> eventRepo;
  private final CommandPublisher<C> commandPublisher;
  private final Set<CommandId> processedCommands;

  public Aggregate(StateId stateId,
                   Domain<S, C, E> domain,
                   EventRepo<E> eventRepo,
                   CommandPublisher<C> commandPublisher) {
    this.stateId = stateId;
    this.domain = domain;
    this.state = new AtomicReference<>();
    this.eventSeqId = new AtomicInteger();
    this.eventRepo = eventRepo;
    this.commandPublisher = commandPublisher;
    this.processedCommands = new HashSet<>();
  }

  public Option<Try<E>> handle(C cmd) {
    if (processedCommands.contains(cmd.meta().commandId())) {
      return Option.none();
    } else {
      var result = decide(cmd).peek(this::evolve).flatMap(this::saga).flatMap(this::publish);
      return Option.some(result);
    }
  }

  Try<E> publish(E event) {
    return eventRepo.append(event, eventSeqId.get());
  }

  Try<Stream<E>> initialize() {
    return eventRepo.fetch(stateId).map(eTry -> eTry.map(this::evolve));
  }

  Try<E> decide(C cmd) {
    S currentState = state.get();
    if (currentState == null) {
      return domain.decider().apply(cmd);
    } else {
      return domain.decider().apply(currentState, cmd);
    }
  }

  E evolve(E e) {
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
    return e;
  }

  Try<E> saga(E e) {
    return domain.saga().apply(e).map(commandPublisher::publish).map(tr -> tr.map(c -> e)).getOrElse(Try.success(e));
  }
}
