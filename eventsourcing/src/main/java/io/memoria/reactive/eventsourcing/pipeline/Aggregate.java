package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.core.caching.KCache;
import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.State;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.reactive.eventsourcing.repo.EventRepo;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Aggregate<S extends State, C extends Command, E extends Event> {
  public final StateId stateId;
  private final Domain<S, C, E> domain;
  private final AtomicReference<S> state;
  private final AtomicInteger eventSeqId;
  private final EventRepo<E> eventRepo;
  private final CommandRoute commandRoute;
  private final CommandStream<C> commandStream;
  private final KCache<CommandId> processedCommands;

  public Aggregate(StateId stateId,
                   Domain<S, C, E> domain,
                   EventRepo<E> eventRepo,
                   CommandRoute commandRoute,
                   CommandStream<C> commandStream,
                   KCache<CommandId> commandsCache) {
    this.stateId = stateId;
    this.domain = domain;
    this.state = new AtomicReference<>();
    this.eventSeqId = new AtomicInteger();
    this.eventRepo = eventRepo;
    this.commandRoute = commandRoute;
    this.commandStream = commandStream;
    this.processedCommands = commandsCache;
  }

  public Try<Stream<E>> initialize() {
    return eventRepo.fetch(stateId).map(eTry -> eTry.map(this::evolve));
  }

  public Option<Try<E>> handle(C cmd) {
    if (processedCommands.contains(cmd.meta().commandId())) {
      return Option.none();
    } else {
      var result = decide(cmd).peek(this::evolve).flatMap(this::saga).flatMap(this::append);
      return Option.some(result);
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
    return domain.saga().apply(e).map(this::publish).map(tr -> tr.map(c -> e)).getOrElse(Try.success(e));
  }

  Try<E> append(E event) {
    return eventRepo.append(event.meta().stateId(), eventSeqId.get(), event);
  }

  Try<C> publish(C cmd) {
    var partition = cmd.meta().partition(commandRoute.totalPartitions());
    return commandStream.append(commandRoute.name(), partition, cmd);
  }
}
