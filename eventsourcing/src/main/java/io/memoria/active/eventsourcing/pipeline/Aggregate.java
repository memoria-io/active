package io.memoria.active.eventsourcing.pipeline;

import io.memoria.active.eventsourcing.CommandRepo;
import io.memoria.active.eventsourcing.EventRepo;
import io.memoria.active.eventsourcing.exceptions.AlreadyHandledException;
import io.memoria.atom.actor.Actor;
import io.memoria.atom.actor.ActorId;
import io.memoria.atom.core.domain.Shardable;
import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.State;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.eventsourcing.exceptions.UnknownImplementation;
import io.vavr.collection.List;
import io.vavr.control.Try;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Aggregate implements Actor {
  public final ActorId actorId;
  public final StateId stateId;
  public final Domain domain;
  private final AtomicReference<State> state;
  private final AtomicInteger eventSeqId;
  private final EventRepo eventRepo;
  private final CommandRepo commandRepo;
  private final Set<CommandId> processedCommands;

  public Aggregate(StateId stateId, Domain domain, EventRepo eventRepo, CommandRepo commandRepo) {
    this.actorId = new ActorId(stateId);
    this.stateId = stateId;
    this.domain = domain;
    this.state = new AtomicReference<>();
    this.eventSeqId = new AtomicInteger();
    this.eventRepo = eventRepo;
    this.commandRepo = commandRepo;
    this.processedCommands = new HashSet<>();
  }

  public Try<List<Event>> initialize() {
    return eventRepo.fetch(stateId).map(eTry -> eTry.map(this::evolve));
  }

  public Try<Event> handle(Command cmd) {
    if (processedCommands.contains(cmd.meta().commandId())) {
      return Try.failure(AlreadyHandledException.of(cmd));
    } else {
      return decide(cmd).peek(this::evolve).flatMap(this::saga).flatMap(this::publish);
    }
  }

  @Override
  public ActorId actorId() {
    return actorId;
  }

  @Override
  public Try<Shardable> apply(Shardable message) {
    if (message instanceof Command c) {
      return handle(c).map(e -> e);
    } else {
      return Try.failure(UnknownImplementation.of(message));
    }
  }

  Try<Event> publish(Event event) {
    return eventRepo.append(event);
  }

  Try<Event> decide(Command cmd) {
    State currentState = state.get();
    if (currentState == null) {
      return domain.decider().apply(cmd);
    } else {
      return domain.decider().apply(currentState, cmd);
    }
  }

  Event evolve(Event e) {
    State currentState = state.get();
    State newState;
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

  Try<Event> saga(Event e) {
    return domain.saga().apply(e).map(commandRepo::publish).map(tr -> tr.map(_ -> e)).getOrElse(Try.success(e));
  }
}
