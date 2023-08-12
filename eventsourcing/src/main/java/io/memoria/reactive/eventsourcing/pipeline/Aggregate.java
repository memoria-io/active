package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.core.caching.KCache;
import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.ESException.MismatchingStateId;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.State;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.reactive.eventsourcing.repo.EventRepo;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class Aggregate<S extends State, C extends Command, E extends Event> {
  public final StateId stateId;
  private final Domain<S, C, E> domain;
  private final AtomicReference<S> state;
  private final AtomicInteger eventSeqId;
  private final EventRepo<E> eventRepo;
  private final CommandTopic commandTopic;
  private final CommandStream<C> commandStream;
  private final KCache<CommandId> processedCommands;
  private final BlockingDeque<C> commands;
  private final Thread thread;

  public Aggregate(StateId stateId,
                   Domain<S, C, E> domain,
                   EventRepo<E> eventRepo,
                   CommandTopic commandTopic,
                   CommandStream<C> commandStream,
                   Consumer<Try<E>> eventConsumer) {
    this(stateId, domain, eventRepo, commandTopic, commandStream, eventConsumer, 1000_000);
  }

  public Aggregate(StateId stateId,
                   Domain<S, C, E> domain,
                   EventRepo<E> eventRepo,
                   CommandTopic commandTopic,
                   CommandStream<C> commandStream,
                   Consumer<Try<E>> eventConsumer,
                   int capacity) {
    this.stateId = stateId;
    this.domain = domain;
    this.state = new AtomicReference<>();
    this.eventSeqId = new AtomicInteger();
    this.eventRepo = eventRepo;
    this.commandTopic = commandTopic;
    this.commandStream = commandStream;
    this.processedCommands = new KCache<>(capacity);
    this.commands = new LinkedBlockingDeque<>();
    this.thread = Thread.ofVirtual().unstarted(() -> Stream.concat(initialize(), handle()).forEach(eventConsumer));
  }

  public void start() {
    this.thread.start();
  }

  public void append(C cmd) {
    if (!this.thread.isAlive()) {
      throw new IllegalStateException("Thread: %s is no longer alive".formatted(thread.getName()));
    }
    if (cmd.meta().stateId().equals(this.stateId)) {
      commands.add(cmd);
    } else {
      throw MismatchingStateId.of(this.stateId, cmd.meta().stateId());
    }
  }

  Stream<Try<E>> initialize() {
    return eventRepo.fetch(stateId).peek(tr -> tr.peek(this::evolve));
  }

  Stream<Try<E>> handle() {
    return Stream.continually(() -> Try.of(commands::take))
                 .map(tr -> tr.flatMap(this::handle).filter(Option::isDefined).map(Option::get));
  }

  Try<Option<E>> handle(C cmd) {
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
    return eventRepo.append(event.meta().stateId(), eventSeqId.get(), event);
  }

  Try<C> publish(C cmd) {
    var partition = cmd.meta().partition(commandTopic.totalPartitions());
    return commandStream.append(commandTopic.name(), partition, cmd);
  }
}
