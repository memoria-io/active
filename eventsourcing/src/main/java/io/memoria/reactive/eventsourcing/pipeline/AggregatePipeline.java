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
import io.vavr.control.Try;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class AggregatePipeline<S extends State, C extends Command, E extends Event> {
  private final Map<StateId, Aggregate<S, C, E>> aggMap;
  private final Domain<S, C, E> domain;
  private final EventRepo<E> eventRepo;
  private final CommandTopic commandTopic;
  private final CommandStream<C> commandStream;
  private final KCache<CommandId> cache;
  private final Consumer<Try<E>> eventConsumer;

  public AggregatePipeline(Domain<S, C, E> domain,
                           EventRepo<E> eventRepo,
                           CommandTopic commandTopic,
                           CommandStream<C> commandStream,
                           KCache<CommandId> commandIdCache,
                           Consumer<Try<E>> eventConsumer) {
    this.domain = domain;
    this.eventRepo = eventRepo;
    this.commandTopic = commandTopic;
    this.commandStream = commandStream;
    this.cache = commandIdCache;
    this.eventConsumer = eventConsumer;
    this.aggMap = new ConcurrentHashMap<>();
  }

  public void handle(C cmd) {
    aggMap.computeIfAbsent(cmd.meta().stateId(), k -> startAggregate(cmd.meta().stateId()));
    aggMap.get(cmd.meta().stateId()).append(cmd);
  }

  private Aggregate<S, C, E> startAggregate(StateId stateId) {
    var agg = new Aggregate<>(stateId, domain, eventRepo, commandTopic, commandStream, cache, eventConsumer);
    agg.start();
    return agg;
  }

  private static <E extends Event> void defaultConsumer(Try<E> eventTry) {
    if (eventTry.isFailure()) {
      eventTry.getCause().printStackTrace();
      Thread.currentThread().interrupt();
    } else {
      eventTry.stdout();
    }
  }
}
