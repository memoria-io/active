package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.core.caching.KCache;
import io.memoria.atom.core.caching.KVCache;
import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.State;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.reactive.eventsourcing.repo.EventRepo;
import io.memoria.reactive.eventsourcing.stream.CommandResult;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.vavr.collection.Stream;
import io.vavr.control.Try;

import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Supplier;

public class PartitionPipeline<S extends State, C extends Command, E extends Event> {
  private final Domain<S, C, E> domain;
  private final EventRepo<E> eventRepo;
  private final CommandTopic commandTopic;
  private final CommandStream<C> commandStream;
  private final KVCache<StateId, Aggregate<S, C, E>> aggMap;
  private final Supplier<KCache<CommandId>> cacheSupplier;

  public PartitionPipeline(Domain<S, C, E> domain,
                           EventRepo<E> eventRepo,
                           CommandTopic commandTopic,
                           CommandStream<C> commandStream,
                           KVCache<StateId, Aggregate<S, C, E>> aggMap,
                           Supplier<KCache<CommandId>> commandIdCacheSupplier) {
    this.domain = domain;
    this.eventRepo = eventRepo;
    this.commandTopic = commandTopic;
    this.commandStream = commandStream;
    this.aggMap = aggMap;
    this.cacheSupplier = commandIdCacheSupplier;
  }

  public Stream<Try<E>> handle(int commandsPartition) {
    return commandStream.stream(commandTopic.name(), commandsPartition)
                        .map(tr -> tr.flatMap(this::handle))
                        .filter(this::isValid);
  }

  public Try<C> pubCommand(C cmd) {
    var partition = cmd.meta().partition(commandTopic.totalPartitions());
    return commandStream.append(commandTopic.name(), partition, cmd);
  }

  Try<E> handle(CommandResult<C> cmdResult) {
    StateId stateId = cmdResult.command().meta().stateId();
    aggMap.putIfAbsent(stateId, createAggregate(stateId));
    var result = aggMap.get(stateId).get().handle(cmdResult.command()).toTry().flatMap(Function.identity());
    if (result.isSuccess() || result.getCause() instanceof NoSuchElementException) {
      cmdResult.acknowledge().run();
    }
    return result;
  }

  private Aggregate<S, C, E> createAggregate(StateId stateId) {
    return new Aggregate<>(stateId, domain, eventRepo, commandTopic, commandStream, cacheSupplier.get());
  }

  private boolean isValid(Try<E> tr) {
    return tr.isSuccess() || !(tr.getCause() instanceof NoSuchElementException);
  }
}
