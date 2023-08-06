package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.State;
import io.memoria.reactive.eventsourcing.repo.EventRepo;
import io.vavr.collection.Stream;
import io.vavr.control.Try;

import java.util.concurrent.atomic.AtomicReference;

public class Aggregate<S extends State, C extends Command, E extends Event> {
  private final String topic;
  private final String aggId;
  private final Domain<S, C, E> domain;
  private final AtomicReference<S> state;
  private final EventRepo<E> eventRepo;

  public Aggregate(String topic, String aggId, Domain<S, C, E> domain, EventRepo<E> eventRepo) {
    this.topic = topic;
    this.aggId = aggId;
    this.domain = domain;
    this.state = new AtomicReference<>();
    this.eventRepo = eventRepo;
  }

  public Stream<Try<E>> init() {
    eventRepo.fetch(topic, aggId).map(tr -> tr)
  }

  public E apply(C cmd) {

  }

  Try<E> evolve(E e) {
    S currentState = this.state.get();
    var isSuccess = false;
    if (currentState == null) {
      var newState = domain.evolver().apply(e);
      isSuccess = this.state.compareAndSet(null, newState);
    } else {
      var newState = domain.evolver().apply(currentState, e);
      isSuccess = this.state.compareAndSet(currentState, newState);
    }
    if (isSuccess) {
      return Try.success(e);
    } else {
      return Try.failure(new IllegalArgumentException("Corrupted current State" + state));
    }
  }
}
