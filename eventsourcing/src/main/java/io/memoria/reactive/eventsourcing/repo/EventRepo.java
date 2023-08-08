package io.memoria.reactive.eventsourcing.repo;

import io.memoria.active.core.repo.seq.SeqRow;
import io.memoria.active.core.repo.seq.SeqRowRepo;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Event;
import io.vavr.collection.Stream;
import io.vavr.control.Try;

public class EventRepo<E extends Event> {
  private final SeqRowRepo repo;
  private final Class<E> eClass;
  private final TextTransformer transformer;

  public EventRepo(SeqRowRepo repo, Class<E> eClass, TextTransformer transformer) {
    this.repo = repo;
    this.eClass = eClass;
    this.transformer = transformer;
  }

  public Try<E> append(String aggId, int seqId, E e) {
    return repo.size(aggId).flatMap(size -> toRow(seqId, e)).flatMap(repo::append).map(row -> e);
  }

  public Stream<Try<E>> fetch(String aggId) {
    return repo.stream(aggId).map(tr -> tr.flatMap(this::toEvent));
  }

  public Try<Integer> size(String aggId) {
    return repo.size(aggId);
  }

  private Try<E> toEvent(SeqRow row) {
    return transformer.deserialize(row.value(), eClass);
  }

  Try<SeqRow> toRow(int seqId, E event) {
    return transformer.serialize(event).map(eventStr -> new SeqRow(event.meta().stateId().id().value(), seqId, eventStr));
  }
}

