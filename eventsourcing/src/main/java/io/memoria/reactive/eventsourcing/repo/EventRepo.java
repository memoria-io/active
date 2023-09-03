package io.memoria.reactive.eventsourcing.repo;

import io.memoria.active.core.repo.seq.SeqRow;
import io.memoria.active.core.repo.seq.SeqRowRepo;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.StateId;
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

  public Try<E> append(E e, int seqId) {
    return repo.size(e.meta().stateId().value()).flatMap(size -> toRow(seqId, e)).flatMap(repo::append).map(row -> e);
  }

  public Try<Stream<E>> fetch(StateId stateId) {
    return repo.fetch(stateId.value()).map(tr -> tr.map(this::toEvent).map(Try::get));
  }

  private Try<E> toEvent(SeqRow row) {
    return transformer.deserialize(row.value(), eClass);
  }

  Try<SeqRow> toRow(int seqId, E event) {
    return transformer.serialize(event)
                      .map(eventStr -> new SeqRow(event.meta().stateId().id().value(), seqId, eventStr));
  }
}

