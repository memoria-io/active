package io.memoria.active.eventsourcing;

import io.memoria.active.core.repo.stack.StackItem;
import io.memoria.active.core.repo.stack.StackRepo;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.StateId;
import io.vavr.collection.List;
import io.vavr.control.Try;

public class EventRepo {
  private final StackRepo repo;
  private final TextTransformer transformer;

  public EventRepo(StackRepo repo, TextTransformer transformer) {
    this.repo = repo;
    this.transformer = transformer;
  }

  public Try<Event> append(Event e, int seqId) {
    return toRow(seqId, e).flatMap(repo::append).map(_ -> e);
  }

  public Try<List<Event>> fetch(StateId stateId) {
    return repo.fetch(stateId.value()).map(tr -> tr.map(this::toEvent).map(Try::get));
  }

  private Try<Event> toEvent(StackItem row) {
    return transformer.deserialize(row.value(), Event.class);
  }

  private Try<StackItem> toRow(int seqId, Event event) {
    return transformer.serialize(event)
                      .map(eventStr -> new StackItem(event.meta().stateId().id().value(), seqId, eventStr));
  }
}

