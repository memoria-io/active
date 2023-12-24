package io.memoria.active.cassandra;

import io.memoria.active.core.queue.QueueId;
import io.memoria.active.core.queue.QueueItem;
import io.memoria.active.core.queue.QueueRepo;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.StateId;
import io.vavr.collection.List;
import io.vavr.control.Try;

public class CassandraEventRepo {
  private final QueueRepo repo;
  private final TextTransformer transformer;

  public CassandraEventRepo(QueueRepo repo, TextTransformer transformer) {
    this.repo = repo;
    this.transformer = transformer;
  }

  public Try<Event> append(Event e, int seqId) {
    return toRow(seqId, e).flatMap(repo::append).map(_ -> e);
  }

  public Try<List<Event>> fetch(StateId stateId) {
    return repo.fetch(new QueueId(stateId)).map(tr -> tr.map(this::toEvent).map(Try::get));
  }

  private Try<Event> toEvent(QueueItem row) {
    return transformer.deserialize(row.value(), Event.class);
  }

  private Try<QueueItem> toRow(int seqId, Event event) {
    return transformer.serialize(event)
                      .map(eventStr -> new QueueItem(new QueueId(event.meta().stateId()), seqId, eventStr));
  }
}

