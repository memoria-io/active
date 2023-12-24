package io.memoria.active.cassandra.event;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import io.memoria.active.cassandra.FailedAppend;
import io.memoria.active.eventsourcing.EventRepo;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.StateId;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.Objects;

import static com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_QUORUM;

public class CassandraEventRepo implements EventRepo {
  private final CqlSession session;
  private final ConsistencyLevel writeConsistency;
  private final ConsistencyLevel readConsistency;
  private final String keyspace;
  private final String table;
  private final TextTransformer transformer;

  public CassandraEventRepo(CqlSession session,
                            ConsistencyLevel writeConsistency,
                            ConsistencyLevel readConsistency,
                            String keyspace,
                            String table,
                            TextTransformer transformer) {
    this.session = session;
    this.writeConsistency = writeConsistency;
    this.readConsistency = readConsistency;
    this.keyspace = keyspace;
    this.table = table;
    this.transformer = transformer;
  }

  /**
   * Using LOCAL_QUORUM as default for read and write consistency
   */
  public CassandraEventRepo(CqlSession session, String keyspace, String table, TextTransformer transformer) {
    this(session, LOCAL_QUORUM, LOCAL_QUORUM, keyspace, table, transformer);
  }

  @Override
  public Try<Event> append(Event event) {
    return Try.of(() -> appendEvent(event));
  }

  @Override
  public Try<List<Event>> fetch(StateId stateId) {
    return Try.of(() -> fetchEvents(stateId.value()));
  }

  @Override
  public Try<Stream<Event>> stream(StateId stateId) {
    return Try.of(() -> streamEvents(stateId.value()));
  }

  @Override
  public Try<Long> size(StateId stateId) {
    return Try.of(() -> {
      var st = EventStatements.size(keyspace, table, stateId.value());
      return Option.of(session.execute(st).one()).map(r -> r.getLong(0)).getOrElse(0L);
    });
  }

  private Event appendEvent(Event event) {
    var payload = transformer.serialize(event).get();
    var st = EventStatements.push(keyspace, table, event.shardKey().value(), event.version(), payload)
                            .setConsistencyLevel(writeConsistency);
    var result = session.execute(st);
    if (result.wasApplied()) {
      return event;
    } else {
      throw FailedAppend.of(keyspace, table, event);
    }
  }

  private List<Event> fetchEvents(String partitionKey) {
    var st = EventStatements.fetchAll(keyspace, table, partitionKey, 0);
    var result = session.execute(st);
    return List.ofAll(result.all()).map(this::toEvent).map(Try::get);
  }

  private Stream<Event> streamEvents(String partitionKey) {
    var st = EventStatements.fetchAll(keyspace, table, partitionKey, 0);
    var result = session.execute(st);
    return Stream.ofAll(result).map(this::toEvent).map(Try::get);
  }

  private Try<Event> toEvent(Row row) {
    var payload = Objects.requireNonNull(row.getString(EventStatements.payloadCol));
    return transformer.deserialize(payload, Event.class);
  }
}
