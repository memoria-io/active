package io.memoria.active.cassandra.event;

import com.datastax.oss.driver.api.core.CqlSession;
import io.memoria.active.cassandra.CassandraUtils;
import io.memoria.active.cassandra.Infra;
import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.EventId;
import io.memoria.atom.eventsourcing.EventMeta;
import io.memoria.atom.eventsourcing.StateId;
import io.vavr.collection.List;
import io.vavr.control.Try;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(OrderAnnotation.class)
class CassandraEventRepoIT {
  private static final String KEYSPACE = "eventsourcing";
  private static final String TABLE = "events2" + System.currentTimeMillis();
  private static final StateId stateId = new StateId("aggId");
  private static final CqlSession session = Infra.cqlSession();
  private static final CassandraEventRepo repo = new CassandraEventRepo(session,
                                                                        KEYSPACE,
                                                                        TABLE,
                                                                        new SerializableTransformer());
  private static final int COUNT = 100;
  private static final List<Event> rows = List.range(0, COUNT).map(SomeEvent::new);

  @BeforeAll
  static void beforeAll() {
    assert session.execute(CassandraUtils.createKeyspace(KEYSPACE, 1)).wasApplied();
    assert session.execute(EventStatements.createTable(KEYSPACE, TABLE)).wasApplied();
  }

  @Test
  @Order(0)
  void append() {
    // When
    var result = rows.map(repo::append);

    // Then
    result.forEach(r -> Assertions.assertThat(r.isSuccess()).isTrue());
  }

  @Test
  @Order(1)
  void stream() {
    // When
    var result = repo.fetch(stateId).toJavaList();

    // Then
    Assertions.assertThat(result).containsExactlyElementsOf(rows.map(Try::success));
  }

  @Test
  @Order(2)
  void size() {
    // When
    var result = repo.size(stateId).get();

    // Then
    Assertions.assertThat(result).isEqualTo(COUNT);
  }

  private record SomeEvent(EventMeta meta) implements Event {
    public SomeEvent(int i) {
      this(new EventMeta(EventId.of(i), i, stateId, CommandId.of(i)));
    }
  }
}
