package io.memoria.active.cassandra.event;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import io.memoria.active.cassandra.CassandraUtils;
import io.memoria.active.cassandra.Infra;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Objects;
import java.util.stream.StreamSupport;

@TestMethodOrder(value = OrderAnnotation.class)
class EventStatementsIT {
  private static final String KEYSPACE = "eventsourcing";
  private static final String TABLE = "events" + System.currentTimeMillis();
  private static final String PARTITION_KEY = "aggId";
  private static final CqlSession session = Infra.cqlSession();
  private static final int COUNT = 100;

  @BeforeAll
  static void beforeAll() {
    // Check connection
    ResultSet rs = session.execute("select release_version from system.local");
    Row row = rs.one();
    var version = Objects.requireNonNull(row).getString("release_version");
    assert version != null && !version.isEmpty();

    // Create namespace
    var st = CassandraUtils.createKeyspace(KEYSPACE, 1);
    var keyspaceCreated = session.execute(st).wasApplied();
    assert keyspaceCreated;

    // Create table
    var tableCreated = session.execute(EventStatements.createTable(KEYSPACE, TABLE)).wasApplied();
    assert tableCreated;
  }

  @Test
  @Order(0)
  void push() {
    // When
    var statements = List.range(0, COUNT)
                         .map(i -> EventStatements.push(KEYSPACE, TABLE, PARTITION_KEY, i, "hello world"));
    // Then
    Assertions.assertThatCode(() -> statements.flatMap(session::execute)).doesNotThrowAnyException();
  }

  @Test
  @Order(1)
  void fetchAll() {
    // When
    var rs = session.execute(EventStatements.fetchAll(KEYSPACE, TABLE, PARTITION_KEY, 0));
    var rows = StreamSupport.stream(rs.spliterator(), false);
    // Then
    Assertions.assertThat(rows.count()).isEqualTo(COUNT);
  }

  @Test
  @Order(2)
  void getWithOffset() {
    // Given
    int startIdx = 2;
    // When
    var rows = session.execute(EventStatements.fetchAll(KEYSPACE, TABLE, PARTITION_KEY, startIdx));
    // Then
    Assertions.assertThat(Stream.ofAll(rows).size()).isEqualTo(COUNT - startIdx);
  }

  @Test
  void getLast() {
    // When
    var lastSeq = session.execute(EventStatements.getLast(KEYSPACE, TABLE, PARTITION_KEY))
                         .map(row -> row.getInt(EventStatements.clusterKeyCol))
                         .one();
    // Then
    Assertions.assertThat(lastSeq).isEqualTo(COUNT - 1);
  }

  @Test
  void getLastButUnknown() {
    // Given
    var st = EventStatements.getLast(KEYSPACE, TABLE, "unknown");
    // When
    var spliterator = session.execute(st).spliterator();
    var count = StreamSupport.stream(spliterator, false).count();
    // Then
    Assertions.assertThat(count).isZero();
  }
}
