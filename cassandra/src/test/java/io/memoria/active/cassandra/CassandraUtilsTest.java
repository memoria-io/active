package io.memoria.active.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
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
class CassandraUtilsTest {
  private static final String KEYSPACE = "eventsourcing";
  private static final String TABLE = "events" + System.currentTimeMillis();
  private static final String AGG_ID = "aggId";
  private static final CqlSession session = Infra.CqlSession();
  private static final int COUNT = 100;

  @BeforeAll
  static void beforeAll() {
    // Check connection
    ResultSet rs = session.execute("select release_version from system.local");
    Row row = rs.one();
    var version = Objects.requireNonNull(row).getString("release_version");
    assert version != null && !version.isEmpty();

    // Create namespace
    var st = CassandraUtils.createEventsKeyspace(KEYSPACE, 1);
    var keyspaceCreated = session.execute(st).wasApplied();
    assert keyspaceCreated;

    // Create table
    var tableCreated = session.execute(CassandraUtils.createEventsTable(KEYSPACE, TABLE)).wasApplied();
    assert tableCreated;
  }

  @Test
  @Order(0)
  void push() {
    // When
    var statements = List.range(0, COUNT).map(i -> CassandraUtils.push(KEYSPACE, TABLE, createRow(i)));
    // Then
    Assertions.assertThatCode(() -> statements.flatMap(session::execute)).doesNotThrowAnyException();
  }

  @Test
  @Order(1)
  void getAll() {
    // When
    var rs = session.execute(CassandraUtils.get(KEYSPACE, TABLE, AGG_ID, 0));
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
    var rows = session.execute(CassandraUtils.get(KEYSPACE, TABLE, AGG_ID, startIdx))
                      .map(CassandraUtils::toCassandraRow);
    // Then
    Assertions.assertThat(Stream.ofAll(rows).size()).isEqualTo(COUNT - startIdx);
  }

  @Test
  void getLast() {
    // When
    var lastSeq = session.execute(CassandraUtils.getLast(KEYSPACE, TABLE, AGG_ID))
                         .map(CassandraUtils::toCassandraRow)
                         .map(CassandraRow::seqId)
                         .one();
    // Then
    Assertions.assertThat(lastSeq).isEqualTo(COUNT - 1);
  }

  @Test
  void getLastButUnknown() {
    // Given
    var st = CassandraUtils.getLast(KEYSPACE, TABLE, "unknown");
    // When
    var spliterator = session.execute(st).map(CassandraUtils::toCassandraRow).spliterator();
    var count = StreamSupport.stream(spliterator, false).count();
    // Then
    Assertions.assertThat(count).isZero();
  }

  private static CassandraRow createRow(int i) {
    return new CassandraRow(AGG_ID, i, "{some event happened here}");
  }
}
