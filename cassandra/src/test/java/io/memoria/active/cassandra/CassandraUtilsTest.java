package io.memoria.active.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import io.vavr.collection.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
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
    // Given
    var statements = List.range(0, COUNT).map(i -> CassandraUtils.push(KEYSPACE, TABLE, createRow(AGG_ID, i)));
    // When, Then
    Assertions.assertThatCode(() -> statements.flatMap(session::execute)).doesNotThrowAnyException();
  }

  @Test
  @Order(1)
  void getAll() {
    // Given previous push
    // When
    var rs = session.execute(CassandraUtils.get(KEYSPACE, TABLE, AGG_ID, 0));
    var rows = StreamSupport.stream(rs.spliterator(), false);
    // Then
    Assertions.assertThat(rows.count()).isEqualTo(COUNT);
  }

  @Test
  @Disabled
  void getWithOffset() {
    // Given
    int startIdx = 2;
    // Given
    var statements = List.range(0, COUNT).map(i -> CassandraUtils.push(KEYSPACE, TABLE, createRow(AGG_ID, i)));
    var isCreatedFlux = statements.flatMap(session::execute).map(Row::getFormattedContents);
    // When
    var row = session.execute(CassandraUtils.get(KEYSPACE, TABLE, AGG_ID, startIdx))
                     .map(CassandraUtils::toCassandraRow);
    // Then
    System.out.println(row);
  }

  @Test
  @Disabled
  void get() {
    // Given
    var statements = List.range(0, COUNT).map(i -> CassandraUtils.push(KEYSPACE, TABLE, createRow(AGG_ID, i)));
    var rowFlux = statements.flatMap(session::execute).map(Row::getFormattedContents);
    // When
    var lastSeq = session.execute(CassandraUtils.getLast(KEYSPACE, TABLE, AGG_ID))
                         .map(CassandraUtils::toCassandraRow)
                         .map(CassandraRow::seqId);
    // Then
    //    StepVerifier.create(lastSeq).expectNext(COUNT - 1).verifyComplete();
  }

  @Test
  @Disabled
  void getLastButUnknown() {
    // Given
    var st = CassandraUtils.getLast(KEYSPACE, TABLE, "unknown");
    // When
    var exec = session.execute(st).map(Row::getFormattedContents);
    // Then
    //    StepVerifier.create(exec).verifyComplete();
  }

  private static CassandraRow createRow(String stateId, int i) {
    return new CassandraRow(stateId, i, "{some event happened here}");
  }
}
