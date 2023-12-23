package io.memoria.active.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import io.memoria.active.core.repo.stack.StackItem;
import io.vavr.collection.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(OrderAnnotation.class)
class CassandraRepoIT {
  private static final String KEYSPACE = "eventsourcing";
  private static final String TABLE = "events2" + System.currentTimeMillis();
  private static final String AGG_ID = "aggId";
  private static final CqlSession session = Infra.cqlSession();
  private static final CassandraAdmin admin = new CassandraAdmin(session);
  private static final CassandraRepo repo = new CassandraRepo(session, KEYSPACE, TABLE);
  private static final int COUNT = 100;
  private static final List<StackItem> rows = List.range(0, COUNT).map(i -> new StackItem(AGG_ID, i, String.valueOf(i)));

  @BeforeAll
  static void beforeAll() {
    // Create Keyspace
    var keyspaceCreated = admin.createKeyspace(KEYSPACE, 1);
    assert keyspaceCreated;
    // Create Table
    var tableCreated = admin.createTopicTable(KEYSPACE, TABLE);
    assert tableCreated;
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
    var result = repo.fetch(AGG_ID).get().toJavaList();

    // Then
    Assertions.assertThat(result).containsExactlyElementsOf(rows);
  }

  @Test
  @Order(2)
  void size() {
    // When
    var result = repo.size(AGG_ID).get();

    // Then
    Assertions.assertThat(result).isEqualTo(COUNT);
  }
}
