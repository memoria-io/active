package io.memoria.active.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import io.memoria.active.cassandra.event.EventStatements;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Objects;

@TestMethodOrder(value = OrderAnnotation.class)
class CassandraUtilsIT {
  private static final String KEYSPACE = "some_space";
  private static final CqlSession session = Infra.cqlSession();

  @BeforeAll
  static void beforeAll() {
    // Check connection
    ResultSet rs = session.execute("select release_version from system.local");
    Row row = rs.one();
    var version = Objects.requireNonNull(row).getString("release_version");
    assert version != null && !version.isEmpty();
  }

  @Test
  @Order(0)
  void createKeyspace() {
    // Create table
    var st = CassandraUtils.createKeyspace(KEYSPACE, 1);
    assert session.execute(st).wasApplied();
  }

  @Test
  @Order(1)
  void truncateTable() {
    String tableName = "SOME_TABLE";
    var creationSt = SchemaBuilder.createTable(KEYSPACE, tableName)
                                  .ifNotExists()
                                  .withPartitionKey("PARTITION_KEY_COL", EventStatements.PARTITION_KEY_COL_TYPE)
                                  .build();
    assert session.execute(creationSt).wasApplied();
    var truncateSt = CassandraUtils.truncate(KEYSPACE, tableName);
    assert session.execute(truncateSt).wasApplied();
  }
}
