package io.memoria.active.cassandra;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

public class CassandraUtils {

  private CassandraUtils() {}

  public static SimpleStatement createKeyspace(String keyspace, int replication) {
    return SchemaBuilder.createKeyspace(keyspace).ifNotExists().withSimpleStrategy(replication).build();
  }

  public static SimpleStatement truncate(String keyspace, String table) {
    return QueryBuilder.truncate(keyspace, table).build();
  }
}
