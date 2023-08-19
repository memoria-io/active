package io.memoria.active.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;

public class CassandraAdmin {
  private final CqlSession session;

  public CassandraAdmin(CqlSession session) {
    this.session = session;
  }

  public boolean truncate(String keyspace, String table) {
    var st = Statements.truncate(keyspace, table);
    return session.execute(st).wasApplied();
  }

  public boolean createKeyspace(String keyspace, int replicationFactor) {
    var st = Statements.createEventsKeyspace(keyspace, replicationFactor);
    return session.execute(st).wasApplied();
  }

  public boolean createTopicTable(String keyspace, String topic) {
    var st = Statements.createEventsTable(keyspace, topic);
    return session.execute(st).wasApplied();
  }
}
