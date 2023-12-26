package io.memoria.active.cassandra;

import io.memoria.atom.eventsourcing.Event;

public class FailedAppend extends IllegalArgumentException {
  private static final String MSG = "Stack item with clustering key:%s append operation wasn't applied in "
                                    + "Keyspace:%s, Table:%s, Partition:%s";

  private FailedAppend(String keyspace, String table, String partitionKey, long clusterKey) {
    super(MSG.formatted(clusterKey, keyspace, table, partitionKey));
  }

  public static FailedAppend of(String keyspace, String table, String partitionKey, long clusterKey) {
    return new FailedAppend(keyspace, table, partitionKey, clusterKey);
  }

  public static FailedAppend of(String keyspace, String table, Event event) {
    return FailedAppend.of(keyspace, table, event.meta().stateId().value(), event.version());
  }
}