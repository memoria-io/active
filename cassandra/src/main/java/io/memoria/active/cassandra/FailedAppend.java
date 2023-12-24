package io.memoria.active.cassandra;

import io.memoria.active.core.queue.QueueItem;

public class FailedAppend extends IllegalArgumentException {
  private static final String MSG = "Stack item with clustering key:%s append operation wasn't applied in "
                                    + "Keyspace:%s, Table:%s, Partition:%s";

  private FailedAppend(String keyspace, String table, String partitionKey, int clusterKey) {
    super(MSG.formatted(clusterKey, keyspace, table, partitionKey));
  }

  public static FailedAppend of(String keyspace, String table, String partitionKey, int clusterKey) {
    return new FailedAppend(keyspace, table, partitionKey, clusterKey);
  }

  public static FailedAppend of(String keyspace, String table, QueueItem esRow) {
    return FailedAppend.of(keyspace, table, esRow.queueId().value(), esRow.itemIndex());
  }
}