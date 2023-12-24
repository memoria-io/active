package io.memoria.active.cassandra;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

record CassandraRow(String stackId, int itemIndex, String payload, long createdAt) {
  // partition key (e.g stateId)
  static final String partitionKeyCol = "partition_key_col";
  static final DataType partitionKeyColType = DataTypes.TEXT;
  // cluster key (e.g index)
  static final String clusterKeyCol = "cluster_key_col";
  static final DataType clusterKeyColType = DataTypes.INT;
  // Value
  static final String payloadCol = "payload";
  static final DataType payloadColType = DataTypes.TEXT;
  // CreatedAt
  static final String createdAtCol = "created_at";
  static final DataType createAtColType = DataTypes.BIGINT;

  public CassandraRow {
    if (itemIndex < 0)
      throw new IllegalArgumentException("Seq can't be less than zero!.");
  }

  public CassandraRow(String partitionKey, int seq, String payload) {
    this(partitionKey, seq, payload, LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
  }
}
