package io.memoria.active.cassandra;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

record CassandraRow(String stateId, int seqId, String payload, long createdAt) {
  // StateId
  static final String stateIdCol = "state_id";
  static final DataType stateIdColType = DataTypes.TEXT;
  // SeqId
  static final String seqCol = "seq_id";
  static final DataType seqColType = DataTypes.INT;
  // Value
  static final String payloadCol = "payload";
  static final DataType payloadColType = DataTypes.TEXT;
  // CreatedAt
  static final String createdAtCol = "created_at";
  static final DataType createAtColType = DataTypes.BIGINT;

  public CassandraRow {
    if (seqId < 0)
      throw new IllegalArgumentException("Seq can't be less than zero!.");
  }

  public CassandraRow(String stateId, int seq, String event) {
    this(stateId, seq, event, LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));
  }
}
