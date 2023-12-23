package io.memoria.active.cassandra;

import io.memoria.active.core.repo.stack.StackItem;

public class FailedAppend extends IllegalArgumentException {
  private static final String MSG = "Event with SeqId:%s append operation wasn't applied in "
                                    + "Keyspace:%s, Table:%s, StateId:%s";

  private FailedAppend(String keyspace, String table, String stateId, int seqId) {
    super(MSG.formatted(seqId, keyspace, table, stateId));
  }

  public static FailedAppend of(String keyspace, String table, String stateId, int seqId) {
    return new FailedAppend(keyspace, table, stateId, seqId);
  }

  public static FailedAppend of(String keyspace, String table, StackItem esRow) {
    return FailedAppend.of(keyspace, table, esRow.stackId(), esRow.index());
  }
}