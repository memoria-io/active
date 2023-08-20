package io.memoria.active.cassandra;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import io.memoria.active.core.repo.seq.SeqRow;

import java.util.Objects;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

class Utils {

  private Utils() {}

  public static SimpleStatement push(String keyspace, String table, CassandraRow row) {
    return QueryBuilder.insertInto(keyspace, table)
                       .value(CassandraRow.stateIdCol, literal(row.stateId()))
                       .value(CassandraRow.seqCol, literal(row.seqId()))
                       .value(CassandraRow.payloadCol, literal(row.payload()))
                       .value(CassandraRow.createdAtCol, literal(row.createdAt()))
                       .ifNotExists()
                       .build();
  }

  public static SimpleStatement getLastRow(String keyspace, String table, String stateId) {
    return QueryBuilder.selectFrom(keyspace, table)
                       .all()
                       .whereColumn(CassandraRow.stateIdCol)
                       .isEqualTo(literal(stateId))
                       .whereColumn(CassandraRow.seqCol)
                       .isGreaterThanOrEqualTo(literal(0))
                       .orderBy(CassandraRow.seqCol, ClusteringOrder.DESC)
                       .limit(1)
                       .build();
  }

  public static SimpleStatement size(String keyspace, String table, String stateId) {
    return QueryBuilder.selectFrom(keyspace, table)
                       .countAll()
                       .whereColumn(CassandraRow.stateIdCol)
                       .isEqualTo(literal(stateId))
                       .build();
  }

  public static SimpleStatement get(String keyspace, String table, String stateId, int startIdx) {
    return QueryBuilder.selectFrom(keyspace, table)
                       .all()
                       .whereColumn(CassandraRow.stateIdCol)
                       .isEqualTo(literal(stateId))
                       .whereColumn(CassandraRow.seqCol)
                       .isGreaterThanOrEqualTo(literal(startIdx))
                       .build();
  }

  public static SimpleStatement getFirst(String keyspace, String table, String stateId) {
    return QueryBuilder.selectFrom(keyspace, table)
                       .all()
                       .whereColumn(CassandraRow.stateIdCol)
                       .isEqualTo(literal(stateId))
                       .whereColumn(CassandraRow.seqCol)
                       .isEqualTo(literal(0))
                       .build();
  }

  public static SimpleStatement createEventsKeyspace(String keyspace, int replication) {
    return SchemaBuilder.createKeyspace(keyspace).ifNotExists().withSimpleStrategy(replication).build();
  }

  public static SimpleStatement truncate(String keyspace, String table) {
    return QueryBuilder.truncate(keyspace, table).build();
  }

  public static SimpleStatement createEventsTable(String keyspace, String table) {
    return SchemaBuilder.createTable(keyspace, table)
                        .ifNotExists()
                        .withPartitionKey(CassandraRow.stateIdCol, CassandraRow.stateIdColType)
                        .withClusteringColumn(CassandraRow.seqCol, CassandraRow.seqColType)
                        .withColumn(CassandraRow.payloadCol, CassandraRow.payloadColType)
                        .withColumn(CassandraRow.createdAtCol, CassandraRow.createAtColType)
                        .build();
  }

  public static CassandraRow toCassandraRow(Row row) {
    var rStateId = Objects.requireNonNull(row.getString(CassandraRow.stateIdCol));
    var rSeqId = row.getInt(CassandraRow.seqCol);
    var rCreatedAt = row.getLong(CassandraRow.createdAtCol);
    var rEvent = Objects.requireNonNull(row.getString(CassandraRow.payloadCol));
    return new CassandraRow(rStateId, rSeqId, rEvent, rCreatedAt);
  }

  public static SeqRow toSeqRow(CassandraRow r) {
    return new SeqRow(r.stateId(), r.seqId(), r.payload());
  }
}
