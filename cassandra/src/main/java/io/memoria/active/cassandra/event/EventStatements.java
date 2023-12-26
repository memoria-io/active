package io.memoria.active.cassandra.event;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

public class EventStatements {
  // partition key (e.g stateId)
  public static final String PARTITION_KEY_COL = "partition_key_col";
  public static final DataType PARTITION_KEY_COL_TYPE = DataTypes.TEXT;
  // cluster key (e.g event version)
  public static final String CLUSTER_KEY_COL = "cluster_key_col";
  public static final DataType CLUSTER_KEY_COL_TYPE = DataTypes.INT;
  // Payload
  public static final String PAYLOAD_COL = "payload";
  public static final DataType PAYLOAD_COL_TYPE = DataTypes.TEXT;
  // CreatedAt
  public static final String CREATED_AT_COL = "created_at";
  public static final DataType CREATE_AT_COL_TYPE = DataTypes.BIGINT;

  public static SimpleStatement createTable(String keyspace, String table) {
    return SchemaBuilder.createTable(keyspace, table)
                        .ifNotExists()
                        .withPartitionKey(PARTITION_KEY_COL, PARTITION_KEY_COL_TYPE)
                        .withClusteringColumn(CLUSTER_KEY_COL, CLUSTER_KEY_COL_TYPE)
                        .withColumn(PAYLOAD_COL, PAYLOAD_COL_TYPE)
                        .withColumn(CREATED_AT_COL, CREATE_AT_COL_TYPE)
                        .build();
  }

  public static SimpleStatement push(String keyspace,
                                     String table,
                                     String partitionKey,
                                     long clusterKey,
                                     String payload) {
    long createdAt = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    return QueryBuilder.insertInto(keyspace, table)
                       .value(PARTITION_KEY_COL, literal(partitionKey))
                       .value(CLUSTER_KEY_COL, literal(clusterKey))
                       .value(PAYLOAD_COL, literal(payload))
                       .value(CREATED_AT_COL, literal(createdAt))
                       .ifNotExists()
                       .build();
  }

  public static SimpleStatement fetchAll(String keyspace, String table, String partitionKey, int startIdx) {
    return QueryBuilder.selectFrom(keyspace, table)
                       .all()
                       .whereColumn(PARTITION_KEY_COL)
                       .isEqualTo(literal(partitionKey))
                       .whereColumn(CLUSTER_KEY_COL)
                       .isGreaterThanOrEqualTo(literal(startIdx))
                       .build();
  }

  public static SimpleStatement getFirst(String keyspace, String table, String partitionKey) {
    return QueryBuilder.selectFrom(keyspace, table)
                       .all()
                       .whereColumn(PARTITION_KEY_COL)
                       .isEqualTo(literal(partitionKey))
                       .whereColumn(CLUSTER_KEY_COL)
                       .isEqualTo(literal(0))
                       .build();
  }

  public static SimpleStatement getLast(String keyspace, String table, String partitionKey) {
    return QueryBuilder.selectFrom(keyspace, table)
                       .all()
                       .whereColumn(PARTITION_KEY_COL)
                       .isEqualTo(literal(partitionKey))
                       .whereColumn(CLUSTER_KEY_COL)
                       .isGreaterThanOrEqualTo(literal(0))
                       .orderBy(CLUSTER_KEY_COL, ClusteringOrder.DESC)
                       .limit(1)
                       .build();
  }

  public static SimpleStatement size(String keyspace, String table, String partitionKey) {
    return QueryBuilder.selectFrom(keyspace, table)
                       .countAll()
                       .whereColumn(PARTITION_KEY_COL)
                       .isEqualTo(literal(partitionKey))
                       .build();
  }

  private EventStatements() {}
}
