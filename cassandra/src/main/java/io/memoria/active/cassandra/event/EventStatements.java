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
  public static final String partitionKeyCol = "partition_key_col";
  public static final DataType partitionKeyColType = DataTypes.TEXT;
  // cluster key (e.g event version)
  public static final String clusterKeyCol = "cluster_key_col";
  public static final DataType clusterKeyColType = DataTypes.BIGINT;
  // Payload
  public static final String payloadCol = "payload";
  public static final DataType payloadColType = DataTypes.TEXT;
  // CreatedAt
  public static final String createdAtCol = "created_at";
  public static final DataType createAtColType = DataTypes.BIGINT;

  public static SimpleStatement createTable(String keyspace, String table) {
    return SchemaBuilder.createTable(keyspace, table)
                        .ifNotExists()
                        .withPartitionKey(partitionKeyCol, partitionKeyColType)
                        .withClusteringColumn(clusterKeyCol, clusterKeyColType)
                        .withColumn(payloadCol, payloadColType)
                        .withColumn(createdAtCol, createAtColType)
                        .build();
  }

  public static SimpleStatement push(String keyspace,
                                     String table,
                                     String partitionKey,
                                     long clusterKey,
                                     String payload) {
    long createdAt = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    return QueryBuilder.insertInto(keyspace, table)
                       .value(partitionKeyCol, literal(partitionKey))
                       .value(clusterKeyCol, literal(clusterKey))
                       .value(payloadCol, literal(payload))
                       .value(createdAtCol, literal(createdAt))
                       .ifNotExists()
                       .build();
  }

  public static SimpleStatement fetchAll(String keyspace, String table, String partitionKey, int startIdx) {
    return QueryBuilder.selectFrom(keyspace, table)
                       .all()
                       .whereColumn(partitionKeyCol)
                       .isEqualTo(literal(partitionKey))
                       .whereColumn(clusterKeyCol)
                       .isGreaterThanOrEqualTo(literal(startIdx))
                       .build();
  }

  public static SimpleStatement getFirst(String keyspace, String table, String partitionKey) {
    return QueryBuilder.selectFrom(keyspace, table)
                       .all()
                       .whereColumn(partitionKeyCol)
                       .isEqualTo(literal(partitionKey))
                       .whereColumn(clusterKeyCol)
                       .isEqualTo(literal(0))
                       .build();
  }

  public static SimpleStatement getLast(String keyspace, String table, String partitionKey) {
    return QueryBuilder.selectFrom(keyspace, table)
                       .all()
                       .whereColumn(partitionKeyCol)
                       .isEqualTo(literal(partitionKey))
                       .whereColumn(clusterKeyCol)
                       .isGreaterThanOrEqualTo(literal(0))
                       .orderBy(clusterKeyCol, ClusteringOrder.DESC)
                       .limit(1)
                       .build();
  }

  public static SimpleStatement size(String keyspace, String table, String partitionKey) {
    return QueryBuilder.selectFrom(keyspace, table)
                       .countAll()
                       .whereColumn(partitionKeyCol)
                       .isEqualTo(literal(partitionKey))
                       .build();
  }
}
