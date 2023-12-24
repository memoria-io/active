package io.memoria.active.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import io.memoria.active.core.queue.QueueId;
import io.memoria.active.core.queue.QueueItem;
import io.memoria.active.core.queue.QueueRepo;
import io.vavr.collection.List;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.stream.StreamSupport;

import static com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_QUORUM;

/**
 * Secondary/driven adapter of cassandra
 */
public class CassandraQueueRepo implements QueueRepo {
  private final CqlSession session;
  private final ConsistencyLevel writeConsistency;
  private final ConsistencyLevel readConsistency;
  private final String keyspace;
  private final String table;

  /**
   * Using LOCAL_QUORUM as default for read and write consistency
   */
  public CassandraQueueRepo(CqlSession session, String keyspace, String table) {
    this(session, LOCAL_QUORUM, LOCAL_QUORUM, keyspace, table);
  }

  public CassandraQueueRepo(CqlSession session,
                            ConsistencyLevel writeConsistency,
                            ConsistencyLevel readConsistency,
                            String keyspace,
                            String table) {
    this.session = session;
    this.writeConsistency = writeConsistency;
    this.readConsistency = readConsistency;
    this.keyspace = keyspace;
    this.table = table;
  }

  @Override
  public Try<QueueItem> append(QueueItem row) {
    return Try.of(() -> appendESRow(row));
  }

  @Override
  public Try<List<QueueItem>> fetch(QueueId queueId) {
    return Try.of(() -> streamRows(queueId));
  }

  @Override
  public Try<Integer> size(QueueId queueId) {
    return Try.of(() -> {
      var st = CassandraUtils.size(keyspace, table, queueId.value());
      var size = Option.of(session.execute(st).one()).map(r -> r.getLong(0)).getOrElse(0L);
      return size.intValue();
    });
  }

  List<QueueItem> streamRows(QueueId queueId) {
    var st = CassandraUtils.get(keyspace, table, queueId.value(), 0).setConsistencyLevel(readConsistency);
    var result = session.execute(st);
    var stream = StreamSupport.stream(result.spliterator(), false);
    return List.ofAll(stream).map(CassandraUtils::toCassandraRow).map(CassandraUtils::toSeqRow);
  }

  QueueItem appendESRow(QueueItem esRow) {
    var row = new CassandraRow(esRow.queueId().value(), esRow.itemIndex(), esRow.value());
    var statement = CassandraUtils.push(keyspace, table, row).setConsistencyLevel(writeConsistency);
    var result = session.execute(statement);
    if (result.wasApplied()) {
      return esRow;
    } else {
      throw FailedAppend.of(keyspace, table, esRow);
    }
  }
}
