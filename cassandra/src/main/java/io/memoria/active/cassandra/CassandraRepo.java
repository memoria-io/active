package io.memoria.active.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import io.memoria.active.core.repo.stack.StackId;
import io.memoria.active.core.repo.stack.StackItem;
import io.memoria.active.core.repo.stack.StackRepo;
import io.vavr.collection.List;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.stream.StreamSupport;

import static com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_QUORUM;

/**
 * EventRepo's secondary/driven adapter of cassandra
 */
public class CassandraRepo implements StackRepo {
  private final CqlSession session;
  private final ConsistencyLevel writeConsistency;
  private final ConsistencyLevel readConsistency;
  private final String keyspace;
  private final String table;

  /**
   * Using LOCAL_QUORUM as default for read and write consistency
   */
  public CassandraRepo(CqlSession session, String keyspace, String table) {
    this(session, LOCAL_QUORUM, LOCAL_QUORUM, keyspace, table);
  }

  public CassandraRepo(CqlSession session,
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
  public Try<StackItem> append(StackItem row) {
    return Try.of(() -> appendESRow(row));
  }

  @Override
  public Try<List<StackItem>> fetch(StackId stackId) {
    return Try.of(() -> streamRows(stackId));
  }

  @Override
  public Try<Integer> size(StackId stackId) {
    return Try.of(() -> {
      var st = CassandraUtils.size(keyspace, table, stackId);
      var size = Option.of(session.execute(st).one()).map(r -> r.getLong(0)).getOrElse(0L);
      return size.intValue();
    });
  }

  @Override
  public void close() {
    session.close();
  }

  List<StackItem> streamRows(StackId stackId) {
    var st = CassandraUtils.get(keyspace, table, stackId.value(), 0).setConsistencyLevel(readConsistency);
    var result = session.execute(st);
    var stream = StreamSupport.stream(result.spliterator(), false);
    return List.ofAll(stream).map(CassandraUtils::toCassandraRow).map(CassandraUtils::toSeqRow);
  }

  StackItem appendESRow(StackItem esRow) {
    var row = new CassandraRow(esRow.stackId().value(), esRow.index(), esRow.value());
    var statement = CassandraUtils.push(keyspace, table, row).setConsistencyLevel(writeConsistency);
    var result = session.execute(statement);
    if (result.wasApplied()) {
      return esRow;
    } else {
      throw FailedAppend.of(keyspace, table, esRow);
    }
  }
}
