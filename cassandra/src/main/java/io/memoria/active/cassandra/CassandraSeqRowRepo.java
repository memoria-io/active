package io.memoria.active.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import io.memoria.active.core.repo.seq.SeqRow;
import io.memoria.active.core.repo.seq.SeqRowRepo;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.stream.StreamSupport;

import static com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_QUORUM;

/**
 * EventRepo's secondary/driven adapter of cassandra
 */
public class CassandraSeqRowRepo implements SeqRowRepo {
  private final CqlSession session;
  private final ConsistencyLevel writeConsistency;
  private final ConsistencyLevel readConsistency;
  private final String keyspace;
  private final String table;

  /**
   * Using LOCAL_QUORUM as default for read and write consistency
   */
  public CassandraSeqRowRepo(CqlSession session, String keyspace, String table) {
    this(session, LOCAL_QUORUM, LOCAL_QUORUM, keyspace, table);
  }

  public CassandraSeqRowRepo(CqlSession session,
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
  public Try<SeqRow> append(SeqRow row) {
    return Try.of(() -> appendESRow(row));
  }

  @Override
  public Try<Stream<SeqRow>> stream(String aggId) {
    return Try.of(() -> streamRows(aggId));
  }

  @Override
  public Try<Integer> size(String aggId) {
    return Try.of(() -> {
      var st = CassandraUtils.size(keyspace, table, aggId);
      return Option.of(session.execute(st).one()).map(r -> r.getInt(0)).getOrElse(0);
    });
  }

  @Override
  public void close() {
    session.close();
  }

  Stream<SeqRow> streamRows(String aggId) {
    var st = CassandraUtils.get(keyspace, table, aggId, 0).setConsistencyLevel(readConsistency);
    var result = session.execute(st);
    var stream = StreamSupport.stream(result.spliterator(), false);
    return Stream.ofAll(stream).map(CassandraUtils::toCassandraRow).map(CassandraUtils::toSeqRow);
  }

  SeqRow appendESRow(SeqRow esRow) {
    var row = new CassandraRow(esRow.aggId(), esRow.seqId(), esRow.value());
    var statement = CassandraUtils.push(keyspace, table, row).setConsistencyLevel(writeConsistency);
    var result = session.execute(statement);
    if (result.wasApplied()) {
      return esRow;
    } else {
      throw FailedAppend.of(keyspace, table, esRow);
    }
  }
}
