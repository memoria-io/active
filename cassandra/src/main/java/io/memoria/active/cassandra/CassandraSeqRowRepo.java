package io.memoria.active.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import io.memoria.active.core.repo.seq.SeqRow;
import io.memoria.active.core.repo.seq.SeqRowRepo;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.Objects;
import java.util.stream.StreamSupport;

/**
 * EventRepo's secondary/driven adapter of cassandra
 */
public class CassandraSeqRowRepo implements SeqRowRepo {
  private final CqlSession session;
  private final String keyspace;
  private final String table;

  public CassandraSeqRowRepo(CqlSession session, String keyspace, String table) {
    this.session = session;
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
      var st = Statements.get(keyspace, table, aggId, 0);
      return Option.of(session.execute(st).one()).map(r -> r.getInt(0)).getOrElse(0);
    });
  }

  @Override
  public void close() {
    session.close();
  }

  Stream<SeqRow> streamRows(String aggId) {
    var st = Statements.get(keyspace, table, aggId, 0);
    var result = session.execute(st);
    var stream = StreamSupport.stream(result.spliterator(), false);
    return Stream.ofAll(stream).map(CassandraSeqRowRepo::from).map(this::toESRepoRow);
  }

  SeqRow appendESRow(SeqRow esRow) {
    var row = new CassandraRow(esRow.aggId(), esRow.seqId(), esRow.value());
    var result = session.execute(Statements.push(keyspace, table, row));
    if (result.wasApplied()) {
      return esRow;
    } else {
      throw FailedAppend.of(keyspace, table, esRow);
    }
  }

  private SeqRow toESRepoRow(CassandraRow r) {
    return new SeqRow(r.stateId(), r.seqId(), r.payload());
  }

  public static CassandraRow from(Row row) {
    var rStateId = Objects.requireNonNull(row.getString(CassandraRow.stateIdCol));
    var rSeqId = row.getInt(CassandraRow.seqCol);
    var rCreatedAt = row.getLong(CassandraRow.createdAtCol);
    var rEvent = Objects.requireNonNull(row.getString(CassandraRow.payloadCol));
    return new CassandraRow(rStateId, rSeqId, rEvent, rCreatedAt);
  }
}
