package io.memoria.reactive.testsuite;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import io.memoria.active.cassandra.CassandraAdmin;
import io.memoria.active.cassandra.CassandraRepo;
import io.memoria.active.core.repo.seq.SeqRowRepo;
import io.memoria.atom.core.caching.KVCache;
import io.memoria.atom.core.id.Id;
import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.testsuite.eventsourcing.banking.AccountDecider;
import io.memoria.atom.testsuite.eventsourcing.banking.AccountEvolver;
import io.memoria.atom.testsuite.eventsourcing.banking.AccountSaga;
import io.memoria.atom.testsuite.eventsourcing.banking.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.banking.event.AccountEvent;
import io.memoria.atom.testsuite.eventsourcing.banking.state.Account;
import io.memoria.reactive.eventsourcing.pipeline.AggregatePool;
import io.memoria.reactive.eventsourcing.repo.EventRepo;
import io.memoria.reactive.eventsourcing.repo.MemCommandPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.function.Supplier;

public class Infra {
  private static final Logger log = LoggerFactory.getLogger(Infra.class.getName());
  public static final String keyspace = "event_sourcing";
  public static final MemCommandPublisher<AccountCommand> commandPublisher = new MemCommandPublisher<>();

  public static AggregatePool<Account, AccountCommand, AccountEvent> aggregatePool(Supplier<Id> idSupplier,
                                                                                   Supplier<Long> timeSupplier) {
    var transformer = new SerializableTransformer();
    var eventRepo = new EventRepo<>(seqRowRepo(), AccountEvent.class, transformer);
    // Pipeline
    var domain = domain(idSupplier, timeSupplier);
    return new AggregatePool<>(domain, eventRepo, commandPublisher, KVCache.inMemory(1000));
  }

  public static SeqRowRepo seqRowRepo() {
    String eventsTable = "events" + System.currentTimeMillis();
    var admin = new CassandraAdmin(cqlSession());
    admin.createKeyspace(keyspace, 1);
    admin.createTopicTable(keyspace, eventsTable);
    log.info("Keyspace %s created".formatted(keyspace));
    return new CassandraRepo(cqlSession(), keyspace, eventsTable);
  }

  public static CqlSession cqlSession() {
    return session("datacenter1", "localhost", 9042).build();
  }

  public static CqlSessionBuilder session(String datacenter, String ip, int port) {
    var sock = InetSocketAddress.createUnresolved(ip, port);
    return CqlSession.builder().withLocalDatacenter(datacenter).addContactPoint(sock);
  }

  public static Domain<Account, AccountCommand, AccountEvent> domain(Supplier<Id> idSupplier,
                                                                     Supplier<Long> timeSupplier) {
    return new Domain<>(Account.class,
                        AccountCommand.class,
                        AccountEvent.class,
                        new AccountDecider(idSupplier, timeSupplier),
                        new AccountEvolver(),
                        new AccountSaga(idSupplier, timeSupplier));
  }

  public static void printRates(String methodName, long now, long msgCount) {
    long totalElapsed = System.currentTimeMillis() - now;
    log.info("{}: Finished processing {} events, in {} millis %n", methodName, msgCount, totalElapsed);
    log.info("{}: Average {} events per second %n", methodName, (long) eventsPerSec(msgCount, totalElapsed));
  }

  private static double eventsPerSec(long msgCount, long totalElapsed) {
    return msgCount / (totalElapsed / 1000d);
  }

  private Infra() {}
}
