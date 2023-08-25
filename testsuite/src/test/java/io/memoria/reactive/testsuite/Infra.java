package io.memoria.reactive.testsuite;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import io.memoria.active.cassandra.CassandraAdmin;
import io.memoria.active.cassandra.CassandraRepo;
import io.memoria.active.core.repo.seq.SeqRowRepo;
import io.memoria.active.core.stream.BlockingStream;
import io.memoria.active.kafka.KafkaStream;
import io.memoria.active.nats.NatsConfig;
import io.memoria.active.nats.NatsStream;
import io.memoria.atom.core.caching.KCache;
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
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.repo.EventRepo;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.nats.client.api.StorageType;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.function.Supplier;

public class Infra {
  private static final Logger log = LoggerFactory.getLogger(Infra.class.getName());
  public static final Duration TIMEOUT = Duration.ofMillis(500);
  public static final String keyspace = "event_sourcing";


  public enum StreamType {
    KAFKA,
    NATS,
    MEMORY
  }

  public static final String NATS_URL = "nats://localhost:4222";
  public static final NatsConfig NATS_CONFIG = NatsConfig.appendOnly(NATS_URL,
                                                                     StorageType.File,
                                                                     1,
                                                                     1000,
                                                                     Duration.ofMillis(100),
                                                                     Duration.ofMillis(300));

  public static PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline(Supplier<Id> idSupplier,
                                                                                  Supplier<Long> timeSupplier,
                                                                                  StreamType streamType,
                                                                                  CommandRoute commandRoute)
          throws IOException, InterruptedException {

    var transformer = new SerializableTransformer();

    // Stream
    var msgStream = msgStream(streamType);
    var commandStream = new CommandStream<>(msgStream, AccountCommand.class, transformer);
    var eventRepo = new EventRepo<>(seqRowRepo(), AccountEvent.class, transformer);

    // Pipeline
    var domain = domain(idSupplier, timeSupplier);
    return new PartitionPipeline<>(domain,
                                   eventRepo,
                                   commandRoute,
                                   commandStream,
                                   KVCache.inMemory(1000),
                                   () -> KCache.inMemory(1000));
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

  public static BlockingStream msgStream(StreamType streamType) throws IOException, InterruptedException {
    return switch (streamType) {
      case KAFKA -> new KafkaStream(kafkaProducerConfigs(), kafkaConsumerConfigs(), Duration.ofMillis(500));
      case NATS -> new NatsStream(NATS_CONFIG, Duration.ofMillis(100));
      case MEMORY -> BlockingStream.inMemory();
    };
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

  public static String commandTopic(String postfix) {
    return "topic%d_%s".formatted(System.currentTimeMillis(), postfix);
  }

  public static void printRates(String methodName, long now, long msgCount) {
    long totalElapsed = System.currentTimeMillis() - now;
    log.info("{}: Finished processing {} events, in {} millis %n", methodName, msgCount, totalElapsed);
    log.info("{}: Average {} events per second %n", methodName, (long) eventsPerSec(msgCount, totalElapsed));
  }

  public static Map<String, Object> kafkaConsumerConfigs() {
    return HashMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                      "localhost:9092",
                      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                      false,
                      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                      "earliest",
                      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                      StringDeserializer.class,
                      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                      StringDeserializer.class,
                      ConsumerConfig.GROUP_ID_CONFIG,
                      "some_group_id1");
  }

  public static Map<String, Object> kafkaProducerConfigs() {
    return HashMap.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                      "localhost:9092",
                      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                      false,
                      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                      StringSerializer.class,
                      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                      StringSerializer.class);
  }

  private static double eventsPerSec(long msgCount, long totalElapsed) {
    return msgCount / (totalElapsed / 1000d);
  }

  private Infra() {}
}
