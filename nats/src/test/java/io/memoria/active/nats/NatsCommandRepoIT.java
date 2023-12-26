package io.memoria.active.nats;

import io.memoria.active.eventsourcing.CommandRepo;
import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.CommandMeta;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.command.CreateAccount;
import io.memoria.atom.testsuite.eventsourcing.command.Credit;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.vavr.collection.Stream;
import io.vavr.control.Try;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

@TestMethodOrder(OrderAnnotation.class)
class NatsCommandRepoIT {
  private static final String NATS_URL = "nats://localhost:4222";
  private static final String topic = "commands_" + System.currentTimeMillis();
  private static final int totalPartitions = 1;
  private static final int count = 1000;
  private static final Connection nc;
  private static final CommandRepo stream;

  static {
    try {
      nc = NatsUtils.createConnection(NATS_URL);
      stream = new NatsCommandRepo(nc, topic, totalPartitions, new SerializableTransformer());
      NatsUtils.createOrUpdateStream(nc.jetStreamManagement(), topic, 1);
      //    var names = nc.jetStreamManagement().getStreamNames();
      //    System.out.println(names);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  @Order(0)
  void publishing() {
    var await = Stream.range(0, count)
                      .map(String::valueOf)
                      .map(NatsCommandRepoIT::createCommand)
                      .map(stream::publish)
                      .map(Try::isSuccess)
                      .forAll(b -> b);
    Awaitility.await().timeout(Duration.ofSeconds(10)).until(() -> await);
  }

  @Test
  @Order(1)
  void stream() throws InterruptedException {
    var latch = new CountDownLatch(count);
    Thread.startVirtualThread(() -> stream.stream().forEach(m -> {
      //      System.out.println("First: " + m);
      latch.countDown();
    }));

    Thread.startVirtualThread(() -> stream.stream().forEach(m -> {
      //      System.out.println("Second: " + m);
      latch.countDown();
    }));
    latch.await();
  }

  private static CreateAccount createCommand(String stateId) {
    var meta = new CommandMeta(CommandId.of(UUID.randomUUID()), StateId.of(stateId));
    return new CreateAccount(meta, stateId, 500);
  }

  private static Credit addBalance(String stateId) {
    var meta = new CommandMeta(CommandId.of(UUID.randomUUID()), StateId.of(stateId));
    return new Credit(meta, StateId.of("the_bank"), 500);
  }
}
