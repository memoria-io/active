package io.memoria.active.kafka;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.CommandMeta;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.command.CreateAccount;
import io.memoria.atom.testsuite.eventsourcing.command.Credit;
import io.vavr.collection.Stream;
import io.vavr.control.Try;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.time.Duration;
import java.util.UUID;

@TestMethodOrder(OrderAnnotation.class)
class KafkaCommandRepoIT {
  private static final int count = 10000;
  private static final String topic = "commands_" + System.currentTimeMillis();
  private static final KafkaCommandRepo stream = new KafkaCommandRepo(Infra.producerConfigs(),
                                                                      Infra.consumerConfigs(),
                                                                      Duration.ofMillis(5000),
                                                                      topic,
                                                                      1,
                                                                      new SerializableTransformer());

  @Test
  @Order(0)
  void publishing() {
    var await = Stream.range(0, count)
                      .map(String::valueOf)
                      .map(KafkaCommandRepoIT::createCommand)
                      .map(stream::publish)
                      .map(Try::isSuccess)
                      .forAll(b -> b);
    Awaitility.await().timeout(Duration.ofSeconds(10)).until(() -> await);
  }

  @Test
  @Order(1)
  void stream() {
    var size = stream.stream().take(count).size();
    Assertions.assertThat(size).isEqualTo(count);
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