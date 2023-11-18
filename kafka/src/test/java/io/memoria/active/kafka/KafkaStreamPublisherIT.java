package io.memoria.active.kafka;

import io.memoria.atom.core.stream.Msg;
import io.vavr.collection.Stream;
import io.vavr.control.Try;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.time.Duration;

@TestMethodOrder(OrderAnnotation.class)
class KafkaStreamPublisherIT {
  private static final int count = 100;
  private static final String topic = "commands_" + System.currentTimeMillis();
  private static final int partition = 0;
  private static final KafkaStreamPublisher stream = new KafkaStreamPublisher(Infra.producerConfigs(),
                                                                              Duration.ofMillis(1000));
  private static boolean await = false;

  @Test
  @Order(0)
  void publishing() {
    await = Stream.range(0, count)
                  .map(String::valueOf)
                  .map(i -> new Msg(i, i))
                  .map(msg -> stream.publish(topic, partition, msg))
                  .map(Try::isSuccess)
                  .forAll(b -> b);
    Awaitility.await().timeout(Duration.ofSeconds(3)).until(() -> await);
  }

  @Test
  @Order(1)
  void size() {
    var size = KafkaUtils.topicSize(topic, partition, Infra.consumerConfigs());
    Assertions.assertThat(size).isEqualTo(count);
  }
}