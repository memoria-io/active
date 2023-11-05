package io.memoria.active.nats;

import io.memoria.atom.core.stream.Msg;
import io.nats.client.JetStreamApiException;
import io.vavr.collection.Stream;
import io.vavr.control.Try;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.time.Duration;

import static io.memoria.active.nats.Infra.NATS_CONFIG;

@TestMethodOrder(OrderAnnotation.class)
class NatsStreamTest {
  private static final int count = 100;
  private static final String topic = "commands_" + System.currentTimeMillis();
  private static final int partition = 0;
  private static final NatsStream stream;
  private static boolean await = false;

  static {
    try {
      stream = new NatsStream(NATS_CONFIG, Duration.ofMillis(500));
      NatsUtils.createOrUpdateTopic(NATS_CONFIG, topic, 1);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

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
  void stream() {
    var size = stream.fetch(topic, partition).get().take(count).size();
    Assertions.assertThat(size).isEqualTo(count);
  }
}
