package io.memoria.active.kafka;

import io.memoria.active.core.stream.Msg;
import io.memoria.active.core.stream.MsgResult;
import io.vavr.collection.Stream;
import io.vavr.control.Try;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;

class KafkaStreamTest {
  private static final int count = 1000;
  private final String topic = "commands_" + System.currentTimeMillis();
  private final int partition = 0;
  private final KafkaStream stream = new KafkaStream(Infra.producerConfigs(),
                                                     Infra.consumerConfigs(),
                                                     Duration.ofMillis(1000));
  private boolean await = false;

  @Test
  void stream() {
    Thread.ofVirtual().start(this::publish);
    stream.fetch(topic, partition).get().take(count - 10).forEach(MsgResult::ack);
    stream.fetch(topic, partition, false).get().take(10).forEach(MsgResult::ack);
    Awaitility.await().timeout(Duration.ofMillis(200)).until(() -> await);
  }

  private void publish() {
    await = Stream.range(0, count)
                  .map(String::valueOf)
                  .map(i -> new Msg(i, i))
                  .map(msg -> stream.publish(topic, partition, msg))
                  .map(Try::isSuccess)
                  .forAll(b -> b);
  }
}
