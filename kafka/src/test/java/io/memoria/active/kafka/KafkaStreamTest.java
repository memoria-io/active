package io.memoria.active.kafka;

import io.memoria.active.core.stream.Msg;
import io.memoria.active.core.stream.MsgResult;
import io.vavr.collection.Stream;
import io.vavr.control.Try;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamTest {
  private static final int count = 1000;
  private final String topic = "commands_" + System.currentTimeMillis();
  private final int partition = 0;
  private final KafkaStream stream = new KafkaStream(Infra.producerConfigs(),
                                                     Infra.consumerConfigs(),
                                                     Duration.ofMillis(1000));

  @Test
  void stream() {
    Thread.ofVirtual().start(this::publish);
    stream.stream(topic, partition).get().take(count - 10).forEach(MsgResult::ack);
    stream.stream(topic, partition, false).get().take(10).forEach(MsgResult::ack);
  }

  private void publish() {
    Stream.range(0, count)
          .map(String::valueOf)
          .map(i -> new Msg(i, i))
          .map(msg -> stream.publish(topic, partition, msg))
          .forEach(KafkaStreamTest::assertSuccess);
  }

  private static void assertSuccess(Try<?> result) {
    assertThat(result.isSuccess()).isTrue();
  }
}
