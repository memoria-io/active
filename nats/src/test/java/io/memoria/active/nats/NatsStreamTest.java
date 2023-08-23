package io.memoria.active.nats;

import io.memoria.active.core.stream.Msg;
import io.memoria.active.core.stream.MsgResult;
import io.nats.client.JetStreamApiException;
import io.vavr.collection.Stream;
import io.vavr.control.Try;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

import static io.memoria.active.nats.Infra.NATS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

class NatsStreamTest {
  private static final int count = 1000;
  private final String topic = "commands_" + System.currentTimeMillis();
  private final int partition = 0;
  private final NatsStream stream = new NatsStream(NATS_CONFIG, Duration.ofMillis(500));

  NatsStreamTest() throws IOException, InterruptedException, JetStreamApiException {
    NatsUtils.createOrUpdateTopic(NATS_CONFIG, topic, 1);
  }

  @Test
  void stream() {
    Thread.ofVirtual().start(this::publish);
    stream.fetch(topic, partition).get().take(count - 10).forEach(MsgResult::ack);
    stream.fetch(topic, partition, false).get().take(10).forEach(MsgResult::ack);
  }

  private void publish() {
    Stream.range(0, count)
          .map(String::valueOf)
          .map(i -> new Msg(i, i))
          .map(msg -> stream.publish(topic, partition, msg).onFailure(Throwable::printStackTrace))
          .forEach(NatsStreamTest::assertSuccess);
  }

  private static void assertSuccess(Try<?> result) {
    assertThat(result.isSuccess()).isTrue();
  }
}
