package io.memoria.active.kafka;

import io.memoria.active.core.stream.Msg;
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
    var msgs = Stream.range(0, count).map(String::valueOf).map(i -> new Msg(i, i));
    msgs.map(msg -> stream.append(topic, partition, msg))
        .map(this::print)
        .forEach(result -> assertThat(result.isSuccess()).isTrue());
    var now = System.currentTimeMillis();
    stream.stream(topic, partition).take(count-10).forEach(tr -> tr.forEach(result -> {
      System.out.println(result.msg());
            result.ack();
    }));
    System.out.println("-----------------------------");
    stream.stream(topic, partition, false).take(10).forEach(tr -> tr.forEach(result -> {
      System.out.println(result.msg());
      //      result.ack();
    }));
    System.out.println(System.currentTimeMillis() - now);
  }

  private Try<Msg> print(Try<Msg> msg) {
    System.out.println("Published:" + msg);
    return msg;
  }
}
