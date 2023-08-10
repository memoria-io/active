package io.memoria.active.kafka;

import io.memoria.active.core.stream.BlockingStream;
import io.memoria.active.core.stream.Msg;
import io.memoria.active.core.stream.MsgResult;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import io.vavr.control.Try;

import java.time.Duration;

public class KafkaStream implements BlockingStream {
  public final Map<String, Object> producerConfig;
  public final Map<String, Object> consumerConfig;
  private final Duration timeout;

  public KafkaStream(Map<String, Object> producerConfig, Map<String, Object> consumerConfig, Duration timeout) {
    this.producerConfig = producerConfig;
    this.consumerConfig = consumerConfig;
    this.timeout = timeout;
  }

  @Override
  public Try<Msg> append(String topic, int partition, Msg msg) {
    return null;
  }

  @Override
  public Stream<Try<MsgResult>> stream(String topic, int partition) {
    return null;
  }

  @Override
  public void close() throws Exception {

  }
}
