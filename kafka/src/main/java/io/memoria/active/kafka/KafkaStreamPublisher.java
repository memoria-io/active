package io.memoria.active.kafka;

import io.memoria.atom.core.stream.BlockingStreamPublisher;
import io.memoria.atom.core.stream.Msg;
import io.vavr.collection.Map;
import io.vavr.control.Try;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.memoria.active.kafka.KafkaUtils.toRecord;

public class KafkaStreamPublisher implements BlockingStreamPublisher {
  public final Map<String, Object> producerConfig;
  private final Duration timeout;
  private final KafkaProducer<String, String> kafkaProducer;

  public KafkaStreamPublisher(Map<String, Object> producerConfig, Duration timeout) {
    this.producerConfig = producerConfig;
    this.timeout = timeout;
    this.kafkaProducer = new KafkaProducer<>(producerConfig.toJavaMap());
  }

  @Override
  public Try<Msg> publish(String topic, int partition, Msg msg) {
    return Try.of(() -> send(topic, partition, msg)).map(meta -> msg);
  }

  @Override
  public void close() {
    this.kafkaProducer.close();
  }

  private RecordMetadata send(String topic, int partition, Msg msg)
          throws InterruptedException, ExecutionException, TimeoutException {
    return this.kafkaProducer.send(toRecord(topic, partition, msg)).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }
}
