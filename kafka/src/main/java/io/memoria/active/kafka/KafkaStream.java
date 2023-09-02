package io.memoria.active.kafka;

import io.memoria.active.core.stream.BlockingStream;
import io.memoria.active.core.stream.Msg;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import io.vavr.control.Try;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.memoria.active.kafka.KafkaUtils.toRecord;

public class KafkaStream implements BlockingStream {
  public final Map<String, Object> producerConfig;
  public final Map<String, Object> consumerConfig;
  private final Duration timeout;
  private final KafkaProducer<String, String> kafkaProducer;
  private final List<KafkaConsumer<String, String>> kafkaConsumers;

  public KafkaStream(Map<String, Object> producerConfig, Map<String, Object> consumerConfig, Duration timeout) {
    this.producerConfig = producerConfig;
    this.consumerConfig = consumerConfig;
    this.timeout = timeout;
    this.kafkaProducer = new KafkaProducer<>(producerConfig.toJavaMap());
    this.kafkaConsumers = new ArrayList<>();
  }

  @Override
  public Try<Msg> publish(String topic, int partition, Msg msg) {
    return Try.of(() -> send(topic, partition, msg)).map(meta -> msg);
  }

  @Override
  public Try<Stream<Msg>> fetch(String topic, int partition) {
    return Try.of(() -> consume(topic, partition));
  }

  @Override
  public void close() {
    this.kafkaProducer.close();
    this.kafkaConsumers.forEach(KafkaConsumer::close);
  }

  private RecordMetadata send(String topic, int partition, Msg msg)
          throws InterruptedException, ExecutionException, TimeoutException {
    return this.kafkaProducer.send(toRecord(topic, partition, msg)).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  private Stream<Msg> consume(String topic, int partition) {
    var consumer = new KafkaConsumer<String, String>(consumerConfig.toJavaMap());
    this.kafkaConsumers.add(consumer);
    var tp = new TopicPartition(topic, partition);
    var tpCol = io.vavr.collection.List.of(tp).toJavaList();
    consumer.assign(tpCol);
    consumer.seekToBeginning(tpCol);
    return KafkaUtils.consume(consumer, tp, timeout).map(KafkaUtils::toMsg);
  }
}
