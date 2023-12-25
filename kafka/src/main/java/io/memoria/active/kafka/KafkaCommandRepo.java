package io.memoria.active.kafka;

import io.memoria.active.eventsourcing.CommandRepo;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Command;
import io.vavr.Function1;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import io.vavr.control.Try;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaCommandRepo implements CommandRepo {
  private final KafkaProducer<String, String> producer;
  private final Map<String, Object> consumerConfig;
  private final Duration timeout;
  private final String topic;
  private final int totalPartitions;
  private final TextTransformer transformer;

  public KafkaCommandRepo(Map<String, Object> producerConfig,
                          Map<String, Object> consumerConfig,
                          Duration timeout,
                          String topic,
                          int totalPartitions,
                          TextTransformer transformer) {
    this.producer = new KafkaProducer<>(producerConfig.toJavaMap());
    this.consumerConfig = consumerConfig;
    if (topic == null || topic.isEmpty()) {
      throw new IllegalArgumentException("Topic can't be null or empty");
    }
    if (totalPartitions < 1) {
      throw new IllegalArgumentException("Partitions count can't be less than 1");
    }
    this.timeout = timeout;
    this.topic = topic;
    this.totalPartitions = totalPartitions;
    this.transformer = transformer;
  }

  @Override
  public Try<Command> publish(Command command) {
    return Try.of(() -> send(topic, command)).map(_ -> command);
  }

  @SuppressWarnings("resource")
  @Override
  public Stream<Try<Command>> stream() {
    var consumer = new KafkaConsumer<String, String>(consumerConfig.toJavaMap());
      consumer.subscribe(List.of(topic));
      consumer.seekToBeginning(consumer.assignment());
      return Stream.continually(() -> Stream.ofAll(consumer.poll(timeout)))
                   .flatMap(Function1.identity())

                   .map(this::toCommand);

  }

  private RecordMetadata send(String topic, Command command)
          throws ExecutionException, InterruptedException, TimeoutException {
    var commandId = command.meta().commandId().value();
    var partition = command.partition(totalPartitions);
    // Using get only safe when this method is private and only used inside Try.of(()-> ...)
    var payload = transformer.serialize(command).get();
    var record = new ProducerRecord<>(topic, partition, commandId, payload);
    return producer.send(record).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  private Try<Command> toCommand(ConsumerRecord<String, String> consumerRecord) {
    return transformer.deserialize(consumerRecord.value(), Command.class);
  }
}
