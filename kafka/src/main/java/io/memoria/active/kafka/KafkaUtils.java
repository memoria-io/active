package io.memoria.active.kafka;

import io.memoria.active.core.stream.Msg;
import io.vavr.Function1;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;

public class KafkaUtils {
  private KafkaUtils() {}

  public static Stream<ConsumerRecord<String, String>> consume(KafkaConsumer<String, String> consumer,
                                                               TopicPartition tp,
                                                               Duration timeout) {
    return Stream.continually(() -> Stream.ofAll(consumer.poll(timeout).records(tp))).flatMap(Function1.identity());
  }

  public static long topicSize(String topic, int partition, Map<String, Object> conf) {
    try (var consumer = new KafkaConsumer<String, String>(conf.toJavaMap())) {
      var tp = new TopicPartition(topic, partition);
      var tpCol = List.of(tp).toJavaList();
      consumer.assign(tpCol);
      consumer.seekToEnd(tpCol);
      return consumer.position(tp);
    }
  }

  public static Option<ConsumerRecord<String, String>> lastKey(String topic,
                                                               int partition,
                                                               Duration timeout,
                                                               Map<String, Object> conf) {
    try (var consumer = new KafkaConsumer<String, String>(conf.toJavaMap())) {
      var tp = new TopicPartition(topic, partition);
      var tpCol = List.of(tp).toJavaList();
      consumer.assign(tpCol);
      consumer.seekToEnd(tpCol);
      var position = consumer.position(tp);
      if (position < 1)
        return Option.none();
      long startIndex = position - 1;
      consumer.seek(tp, startIndex);
      var records = consumer.poll(timeout).records(tp);
      var size = records.size();
      if (size > 0) {
        return Option.of(records.get(size - 1));
      } else {
        return Option.none();
      }
    }
  }

  public static ProducerRecord<String, String> toRecord(String topic, int partition, Msg msg) {
    return new ProducerRecord<>(topic, partition, msg.key(), msg.value());
  }

  public static Msg toMsg(ConsumerRecord<String, String> consumerRecord) {
    return new Msg(consumerRecord.key(), consumerRecord.value());
  }

}
