package io.memoria.active.kafka;

import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Option;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;

public class Utils {
  private Utils() {}

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
}
