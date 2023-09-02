package io.memoria.active.kafka;

import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

class Infra {
  private Infra() {}

  /**
   * Acknowledgement only means committing the offset in Kafka for a certain consumer group, unlike nats which stops
   * consumption if explicit ack is set.
   * <a href="https://stackoverflow.com/a/59846269/263215"> kafka committing stackoverflow answer</a>
   */
  public static Map<String, Object> consumerConfigs() {
    return HashMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                      "localhost:9092",
                      ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                      10,
                      ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                      IsolationLevel.READ_COMMITTED.toString().toLowerCase(),
                      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                      true,
                      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                      "earliest",
                      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                      StringDeserializer.class,
                      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                      StringDeserializer.class,
                      ConsumerConfig.GROUP_ID_CONFIG,
                      "some_group_id1");
  }

  public static Map<String, Object> producerConfigs() {
    return HashMap.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                      "localhost:9092",
                      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                      false,
                      ProducerConfig.MAX_BLOCK_MS_CONFIG,
                      1000,
                      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                      StringSerializer.class,
                      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                      StringSerializer.class);
  }
}
