package io.memoria.reactive.eventsourcing.pipeline;

public record EventRoute(String topicName, int partition, int totalPartitions) {
  public EventRoute(String topicName, int partition) {
    this(topicName, partition, 1);
  }

  public EventRoute {
    if (topicName == null || topicName.isEmpty()) {
      throw new IllegalArgumentException("Topic name can't be null or empty string");
    }
    if (partition < 0) {
      throw new IllegalArgumentException("Partition can't be less than 0");
    }
    if (totalPartitions < 1) {
      throw new IllegalArgumentException("Command Total partitions can't be less than 1");
    }
  }
}
