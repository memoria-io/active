package io.memoria.reactive.eventsourcing.pipeline;

public record CommandRoute(String topicName, int partition, int totalPartitions) {
  public CommandRoute(String name, int partition) {
    this(name, partition, 1);
  }

  public CommandRoute {
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
