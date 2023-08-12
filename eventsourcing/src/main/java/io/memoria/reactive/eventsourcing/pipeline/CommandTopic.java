package io.memoria.reactive.eventsourcing.pipeline;

public record CommandTopic(String name, int totalPartitions) {
  public CommandTopic {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Topic name can't be null or empty string");
    }
    if (totalPartitions < 1) {
      throw new IllegalArgumentException("Command Total partitions can't be less than 1");
    }
  }
}
