package io.memoria.reactive.eventsourcing.pipeline;

public record CommandRoute(String topic, int partition, int totalPartitions) {}
