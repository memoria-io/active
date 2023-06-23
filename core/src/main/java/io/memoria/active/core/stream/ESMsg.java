package io.memoria.active.core.stream;

public record ESMsg(String topic, int partition, String key, String value) {}
