package io.memoria.active.core.queue;

import java.io.Serializable;

public record QueueItem(QueueId queueId, int itemIndex, String value) implements Serializable {}
