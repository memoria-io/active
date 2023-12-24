package io.memoria.active.core.queue;

import io.vavr.collection.List;
import io.vavr.control.Try;

public interface QueueRepo {
  Try<QueueItem> append(QueueItem row);

  Try<List<QueueItem>> fetch(QueueId queueId);

  Try<Integer> size(QueueId queueId);

  static QueueRepo inMemory() {
    return new MemQueueAdapter();
  }
}
