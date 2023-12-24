package io.memoria.active.core.queue;

import io.memoria.atom.core.id.Id;

import java.util.UUID;

public class QueueId extends Id {
  public QueueId(String value) {
    super(value);
  }

  public QueueId(long value) {
    super(value);
  }

  public QueueId(UUID uuid) {
    super(uuid);
  }

  public QueueId(Id id) {
    super(id);
  }
}
