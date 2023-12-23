package io.memoria.active.core.repo.stack;

import io.memoria.atom.core.id.Id;

import java.util.UUID;

public class StackItemId extends Id {
  public StackItemId(String value) {
    super(value);
  }

  public StackItemId(long value) {
    super(value);
  }

  public StackItemId(UUID uuid) {
    super(uuid);
  }

  public StackItemId(Id id) {
    super(id);
  }
}
