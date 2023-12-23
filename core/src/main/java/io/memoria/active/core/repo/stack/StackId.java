package io.memoria.active.core.repo.stack;

import io.memoria.atom.core.id.Id;

import java.util.UUID;

public class StackId extends Id {
  public StackId(String value) {
    super(value);
  }

  public StackId(long value) {
    super(value);
  }

  public StackId(UUID uuid) {
    super(uuid);
  }

  public StackId(Id id) {
    super(id);
  }
}
