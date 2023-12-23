package io.memoria.active.core.repo.stack;

import io.vavr.collection.List;
import io.vavr.control.Try;

public interface StackRepo extends AutoCloseable {
  Try<StackItem> append(StackItem row);

  Try<List<StackItem>> fetch(StackId stackId);

  Try<Integer> size(StackId stackId);

  static StackRepo inMemory() {
    return new MemStackAdapter();
  }
}
