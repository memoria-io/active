package io.memoria.active.core.repo.seq;

import io.vavr.collection.Stream;
import io.vavr.control.Try;

public interface SeqRowRepo extends AutoCloseable {
  Try<SeqRow> append(SeqRow row);

  Stream<Try<SeqRow>> stream(String aggId);

  Try<Integer> size(String aggId);

  static SeqRowRepo inMemory() {
    return new MemMsgRepo();
  }
}
