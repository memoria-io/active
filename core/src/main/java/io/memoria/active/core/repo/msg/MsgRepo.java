package io.memoria.active.core.repo.msg;

import io.vavr.collection.List;
import io.vavr.control.Try;

public interface MsgRepo extends AutoCloseable {

  Try<Msg> append(String topic, Msg msg);

  Try<List<Msg>> getAll(String topic, String aggId);

  Try<Integer> size(String topic, String aggId);

  /**
   * @return an in memory ESStream
   */
  static MsgRepo inMemory() {
    return new MemMsgRepo();
  }
}
