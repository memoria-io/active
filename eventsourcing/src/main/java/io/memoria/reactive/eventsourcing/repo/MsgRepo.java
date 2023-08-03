package io.memoria.reactive.eventsourcing.repo;

import io.vavr.control.Try;

public interface MsgRepo extends AutoCloseable {

  /**
   * @return known key
   */
  Try<Msg> last(String topic, int partition, long seqId);

  Try<Msg> pub(String topic, int partition, Msg msg);

  Try<Msg> sub(String topic, int partition);

  /**
   * @return an in memory ESStream
   */
  static MsgRepo inMemory() {
    return new MemMsgRepo();
  }

  static MsgRepo inMemory(int historySize) {
    return new MemMsgRepo(historySize);
  }
}
