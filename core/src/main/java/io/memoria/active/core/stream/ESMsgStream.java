package io.memoria.active.core.stream;

import io.vavr.collection.Stream;
import io.vavr.control.Try;

public interface ESMsgStream {
  Try<ESMsg> pub(ESMsg esMsg);

  Stream<ESMsg> sub(String topic, int partition);

  /**
   * @return an in memory ESStream
   */
  static ESMsgStream inMemory() {
    return new MemESMsgStream();
  }
}
