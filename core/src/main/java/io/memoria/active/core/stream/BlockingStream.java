package io.memoria.active.core.stream;

public interface BlockingStream extends BlockingStreamPublisher, BlockingStreamSubscriber {
  static BlockingStream inMemory() {
    return new MemBlockingStream();
  }
}
