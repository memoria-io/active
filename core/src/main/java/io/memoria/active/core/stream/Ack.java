package io.memoria.active.core.stream;

public interface Ack {
  Runnable acknowledge();

  default void ack() {
    Thread.startVirtualThread(acknowledge()).start();
  }

  static Ack of(Runnable acknowledge) {
    return new DefaultAck(acknowledge);
  }
}
