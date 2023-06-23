package io.memoria.active.core.stream;

import io.vavr.control.Try;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

class MemStreamTest {
  private static final int count = 10_000;
  private final MemStream<Integer> q = new MemStream<>();

  @Test
  @DisplayName("Stream single item")
  void streamOneElement() {
    Thread.startVirtualThread(() -> {
      for (int i = 0; i < 1; i++) {
        q.add(i);
      }
    });

    AtomicInteger idx = new AtomicInteger(0);
    q.stream().take(1).forEach(i -> {
      assert i == idx.getAndIncrement();
    });
  }

  @Test
  @DisplayName("Stream multiple items are in same order")
  void stream() {
    Thread.startVirtualThread(() -> {
      for (int i = 0; i < count; i++) {
        q.add(i);
      }
    });

    AtomicInteger idx = new AtomicInteger(0);
    q.stream().take(count).forEach(i -> {
      assert i == idx.getAndIncrement();
    });
  }

  @Test
  @DisplayName("Should block until tail is added")
  void tailBlocking() {
    Thread.startVirtualThread(() -> {
      try {
        Thread.sleep(200);
        q.add(1);
        q.add(2);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    Awaitility.await().timeout(Duration.ofMillis(250)).until(() -> q.stream().take(2).length() == 2);
  }
}
