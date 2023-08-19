package io.memoria.active.core.stream;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class BlockingChainTest {
  private static final int count = 10_000;
  private final BlockingChain<Integer> stream = BlockingChain.inMemory();

  @Test
  @DisplayName("Stream single item")
  void streamOneElement() {
    Thread.startVirtualThread(() -> {
      for (int i = 0; i < 1; i++) {
        stream.append(i);
      }
    });

    AtomicInteger idx = new AtomicInteger(0);
    stream.stream().get().take(1).forEach(i -> {
      assertThat(i).isEqualTo(idx.getAndIncrement());
    });
  }

  @Test
  @DisplayName("Stream multiple items are in same order")
  void stream() {
    Thread.startVirtualThread(() -> {
      for (int i = 0; i < count; i++) {
        stream.append(i);
      }
    });

    AtomicInteger idx = new AtomicInteger(0);
    stream.stream().get().take(count).forEach(i -> {
      assertThat(i).isEqualTo(idx.getAndIncrement());
    });
  }

  @Test
  @DisplayName("Should block until tail is added")
  void tailBlocking() {
    Thread.startVirtualThread(() -> {
      try {
        Thread.sleep(200);
        stream.append(1);
        stream.append(2);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
    Awaitility.await().timeout(Duration.ofMillis(250)).until(() -> stream.stream().get().take(2).length() == 2);
  }
}
