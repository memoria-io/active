package io.memoria.active.core.queue;

import io.vavr.collection.List;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(value = OrderAnnotation.class)
class QueueRepoTest {
  private static final int count = 1000;
  private static final String stack01 = "agg01";
  private static final String stack02 = "agg02";
  private static final QueueRepo repo = QueueRepo.inMemory();

  @Test
  @Order(0)
  void append() {
    List.range(0, count)
        .map(i -> repo.append(createStackItem(stack01, i)))
        .forEach(tr -> assertThat(tr.isSuccess()).isTrue());
    List.range(0, count)
        .map(i -> repo.append(createStackItem(stack02, i)))
        .forEach(tr -> assertThat(tr.isSuccess()).isTrue());
  }

  @Order(1)
  @ParameterizedTest
  @ValueSource(strings = {stack01, stack02})
  void stream(String id) {
    AtomicInteger idx = new AtomicInteger(0);
    QueueId queueId = new QueueId(id);
    for (QueueItem queueItem : repo.fetch(queueId).get()) {
      assertThat(queueItem.itemIndex()).isEqualTo(idx.getAndIncrement());
      assertThat(queueItem.queueId()).isEqualTo(queueId);
    }
  }

  private static QueueItem createStackItem(String stackId, int i) {
    return new QueueItem(new QueueId(stackId), i, "hello_" + i);
  }
}
