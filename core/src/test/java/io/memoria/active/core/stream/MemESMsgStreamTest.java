package io.memoria.active.core.stream;

import io.memoria.atom.core.id.Id;
import io.vavr.collection.List;
import io.vavr.control.Try;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class MemESMsgStreamTest {
  private static final Duration timeout = Duration.ofSeconds(5);
  private static final int ELEMENTS_SIZE = 1000;
  private static final String topic = "some_topic";
  private static final Id S0 = Id.of(0);
  private static final Id S1 = Id.of(1);
  private static final int TOTAL_PARTITIONS = 2;

  private final ESMsgStream stream = new MemESMsgStream();

  @Test
  void publishAndSubscribe() {
    // Given
    var msgs = createMessages(S0).appendAll(createMessages(S1));

    // When
    assert msgs.map(stream::pub).forAll(Try::isSuccess);

    // Then
    verifyPartition(0);
    verifyPartition(1);
  }

  private void verifyPartition(int partition) {
    var idx = new AtomicInteger();
    stream.sub(topic, partition).take(ELEMENTS_SIZE).forEach(msg -> {
      assertThat(msg.partition()).isEqualTo(partition);
      assertThat(msg.key()).isEqualTo(String.valueOf(idx.getAndIncrement()));
    });
  }

  private List<ESMsg> createMessages(Id stateId) {
    return List.range(0, ELEMENTS_SIZE).map(i -> new ESMsg(topic, getPartition(stateId), String.valueOf(i), "hello"));
  }

  private static int getPartition(Id stateId) {
    return Integer.parseInt(stateId.value()) % TOTAL_PARTITIONS;
  }
}
