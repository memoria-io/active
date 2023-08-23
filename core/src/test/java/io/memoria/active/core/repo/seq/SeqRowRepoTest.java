package io.memoria.active.core.repo.seq;

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
class SeqRowRepoTest {
  private static final int count = 1000;
  private static final String agg01 = "agg01";
  private static final String agg02 = "agg02";
  private static final SeqRowRepo repo = SeqRowRepo.inMemory();

  @Test
  @Order(0)
  void append() {
    List.range(0, count)
        .map(i -> repo.append(createSeqRow(agg01, i)))
        .forEach(tr -> assertThat(tr.isSuccess()).isTrue());
    List.range(0, count)
        .map(i -> repo.append(createSeqRow(agg02, i)))
        .forEach(tr -> assertThat(tr.isSuccess()).isTrue());
  }

  @Order(1)
  @ParameterizedTest
  @ValueSource(strings = {agg01, agg02})
  void stream(String agg) {
    AtomicInteger idx = new AtomicInteger(0);
    for (SeqRow seqRow : repo.fetch(agg).get()) {
      assertThat(seqRow.seqId()).isEqualTo(idx.getAndIncrement());
      assertThat(seqRow.aggId()).isEqualTo(agg);
    }
  }

  private static SeqRow createSeqRow(String agg, int i) {
    return new SeqRow(agg, i, "hello_" + i);
  }
}
