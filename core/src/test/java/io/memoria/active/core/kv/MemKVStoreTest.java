package io.memoria.active.core.kv;

import io.vavr.collection.List;
import io.vavr.control.Option;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MemKVStoreTest {
  private final KVStore kvStore = KVStore.inMemory();

  @Test
  void getAndPut() {
    // Given
    int count = 1000;

    // When
    var setKV = List.range(0, count).flatMap(i -> kvStore.set(toKey(i), toValue(i))).toJavaList();
    var getKV = List.range(0, count).flatMap(i -> kvStore.get(toKey(i))).map(Option::get).toJavaList();

    // Then
    var expectedValues = List.range(0, count).map(MemKVStoreTest::toValue).toJavaList();
    assertThat(setKV).hasSize(count);
    assertThat(getKV).hasSameElementsAs(expectedValues);
  }

  private static String toKey(Integer i) {
    return "key:" + i;
  }

  private static String toValue(Integer i) {
    return "value:" + i;
  }
}
