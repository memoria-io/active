package io.memoria.active.etcd;

import io.etcd.jetcd.Client;
import io.vavr.collection.List;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class EtcdKVStoreTest {
  private final static Random random = new Random();
  private final static String keyPrefix = "key_" + random.nextInt(1000);
  private final Client client = Client.builder().endpoints("http://localhost:2379").build();
  private final EtcdKVStore kvStore = new EtcdKVStore(client, Duration.ofMillis(500));

  @Test
  void getAndPut() {
    // Given
    int count = 100;

    // When
    var setKV = List.range(0, count).map(i -> kvStore.set(toKey(i), toValue(i))).map(Try::get).toJavaList();
    var getKV = List.range(0, count).flatMap(i -> kvStore.get(toKey(i))).map(Option::get).toJavaList();

    // Then
    assertThat(setKV).hasSize(count);

    var expectedValues = List.range(0, count).map(EtcdKVStoreTest::toValue).toJavaList();
    assertThat(getKV).hasSameElementsAs(expectedValues);
  }

  @Test
  void notFound() {
    assertThat(kvStore.get("some_value").get().isEmpty()).isTrue();
  }

  private static String toKey(int i) {
    return keyPrefix + "_" + i;
  }

  private static String toValue(int i) {
    return "value:" + i;
  }
}
