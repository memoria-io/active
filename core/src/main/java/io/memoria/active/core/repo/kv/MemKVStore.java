package io.memoria.active.core.repo.kv;

import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class MemKVStore implements KVStore {
  private final Map<String, String> store;

  public MemKVStore() {
    store = new ConcurrentHashMap<>();
  }

  public MemKVStore(Map<String, String> store) {
    this.store = store;
  }

  @Override
  public Try<String> get(String key) {
    return Option.of(store.get(key)).toTry();
  }

  @Override
  public Try<String> set(String key, String value) {
    return Try.of(() -> {
      store.computeIfPresent(key, (k, v) -> value);
      store.computeIfAbsent(key, k -> value);
      return value;
    });
  }
}
