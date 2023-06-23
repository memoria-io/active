package io.memoria.active.core.repo;

import io.vavr.control.Try;

import java.util.Map;

public interface KVStore {
  Try<String> get(String key);

  Try<String> set(String key, String value);

  static KVStore inMemory() {
    return new MemKVStore();
  }

  static KVStore inMemory(Map<String, String> store) {
    return new MemKVStore(store);
  }
}
