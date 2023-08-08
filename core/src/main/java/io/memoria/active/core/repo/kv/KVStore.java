package io.memoria.active.core.repo.kv;

import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.Map;

public interface KVStore {
  Try<Option<String>> get(String key);

  Try<String> set(String key, String value);

  static KVStore inMemory() {
    return new MemKVStore();
  }

  static KVStore inMemory(Map<String, String> store) {
    return new MemKVStore(store);
  }
}
