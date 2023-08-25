package io.memoria.active.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.memoria.active.core.repo.kv.KVStore;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EtcdKVStore implements KVStore {
  private final KV kv;
  private final Duration timeout;

  public EtcdKVStore(Client client, Duration timeout) {
    this.kv = client.getKVClient();
    this.timeout = timeout;
  }

  /**
   * @return first value of such key and ignores any other
   */
  @Override
  public Try<Option<String>> get(String key) {
    return Try.of(() -> getValue(key));
  }

  /**
   * @return the value which was set
   */
  @Override
  public Try<String> set(String key, String value) {
    return Try.of(() -> {
      kv.put(toByteSequence(key), toByteSequence(value)).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      return value;
    });
  }

  private Option<String> getValue(String key) throws ExecutionException, InterruptedException, TimeoutException {
    var opt = kv.get(toByteSequence(key))
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .getKvs()
                .stream()
                .findFirst()
                .map(kv -> String.valueOf(kv.getValue()));
    return Option.ofOptional(opt);
  }

  private static ByteSequence toByteSequence(String value) {
    return ByteSequence.from(value, StandardCharsets.UTF_8);
  }
}
