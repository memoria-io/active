package io.memoria.active.nats;

import io.nats.client.api.StorageType;

import java.time.Duration;

public record NatsConfig(String url,
                         StorageType storageType,
                         int replicas,
                         int fetchBatchSize,
                         Duration fetchMaxWait,
                         Duration fetchLastMaxWait,
                         boolean denyDelete,
                         boolean denyPurge) {
  public NatsConfig {
    if (url == null || url.isEmpty()) {
      throw new IllegalArgumentException("Url is null or empty");
    }
    if (replicas < 1) {
      throw new IllegalArgumentException("Replicas can not be less than 1");
    }
    if (fetchBatchSize < 1) {
      throw new IllegalArgumentException("fetchBatchSize can not be less than 1");
    }
  }

  public static NatsConfig appendOnly(String url,
                                      StorageType storageType,
                                      int replicationFactor,
                                      int fetchBatch,
                                      Duration fetchOnceMaxWait,
                                      Duration fetchLastMsgMaxWait) {
    return new NatsConfig(url,
                          storageType,
                          replicationFactor,
                          fetchBatch,
                          fetchOnceMaxWait,
                          fetchLastMsgMaxWait,
                          true,
                          true);
  }
}
