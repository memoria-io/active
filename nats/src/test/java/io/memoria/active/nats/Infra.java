package io.memoria.active.nats;

import io.nats.client.api.StorageType;

import java.time.Duration;

public class Infra {
  public static final String NATS_URL = "nats://localhost:4222";
  public static final NatsConfig NATS_CONFIG = NatsConfig.appendOnly(NATS_URL,
                                                                     StorageType.File,
                                                                     1,
                                                                     10,
                                                                     Duration.ofMillis(100),
                                                                     Duration.ofMillis(300));

  private Infra() {}
}
