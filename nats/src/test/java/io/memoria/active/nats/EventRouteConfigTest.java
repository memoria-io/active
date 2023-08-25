package io.memoria.active.nats;

import io.nats.client.api.StorageType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

class EventRouteConfigTest {
  @Test
  void validation() {
    Assertions.assertThatIllegalArgumentException()
              .isThrownBy(() -> new NatsConfig("",
                                               StorageType.File,
                                               1,
                                               100,
                                               Duration.ofMillis(100),
                                               Duration.ofMillis(300),
                                               false,
                                               false));
    Assertions.assertThatIllegalArgumentException()
              .isThrownBy(() -> new NatsConfig("",
                                               StorageType.File,
                                               1,
                                               100,
                                               Duration.ofMillis(100),
                                               Duration.ofMillis(300),
                                               false,
                                               false));
    Assertions.assertThatIllegalArgumentException()
              .isThrownBy(() -> new NatsConfig("url",
                                               StorageType.File,
                                               0,
                                               100,
                                               Duration.ofMillis(100),
                                               Duration.ofMillis(300),
                                               false,
                                               false));
    Assertions.assertThatIllegalArgumentException()
              .isThrownBy(() -> new NatsConfig("url",
                                               StorageType.File,
                                               1,
                                               -1,
                                               Duration.ofMillis(100),
                                               Duration.ofMillis(300),
                                               false,
                                               false));
  }
}
