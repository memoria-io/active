package io.memoria.active.nats;

import io.memoria.atom.core.stream.BlockingStreamPublisher;
import io.memoria.atom.core.stream.Msg;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.PublishOptions;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class NatsStreamPublisher implements BlockingStreamPublisher {
  private static final Logger log = LoggerFactory.getLogger(NatsStreamPublisher.class.getName());
  private final Connection connection;
  private final JetStream jetStream;
  private final Duration pollTimeout;

  public NatsStreamPublisher(NatsConfig natsConfig, Duration pollTimeout) throws IOException, InterruptedException {
    this.pollTimeout = pollTimeout;
    this.connection = NatsUtils.createConnection(natsConfig);
    this.jetStream = connection.jetStream();
  }

  @Override
  public Try<Msg> publish(String topic, int partition, Msg msg) {
    var opts = PublishOptions.builder().clearExpected().messageId(msg.key()).build();
    var natsMessage = NatsUtils.natsMessage(topic, partition, msg);
    return Try.of(() -> jetStream.publishAsync(natsMessage, opts).get(pollTimeout.toMillis(), TimeUnit.MILLISECONDS))
              .map(_ -> msg);
  }

  @Override
  public void close() throws Exception {
    log.info("Closing connection:{}", connection.getServerInfo());
    connection.close();
  }
}
