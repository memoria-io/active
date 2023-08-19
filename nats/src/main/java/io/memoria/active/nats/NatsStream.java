package io.memoria.active.nats;

import io.memoria.active.core.stream.BlockingStream;
import io.memoria.active.core.stream.Msg;
import io.memoria.active.core.stream.MsgResult;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.PublishOptions;
import io.nats.client.api.DeliverPolicy;
import io.vavr.collection.Stream;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.memoria.active.nats.NatsUtils.createSubscription;
import static io.memoria.active.nats.NatsUtils.fetchMessages;

public class NatsStream implements BlockingStream {
  private static final Logger log = LoggerFactory.getLogger(NatsStream.class.getName());
  private final NatsConfig natsConfig;
  private final Connection connection;
  private final JetStream jetStream;
  private final Duration pollTimeout;

  public NatsStream(NatsConfig natsConfig, Duration pollTimeout) throws IOException, InterruptedException {
    this.natsConfig = natsConfig;
    this.pollTimeout = pollTimeout;
    this.connection = NatsUtils.createConnection(this.natsConfig);
    this.jetStream = connection.jetStream();
  }

  @Override
  public Try<Msg> publish(String topic, int partition, Msg msg) {
    var opts = PublishOptions.builder().clearExpected().messageId(msg.key()).build();
    var natsMessage = NatsUtils.natsMessage(topic, partition, msg);
    return Try.of(() -> jetStream.publishAsync(natsMessage, opts).get(pollTimeout.toMillis(), TimeUnit.MILLISECONDS))
              .map(ack -> msg);
  }

  @Override
  public Try<Stream<MsgResult>> stream(String topic, int partition, boolean fromStart) {
    var subscriptionTry = createSubscription(this.jetStream, DeliverPolicy.All, topic, partition);
    return subscriptionTry.map(this::messageStream);
  }

  @Override
  public void close() throws Exception {
    log.info("Closing connection:{}", connection.getServerInfo());
    connection.close();
  }

  private Stream<MsgResult> messageStream(JetStreamSubscription sub) {
    return Stream.continually(() -> fetchMessages(sub, natsConfig.fetchBatchSize(), natsConfig.fetchMaxWait()))
                 .flatMap(Stream::ofAll)
                 .map(NatsUtils::toMsgResult);
  }
}
