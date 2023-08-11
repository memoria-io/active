package io.memoria.active.nats;

import io.memoria.active.core.stream.BlockingStream;
import io.memoria.active.core.stream.Msg;
import io.memoria.active.core.stream.MsgResult;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.PublishOptions;
import io.nats.client.api.DeliverPolicy;
import io.vavr.collection.Stream;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.memoria.active.nats.NatsUtils.createSubscription;
import static io.memoria.active.nats.NatsUtils.fetchMessages;

public class NatsMsgStream implements BlockingStream {
  private static final Logger log = LoggerFactory.getLogger(NatsMsgStream.class.getName());
  private final NatsConfig natsConfig;
  private final Connection connection;
  private final JetStream jetStream;

  public NatsMsgStream(NatsConfig natsConfig) throws IOException, InterruptedException {
    this.natsConfig = natsConfig;
    this.connection = NatsUtils.createConnection(this.natsConfig);
    this.jetStream = connection.jetStream();
  }

  @Override
  public Try<Msg> append(String topic, int partition, Msg msg) {
    var opts = PublishOptions.builder().clearExpected().messageId(msg.key()).build();
    var natsMessage = NatsUtils.natsMessage(topic, partition, msg);
    return Try.of(() -> jetStream.publishAsync(natsMessage, opts).get()).map(ack -> msg);
  }

  @Override
  public Stream<Try<MsgResult>> stream(String topic, int partition) {
    var subTry = createSubscription(this.jetStream, DeliverPolicy.All, topic, partition);
    if (subTry.isSuccess()) {
      return Stream.continually(() -> fetchMessages(subTry.get(),
                                                    natsConfig.fetchBatchSize(),
                                                    natsConfig.fetchMaxWait()))
                   .flatMap(Stream::ofAll)
                   .map(NatsUtils::toMsgResult)
                   .map(Try::success);
    } else {
      return Stream.of(Try.failure(subTry.getCause()));
    }
  }

  @Override
  public void close() throws Exception {
    log.info("Closing connection:{}", connection.getServerInfo());
    connection.close();
  }
}
