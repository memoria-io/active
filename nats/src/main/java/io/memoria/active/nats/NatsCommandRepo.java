package io.memoria.active.nats;

import io.memoria.active.eventsourcing.CommandRepo;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Command;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PublishOptions;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.impl.NatsMessage;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static io.memoria.active.nats.NatsUtils.toPartitionedSubjectName;
import static io.memoria.active.nats.NatsUtils.toSubscriptionName;

public class NatsCommandRepo implements CommandRepo {
  private static final Logger log = LoggerFactory.getLogger(NatsCommandRepo.class.getName());

  private final JetStream jetStream;
  private final PullSubscribeOptions subscribeOptions;
  private final String topic;
  private final String subjectName;
  private final int totalPartitions;

  // Polling Config
  private final Duration pollTimeout;
  private final int fetchBatchSize;
  private final Duration fetchMaxWait;

  // SerDes
  private final TextTransformer transformer;

  /**
   * Constructor with default settings
   */
  public NatsCommandRepo(Connection connection, String topic, int totalPartitions, TextTransformer transformer)
          throws IOException {
    this(connection,
         NatsUtils.defaultCommandConsumerConfigs(toSubscriptionName(topic)).build(),
         topic,
         totalPartitions,
         Duration.ofMillis(1000),
         100,
         Duration.ofMillis(100),
         transformer);
  }

  public NatsCommandRepo(Connection connection,
                         ConsumerConfiguration consumerConfig,
                         String topic,
                         int totalPartitions,
                         Duration pollTimeout,
                         int fetchBatchSize,
                         Duration fetchMaxWait,
                         TextTransformer transformer) throws IOException {
    this.jetStream = connection.jetStream();
    this.subscribeOptions = PullSubscribeOptions.builder().stream(topic).configuration(consumerConfig).build();
    this.topic = topic;
    this.subjectName = toPartitionedSubjectName(topic);
    this.totalPartitions = totalPartitions;
    this.pollTimeout = pollTimeout;
    this.fetchBatchSize = fetchBatchSize;
    this.fetchMaxWait = fetchMaxWait;
    this.transformer = transformer;
  }

  @Override
  public Try<Command> publish(Command command) {
    var opts = PublishOptions.builder().clearExpected().messageId(command.meta().commandId().value()).build();
    return natsMessage(command).map(nm -> jetStream.publishAsync(nm, opts))
                               .mapTry(o -> o.get(pollTimeout.toMillis(), TimeUnit.MILLISECONDS))
                               .map(_ -> command);
  }

  @Override
  public Stream<Try<Command>> stream() {
    return Try.of(this::createSubscription).getOrElseGet(t -> {
      log.error("Error while Creating subscription", t);
      return Stream.of(Try.failure(t));
    });
  }

  private List<Message> fetchMessages(JetStreamSubscription sub, int fetchBatchSize, Duration fetchMaxWait) {
    var msgs = sub.fetch(fetchBatchSize, fetchMaxWait);
    return List.ofAll(msgs).dropWhile(Message::isStatusMessage);
  }

  private Try<NatsMessage> natsMessage(Command command) {
    var partition = command.partition(totalPartitions);
    var subject = toPartitionedSubjectName(topic, partition);
    return transformer.serialize(command).map(payload -> NatsMessage.builder().subject(subject).data(payload).build());
  }

  private Try<Command> toCommand(Message message) {
    var value = new String(message.getData(), StandardCharsets.UTF_8);
    return transformer.deserialize(value, Command.class);
  }

  private Stream<Try<Command>> createSubscription() throws JetStreamApiException, IOException {
    var sub = this.jetStream.subscribe(subjectName, subscribeOptions);
    return Stream.continually(() -> fetchMessages(sub, fetchBatchSize, fetchMaxWait))
                 .flatMap(Stream::ofAll)
                 .peek(Message::ack)
                 .map(this::toCommand);
  }
}
