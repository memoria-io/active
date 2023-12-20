package io.memoria.active.nats;

import io.memoria.atom.core.stream.Msg;
import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.ReplayPolicy;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.vavr.collection.List;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Utility class for directly using nats, this layer is for testing NATS java driver, and ideally it shouldn't have
 * Flux/Mono utilities, only pure java/nats APIs
 */
public class NatsUtils {
  public static final String ID_HEADER = "ID_HEADER";
  public static final String SPLIT_TOKEN = "_";
  public static final String SUBJECT_EXT = ".subject";

  private NatsUtils() {}

  public static String subjectName(String topic, int partition) {
    return streamName(topic, partition) + SUBJECT_EXT;
  }

  public static Connection createConnection(NatsConfig natsConfig) throws IOException, InterruptedException {
    return Nats.connect(Options.builder().server(natsConfig.url()).errorListener(errorListener()).build());
  }

  public static List<StreamInfo> createOrUpdateTopic(NatsConfig natsConfig, String topic, int numOfPartitions)
          throws IOException, InterruptedException, JetStreamApiException {
    var result = List.<StreamInfo>empty();
    try (var nc = NatsUtils.createConnection(natsConfig)) {
      var streamConfigs = List.range(0, numOfPartitions)
                              .map(partition -> streamConfiguration(natsConfig, topic, partition));
      for (StreamConfiguration config : streamConfigs) {
        StreamInfo createOrUpdate = createOrUpdateStream(nc, config);
        result = result.append(createOrUpdate);
      }
    }
    return result;
  }

  public static Try<JetStreamSubscription> createSubscription(JetStream js,
                                                              DeliverPolicy deliverPolicy,
                                                              String topic,
                                                              int partition) {
    var config = ConsumerConfiguration.builder()
                                      .ackPolicy(AckPolicy.None)
                                      .deliverPolicy(deliverPolicy)
                                      .replayPolicy(ReplayPolicy.Instant)
                                      .build();
    var streamName = streamName(topic, partition);
    var subscribeOptions = PullSubscribeOptions.builder().stream(streamName).configuration(config).build();
    String subjectName = subjectName(topic, partition);
    return Try.of(() -> js.subscribe(subjectName, subscribeOptions));
  }

  static List<Message> fetchMessages(JetStreamSubscription sub, int fetchBatchSize, Duration fetchMaxWait) {
    var msgs = sub.fetch(fetchBatchSize, fetchMaxWait);
    return List.ofAll(msgs).dropWhile(Message::isStatusMessage);
  }

  static StreamConfiguration streamConfiguration(NatsConfig natsConfig, String topic, int partition) {
    return StreamConfiguration.builder()
                              .replicas(natsConfig.replicas())
                              .storageType(natsConfig.storageType())
                              .denyDelete(natsConfig.denyDelete())
                              .denyPurge(natsConfig.denyPurge())
                              .name(streamName(topic, partition))
                              .subjects(subjectName(topic, partition))
                              .build();
  }

  public static Option<Message> fetchLastMessage(JetStreamSubscription sub, NatsConfig config) {
    return List.ofAll(sub.fetch(config.fetchBatchSize(), config.fetchMaxWait())).lastOption();
  }

  static StreamInfo createOrUpdateStream(Connection nc, StreamConfiguration streamConfiguration)
          throws IOException, JetStreamApiException {
    var streamNames = nc.jetStreamManagement().getStreamNames();
    if (streamNames.contains(streamConfiguration.getName()))
      return nc.jetStreamManagement().updateStream(streamConfiguration);
    else
      return nc.jetStreamManagement().addStream(streamConfiguration);
  }

  static ErrorListener errorListener() {
    return new ErrorListener() {
      @Override
      public void errorOccurred(Connection conn, String error) {
        ErrorListener.super.errorOccurred(conn, error);
      }
    };
  }

  static String streamName(String topic, int partition) {
    return "%s%s%d".formatted(topic, SPLIT_TOKEN, partition);
  }

  static NatsMessage natsMessage(String topic, int partition, Msg msg) {
    var subjectName = subjectName(topic, partition);
    var headers = new Headers();
    headers.add(ID_HEADER, msg.key());
    return NatsMessage.builder().subject(subjectName).headers(headers).data(msg.value()).build();
  }

  static Msg toMsg(Message message) {
    String key = message.getHeaders().getFirst(ID_HEADER);
    var value = new String(message.getData(), StandardCharsets.UTF_8);
    return new Msg(key, value);
  }
}
