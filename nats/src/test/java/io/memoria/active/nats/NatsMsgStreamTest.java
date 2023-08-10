package io.memoria.active.nats;

import io.memoria.reactive.core.stream.Msg;
import io.memoria.reactive.testsuite.MsgStreamScenario;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.StreamInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.test.StepVerifier;

import java.io.IOException;

import static io.memoria.reactive.nats.TestUtils.NATS_CONFIG;
import static io.memoria.reactive.testsuite.Utils.MSG_COUNT;
import static io.memoria.reactive.testsuite.Utils.SCHEDULER;
import static io.memoria.reactive.testsuite.Utils.TIMEOUT;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NatsMsgStreamTest {
  private static final Logger log = LoggerFactory.getLogger(NatsMsgStreamTest.class.getName());
  private static final MsgStreamScenario scenario;

  static {
    try {
      String topic = io.memoria.reactive.testsuite.Utils.topicName(NatsMsgStreamTest.class);
      Utils.createOrUpdateTopic(NATS_CONFIG, topic, 1).map(StreamInfo::toString).forEach(log::info);
      var repo = new NatsMsgStream(NATS_CONFIG, SCHEDULER);
      scenario = new MsgStreamScenario(MSG_COUNT, topic, 0, repo);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  @Order(0)
  void publish() {
    var now = System.currentTimeMillis();
    StepVerifier.create(scenario.publish()).expectNextCount(MSG_COUNT).verifyComplete();
    io.memoria.reactive.testsuite.Utils.printRates("publish", now);
    //    StepVerifier.create(scenario.last().map(Msg::key)).expectNext(String.valueOf(MSG_COUNT - 1)).verifyComplete();
  }

  @Test
  @Order(1)
  void subscribe() {
    var now = System.currentTimeMillis();
    StepVerifier.create(scenario.subscribe()).expectNextCount(MSG_COUNT).expectTimeout(TIMEOUT).verify();
    io.memoria.reactive.testsuite.Utils.printRates("subscribe", now);
  }

  @Test
  @Order(2)
  void last() {
    StepVerifier.create(scenario.last().map(Msg::key)).expectNext(String.valueOf(MSG_COUNT - 1)).verifyComplete();
  }

  @Test
  @Order(3)
  void toMessage() {
    var message = NatsMsgStream.natsMessage("topic", 0, new Msg("0", "hello world"));
    Assertions.assertEquals("topic_0.subject", message.getSubject());
  }
}
