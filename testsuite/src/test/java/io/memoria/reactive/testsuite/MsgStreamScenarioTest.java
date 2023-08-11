//package io.memoria.reactive.testsuite;
//
//import io.memoria.reactive.core.stream.Msg;
//import io.memoria.reactive.core.stream.MsgStream;
//import io.memoria.reactive.nats.Utils;
//import io.nats.client.JetStreamApiException;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.params.ParameterizedTest;
//import org.junit.jupiter.params.provider.Arguments;
//import org.junit.jupiter.params.provider.MethodSource;
//import reactor.core.publisher.Flux;
//import reactor.core.publisher.Mono;
//import reactor.test.StepVerifier;
//
//import java.io.IOException;
//import java.util.stream.Stream;
//
//import static io.memoria.reactive.testsuite.Infra.NATS_CONFIG;
//import static io.memoria.reactive.testsuite.Infra.StreamType.KAFKA;
//import static io.memoria.reactive.testsuite.Infra.StreamType.MEMORY;
//import static io.memoria.reactive.testsuite.Infra.StreamType.NATS;
//import static io.memoria.reactive.testsuite.Infra.TIMEOUT;
//import static io.memoria.reactive.testsuite.Infra.msgStream;
//
//class MsgStreamScenarioTest {
//  private static final String topic = Infra.topicName(MsgStreamScenarioTest.class.getSimpleName());
//  private static final int partition = 0;
//  private static final int totalPartitions = 1;
//  private static final int msgCount = 1000;
//
//  @BeforeAll
//  static void beforeAll() throws JetStreamApiException, IOException, InterruptedException {
//    Utils.createOrUpdateTopic(NATS_CONFIG, topic, totalPartitions);
//  }
//
//  @ParameterizedTest(name = "Using {0} adapter")
//  @MethodSource("dataSource")
//  void test(String name, MsgStream msgStream, int msgCount) {
//    // Publish
//    var now = System.currentTimeMillis();
//    StepVerifier.create(publish(msgStream)).expectNextCount(msgCount).verifyComplete();
//    Infra.printRates("publish", now, msgCount);
//
//    // Subscribe
//    now = System.currentTimeMillis();
//    StepVerifier.create(subscribe(msgStream)).expectNextCount(msgCount).expectTimeout(TIMEOUT).verify();
//    Infra.printRates("subscribe", now, msgCount);
//
//    // Last
//    StepVerifier.create(last(msgStream).map(Msg::key)).expectNext(String.valueOf(msgCount - 1)).verifyComplete();
//  }
//
//  private Flux<Msg> publish(MsgStream stream) {
//    var msgs = Flux.range(0, msgCount).map(i -> new Msg(String.valueOf(i), "hello world"));
//    return msgs.concatMap(msg -> stream.pub(topic, partition, msg));
//  }
//
//  private Flux<Msg> subscribe(MsgStream stream) {
//    return stream.sub(topic, partition);
//  }
//
//  private Mono<Msg> last(MsgStream stream) {
//    return stream.last(topic, partition);
//  }
//
//  private static Stream<Arguments> dataSource() throws IOException, InterruptedException {
//    var arg1 = Arguments.of(MEMORY.name(), msgStream(MEMORY), 1000);
//    var arg2 = Arguments.of(KAFKA.name(), msgStream(KAFKA), 1000);
//    var arg3 = Arguments.of(NATS.name(), msgStream(NATS), 1000);
//    return Stream.of(arg1, arg2, arg3);
//  }
//}
