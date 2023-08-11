//package io.memoria.reactive.testsuite;
//
//import io.memoria.active.testsuite.Data;
//import io.memoria.atom.testsuite.eventsourcing.banking.command.AccountCommand;
//import io.memoria.atom.testsuite.eventsourcing.banking.event.AccountEvent;
//import io.memoria.atom.testsuite.eventsourcing.banking.state.Account;
//import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
//import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
//import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
//import io.memoria.reactive.nats.Utils;
//import io.nats.client.JetStreamApiException;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.Disabled;
//import org.junit.jupiter.params.ParameterizedTest;
//import org.junit.jupiter.params.provider.Arguments;
//import org.junit.jupiter.params.provider.MethodSource;
//import reactor.test.StepVerifier;
//
//import java.io.IOException;
//import java.util.stream.Stream;
//
//import static io.memoria.active.nats.NatsUtils.*;
//import static io.memoria.reactive.testsuite.Infra.NATS_CONFIG;
//import static io.memoria.reactive.testsuite.Infra.StreamType.KAFKA;
//import static io.memoria.reactive.testsuite.Infra.StreamType.MEMORY;
//import static io.memoria.reactive.testsuite.Infra.StreamType.NATS;
//import static io.memoria.reactive.testsuite.Infra.pipeline;
//import static io.memoria.reactive.testsuite.Infra.topicName;
//
//class ESScenarioTest {
//  private static final Data data = Data.ofUUID();
//  private static final CommandRoute commandRoute = new CommandRoute(topicName("Commands"), 0);
//
//  @BeforeAll
//  static void beforeAll() throws JetStreamApiException, IOException, InterruptedException {
//    createOrUpdateTopic(NATS_CONFIG, commandRoute.name(), commandRoute.totalPartitions());
//  }
//
//  @ParameterizedTest(name = "Using {0} adapter")
//  @MethodSource("dataSource")
//  void simpleDebitScenario(String name,
//                           Data data,
//                           PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline,
//                           int numOfAccounts) {
//    // When
//    var scenario = new SimpleDebitScenario(data, pipeline, numOfAccounts);
//    StepVerifier.create(scenario.publishCommands()).expectNextCount(numOfAccounts * 3L).verifyComplete();
//
//    // Then
//    StepVerifier.create(scenario.handleCommands())
//                .expectNextCount(numOfAccounts * 5L)
//                .expectTimeout(Infra.TIMEOUT)
//                .verify();
//  }
//
//  @Disabled("For debugging purposes only")
//  @ParameterizedTest(name = "Using {0} adapter")
//  @MethodSource("dataSource")
//  void performance(String name, Data data, PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline) {
//    // Given
//    int numOfAccounts = 1000_000;
//
//    // When
//    var scenario = new PerformanceScenario(data, pipeline, numOfAccounts);
//    StepVerifier.create(scenario.publishCommands()).expectNextCount(numOfAccounts * 3L).verifyComplete();
//    // Then
//    StepVerifier.create(scenario.handleCommands())
//                .expectNextCount(numOfAccounts * 5L)
//                .expectTimeout(Infra.TIMEOUT)
//                .verify();
//  }
//
//  private static Stream<Arguments> dataSource() throws IOException, InterruptedException {
//
//    var inMemoryPipeline = pipeline(data.idSupplier, data.timeSupplier, MEMORY, commandRoute, eventRoute);
//    var kafkaPipeline = pipeline(data.idSupplier, data.timeSupplier, KAFKA, commandRoute, eventRoute);
//    var natsPipeline = pipeline(data.idSupplier, data.timeSupplier, NATS, commandRoute, eventRoute);
//
//    var arg1 = Arguments.of(MEMORY.name(), data, inMemoryPipeline, 10);
//    var arg2 = Arguments.of(KAFKA.name(), data, kafkaPipeline, 10);
//    var arg3 = Arguments.of(NATS.name(), data, natsPipeline, 10);
//    return Stream.of(arg1, arg2, arg3);
//  }
//}
