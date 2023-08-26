package io.memoria.reactive.testsuite;

import io.memoria.active.testsuite.Data;
import io.memoria.active.testsuite.PerformanceScenario;
import io.memoria.active.testsuite.SimpleDebitScenario;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.banking.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.banking.event.AccountEvent;
import io.memoria.atom.testsuite.eventsourcing.banking.state.Account;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.nats.client.JetStreamApiException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static io.memoria.active.nats.NatsUtils.createOrUpdateTopic;
import static io.memoria.reactive.testsuite.Infra.NATS_CONFIG;
import static io.memoria.reactive.testsuite.Infra.StreamType.KAFKA;
import static io.memoria.reactive.testsuite.Infra.StreamType.MEMORY;
import static io.memoria.reactive.testsuite.Infra.StreamType.NATS;
import static io.memoria.reactive.testsuite.Infra.commandTopic;
import static io.memoria.reactive.testsuite.Infra.pipeline;
import static org.assertj.core.api.Assertions.assertThat;

class ESScenarioTest {
  private static final Data data = Data.ofUUID();

  @ParameterizedTest(name = "Using {0} adapter")
  @MethodSource("dataSource")
  void simpleDebitScenario(String name,
                           Data data,
                           PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline,
                           int numOfAccounts) {
    // When
    var scenario = new SimpleDebitScenario(data, pipeline, numOfAccounts);
    var published = scenario.publishCommands();
    assertThat(published.size()).isEqualTo(numOfAccounts * 3L);

    // Then
    var eventsSize = scenario.handleCommands().take(numOfAccounts * 5).size();
    assertThat(eventsSize).isEqualTo(numOfAccounts * 5L);

    // And
    assertThat(scenario.verify(StateId.of(0))).isTrue();
  }

  @Disabled("For debugging purposes only")
  @ParameterizedTest(name = "Using {0} adapter")
  @MethodSource("dataSource")
  void performance(String name, Data data, PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline) {
    // Given
    int numOfAccounts = 1000_000;

    // When
    var scenario = new PerformanceScenario(data, pipeline, numOfAccounts);
    var published = scenario.publishCommands();
    assertThat(published.size()).isEqualTo(numOfAccounts * 3L);

    // Then
    var eventsSize = scenario.handleCommands().size();
    assertThat(eventsSize).isEqualTo(numOfAccounts * 5L);
  }

  private static Stream<Arguments> dataSource() throws IOException, InterruptedException, JetStreamApiException {
    return Stream.of(inMemoryArgs(), kafkaArgs(), natsArgs());
  }

  private static Arguments kafkaArgs() throws IOException, InterruptedException {
    var route = commandRoute(KAFKA.name());
    var kafkaPipeline = pipeline(data.idSupplier, data.timeSupplier, KAFKA, route);
    return Arguments.of(KAFKA.name(), data, kafkaPipeline, 10);
  }

  private static Arguments natsArgs() throws IOException, InterruptedException, JetStreamApiException {
    var route = commandRoute(NATS.name());
    var natsPipeline = pipeline(data.idSupplier, data.timeSupplier, NATS, route);
    createOrUpdateTopic(NATS_CONFIG, route.name(), route.totalPartitions());
    return Arguments.of(NATS.name(), data, natsPipeline, 10);
  }

  private static Arguments inMemoryArgs() throws IOException, InterruptedException {
    var route = commandRoute(MEMORY.name());
    var inMemoryPipeline = pipeline(data.idSupplier, data.timeSupplier, MEMORY, route);
    return Arguments.of(MEMORY.name(), data, inMemoryPipeline, 10);
  }

  private static CommandRoute commandRoute(String prefix) {
    return new CommandRoute(commandTopic(prefix + "_commands"), 0);
  }
}
