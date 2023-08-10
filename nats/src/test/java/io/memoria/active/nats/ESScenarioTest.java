package io.memoria.active.nats;

import io.memoria.reactive.testsuite.eventsourcing.banking.pipeline.PerformanceScenario;
import io.memoria.reactive.testsuite.eventsourcing.banking.pipeline.SimpleDebitScenario;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.test.StepVerifier;

import static io.memoria.reactive.nats.TestUtils.DATA;
import static io.memoria.reactive.nats.TestUtils.createPipeline;
import static io.memoria.reactive.testsuite.Utils.MSG_COUNT;
import static io.memoria.reactive.testsuite.Utils.TIMEOUT;

class ESScenarioTest {

  @ParameterizedTest(name = "Using {0} accounts")
  //  @ValueSource(ints = {0, 1,4, 5, 7, 10, 20, 30, 101, 202, 500, 501, 700, 800, 900, 997, 998, 999, 1000, 1111, 2222, 3333})
  //  @ValueSource(ints = {0, 1, 4, 5, 7, 9, 10, 11, 10001})
  @ValueSource(ints = MSG_COUNT)
  void simpleDebitScenario(int numOfAccounts) {
    // When
    var scenario = new SimpleDebitScenario(DATA, createPipeline(), numOfAccounts);
    StepVerifier.create(scenario.publishCommands()).expectNextCount(scenario.expectedCommandsCount()).verifyComplete();

    // Then
    var now = System.currentTimeMillis();
    StepVerifier.create(scenario.handleCommands())
                .expectNextCount(scenario.expectedEventsCount())
                .expectTimeout(TIMEOUT)
                .verify();
    Utils.printRates("scenario", now);
    //    if (numOfAccounts > 0) {
    //      StepVerifier.create(scenario.verify()).expectNext(true).verifyComplete();
    //    }
  }

  @Disabled("For debugging purposes only")
  @ParameterizedTest(name = "Using {0} accounts")
  @ValueSource(ints = {1, 10, 100, 1000, 10_000, 100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 1000_000})
  void performance(int numOfAccounts) {
    // When
    var scenario = new PerformanceScenario(DATA, createPipeline(), numOfAccounts);
    StepVerifier.create(scenario.publishCommands().doOnNext(System.out::println))
                .expectNextCount(scenario.expectedCommandsCount())
                .verifyComplete();

    // Then
    var now = System.currentTimeMillis();
    StepVerifier.create(scenario.handleCommands()).expectNextCount(numOfAccounts * 5L).expectTimeout(TIMEOUT).verify();
    Utils.printRates("performance scenario", now);
  }
}
