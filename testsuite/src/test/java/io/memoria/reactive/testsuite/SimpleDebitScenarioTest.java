package io.memoria.reactive.testsuite;

import io.memoria.active.testsuite.Data;
import io.memoria.active.testsuite.SimpleDebitScenario;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.banking.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.banking.event.AccountEvent;
import io.memoria.atom.testsuite.eventsourcing.banking.state.Account;
import io.memoria.reactive.eventsourcing.pipeline.AggregatePool;
import org.junit.jupiter.api.Test;

import static io.memoria.reactive.testsuite.Infra.aggregatePool;
import static org.assertj.core.api.Assertions.assertThat;

class SimpleDebitScenarioTest {
  private static final Data data = Data.ofUUID();
  private static final AggregatePool<Account, AccountCommand, AccountEvent> aggPool;

  static {
    aggPool = aggregatePool(data.idSupplier, data.timeSupplier);
  }

  @Test
  void scenario() {
    // Given
    int numOfAccounts = 10;

    // When
    var scenario = new SimpleDebitScenario(data, aggPool, numOfAccounts);
    var published = scenario.handleCommands();
    assertThat(published.size()).isEqualTo(numOfAccounts * 3L);

    // Then
    var eventsSize = scenario.handleCommands().take(numOfAccounts * 5).size();
    assertThat(eventsSize).isEqualTo(numOfAccounts * 5L);

    // And
    assertThat(scenario.verify(StateId.of(0))).isTrue();
  }
}
