package io.memoria.reactive.testsuite;

import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.banking.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.banking.event.AccountEvent;
import io.memoria.atom.testsuite.eventsourcing.banking.state.Account;
import io.memoria.atom.testsuite.eventsourcing.banking.state.OpenAccount;
import io.memoria.reactive.eventsourcing.Utils;
import io.memoria.reactive.eventsourcing.exceptions.AlreadyHandledException;
import io.memoria.reactive.eventsourcing.pipeline.AggregatePool;
import io.vavr.Tuple;
import io.vavr.collection.Stream;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static io.memoria.reactive.testsuite.Infra.aggregatePool;
import static org.assertj.core.api.Assertions.assertThat;

class SimpleDebitScenarioIT {
  private static final Data data = Data.ofUUID();
  private static final int INITIAL_BALANCE = 500;
  private static final int DEBIT_AMOUNT = 300;
  private static final int numOfAccounts = 1000;

  private final AggregatePool<Account, AccountCommand, AccountEvent> pool = aggregatePool(data.idSupplier,
                                                                                          data.timeSupplier);

  @Test
  void scenario() {
    // Given
    int expectedSuccessCount = numOfAccounts * 3;
    int expectedFailureCount = 0;

    // When
    var successCount = new AtomicInteger();
    var duplicatesCount = new AtomicInteger();
    var failuresCount = new AtomicInteger();

    commands().map(pool::handle).forEach(eTr -> {
      if (eTr.isSuccess()) {
        switch (eTr.get()) {
          case AccountEvent _ -> successCount.getAndIncrement();
          case null -> throw new IllegalArgumentException();
        }
      } else {
        //noinspection ThrowableNotThrown
        switch (eTr.getCause()) {
          case AlreadyHandledException _ -> duplicatesCount.getAndIncrement();
          case null -> throw new IllegalArgumentException();
          default -> failuresCount.getAndIncrement();
        }
      }
    });
    assertThat(successCount.get()).isEqualTo(expectedSuccessCount);
    assertThat(duplicatesCount.get()).isEqualTo(0);
    assertThat(failuresCount.get()).isEqualTo(expectedFailureCount);

    //    // Then
    //    var eventsSize = pool.fetchEvents()
    //    assertThat(eventsSize).isEqualTo(numOfAccounts * 5L);
    //
    //    // And
    //    assertThat(verify(StateId.of(0))).isTrue();
  }

  private Stream<AccountCommand> commands() {
    var debitedIds = data.createIds(0, numOfAccounts).map(StateId::of);
    var creditedIds = data.createIds(numOfAccounts, numOfAccounts * 2).map(StateId::of);
    var createDebitedAcc = data.createAccountCmd(debitedIds, INITIAL_BALANCE);
    var createCreditedAcc = data.createAccountCmd(creditedIds, INITIAL_BALANCE);
    var debitTheAccounts = data.debitCmd(debitedIds.zipWith(creditedIds, Tuple::of), DEBIT_AMOUNT);
    return createDebitedAcc.appendAll(createCreditedAcc).appendAll(debitTheAccounts);
  }

  private boolean verify(StateId stateId) {
    var opt = Utils.reduce(pool.domain.evolver(), pool.fetchEvents(stateId).get());
    var account = (OpenAccount) opt.get();
    return verify(account);
  }

  private boolean verify(OpenAccount acc) {
    if (acc.debitCount() > 0) {
      return acc.balance() == INITIAL_BALANCE - DEBIT_AMOUNT;
    } else if (acc.creditCount() > 0) {
      return acc.balance() == INITIAL_BALANCE + DEBIT_AMOUNT;
    } else {
      throw new IllegalStateException(acc.toString());
    }
  }
}
