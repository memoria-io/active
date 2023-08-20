package io.memoria.active.testsuite;

import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.banking.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.banking.event.AccountCreated;
import io.memoria.atom.testsuite.eventsourcing.banking.event.AccountEvent;
import io.memoria.atom.testsuite.eventsourcing.banking.event.Credited;
import io.memoria.atom.testsuite.eventsourcing.banking.event.DebitConfirmed;
import io.memoria.atom.testsuite.eventsourcing.banking.event.Debited;
import io.memoria.atom.testsuite.eventsourcing.banking.state.Account;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.vavr.collection.Stream;
import io.vavr.control.Try;

public class PerformanceScenario implements PartitionScenario<AccountCommand, AccountEvent> {
  private static final int INITIAL_BALANCE = 500;
  private static final int DEBIT_AMOUNT = 300;

  private final Data data;
  private final PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline;
  private final int numOfAccounts;

  public PerformanceScenario(Data data,
                             PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline,
                             int numOfAccounts) {
    this.data = data;
    this.pipeline = pipeline;
    this.numOfAccounts = numOfAccounts;
  }

  @Override
  public int expectedCommandsCount() {
    return numOfAccounts * 3;
  }

  @Override
  public int expectedEventsCount() {
    return numOfAccounts * 5;
  }

  @Override
  public Stream<AccountCommand> publishCommands() {
    var debitedIds = data.createIds(0, numOfAccounts).map(StateId::of);
    var creditedIds = data.createIds(numOfAccounts, numOfAccounts).map(StateId::of);
    var createDebitedAcc = data.createAccountCmd(debitedIds, INITIAL_BALANCE);
    var createCreditedAcc = data.createAccountCmd(creditedIds, INITIAL_BALANCE);
    var debitTheAccounts = data.debitCmd(debitedIds.zipWith(creditedIds), DEBIT_AMOUNT);
    var commands = Stream.concat(createDebitedAcc, createCreditedAcc, debitTheAccounts);

    return commands.map(pipeline::pubCommand).map(Try::get);
  }

  @Override
  public Stream<AccountEvent> handleCommands() {
    return pipeline.handle();
  }

  @Override
  public Mono<Boolean> verify() {
    return pipeline.subToEvents().map(PerformanceScenario::isTypeOf).all(b -> b);
  }

  private static boolean isTypeOf(AccountEvent acc) {
    if (acc instanceof AccountCreated
        || acc instanceof Debited
        || acc instanceof Credited
        || acc instanceof DebitConfirmed) {
      return true;
    } else {
      throw new IllegalStateException("Unknown event %s".formatted(acc.getClass().getSimpleName()));
    }
  }
}
