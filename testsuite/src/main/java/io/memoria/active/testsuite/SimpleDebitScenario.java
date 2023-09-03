package io.memoria.active.testsuite;

import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.banking.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.banking.event.AccountEvent;
import io.memoria.atom.testsuite.eventsourcing.banking.state.Account;
import io.memoria.atom.testsuite.eventsourcing.banking.state.OpenAccount;
import io.memoria.reactive.eventsourcing.Utils;
import io.memoria.reactive.eventsourcing.pipeline.AggregatePool;
import io.vavr.Tuple;
import io.vavr.collection.List;
import io.vavr.control.Option;
import io.vavr.control.Try;

@SuppressWarnings("ClassCanBeRecord")
public class SimpleDebitScenario implements ESScenario<AccountEvent> {
  public static final int INITIAL_BALANCE = 500;
  public static final int DEBIT_AMOUNT = 300;

  public final Data data;
  public final AggregatePool<Account, AccountCommand, AccountEvent> pipeline;
  public final int numOfAccounts;

  public SimpleDebitScenario(Data data,
                             AggregatePool<Account, AccountCommand, AccountEvent> pipeline,
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
  public List<Option<Try<AccountEvent>>> handleCommands() {
    var debitedIds = data.createIds(0, numOfAccounts).map(StateId::of);
    var creditedIds = data.createIds(numOfAccounts, numOfAccounts * 2).map(StateId::of);
    var createDebitedAcc = data.createAccountCmd(debitedIds, INITIAL_BALANCE);
    var createCreditedAcc = data.createAccountCmd(creditedIds, INITIAL_BALANCE);
    var debitTheAccounts = data.debitCmd(debitedIds.zipWith(creditedIds, Tuple::of), DEBIT_AMOUNT);
    return createDebitedAcc.appendAll(createCreditedAcc).appendAll(debitTheAccounts).map(pipeline::handle);
  }

  @Override
  public boolean verify(StateId stateId) {
    var opt = Utils.reduce(pipeline.domain.evolver(), pipeline.fetchEvents(stateId).get().take(expectedEventsCount()));
    var account = (OpenAccount) opt.get();
    return verify(account);
  }

  boolean verify(OpenAccount acc) {
    if (acc.debitCount() > 0) {
      return acc.balance() == INITIAL_BALANCE - DEBIT_AMOUNT;
    } else if (acc.creditCount() > 0) {
      return acc.balance() == INITIAL_BALANCE + DEBIT_AMOUNT;
    } else {
      throw new IllegalStateException(acc.toString());
    }
  }
}
