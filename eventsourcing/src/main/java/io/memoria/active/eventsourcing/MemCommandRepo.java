package io.memoria.active.eventsourcing;

import io.memoria.active.core.stream.BlockingChain;
import io.memoria.atom.eventsourcing.Command;
import io.vavr.collection.Stream;
import io.vavr.control.Try;

class MemCommandRepo implements CommandRepo {
  private final BlockingChain<Command> chain;

  MemCommandRepo() {
    this(BlockingChain.inMemory());
  }

  MemCommandRepo(BlockingChain<Command> chain) {
    this.chain = chain;
  }

  @Override
  public Try<Command> publish(Command command) {
    return chain.append(command);
  }

  @Override
  public Stream<Try<Command>> stream() {
    return chain.fetch();
  }
}
