package io.memoria.reactive.eventsourcing.repo;

import io.memoria.atom.eventsourcing.Command;
import io.vavr.control.Try;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class MemCommandPublisher<C extends Command> implements CommandPublisher<C> {
  private final BlockingDeque<C> blockingDeque;

  public MemCommandPublisher() {
    this.blockingDeque = new LinkedBlockingDeque<>();
  }

  public MemCommandPublisher(BlockingDeque<C> blockingDeque) {
    this.blockingDeque = blockingDeque;
  }

  @Override
  public Try<C> publish(C cmd) {
    blockingDeque.add(cmd);
    return Try.success(cmd);
  }

  public C take() throws InterruptedException {
    return blockingDeque.take();
  }
}
