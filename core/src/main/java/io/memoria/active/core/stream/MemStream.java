package io.memoria.active.core.stream;

import io.vavr.collection.Stream;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This combines BlockingDeque and iterables where it Blocks on next element, while it keeps all elements in memory for
 * replaying, and streams any new added elements to listeners.
 */
public class MemStream<T> {
  private final ReentrantLock lock;
  private final CountDownLatch latch;
  private final AtomicReference<Node<T>> first;
  private final AtomicReference<Node<T>> last;

  public MemStream() {
    this.lock = new ReentrantLock();
    this.latch = new CountDownLatch(1);
    this.first = new AtomicReference<>();
    this.last = new AtomicReference<>();
  }

  public void add(T t) {
    Objects.requireNonNull(t);
    this.lock.lock();
    var node = new Node<>(t);
    var casFirst = this.first.compareAndSet(null, node);
    if (casFirst) {
      this.last.set(node);
      this.latch.countDown();
    } else {
      this.last.get().add(node);
      this.last.set(node);
    }
    this.lock.unlock();
  }

  public Stream<T> stream() {
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return Stream.iterate(first.get(), t -> t.tail().get()).map(Node::head);
  }
}
