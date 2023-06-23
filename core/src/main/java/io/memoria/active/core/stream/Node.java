package io.memoria.active.core.stream;

import io.vavr.control.Try;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

class Node<T> {
  private final T head;
  private final CountDownLatch countDownLatch;
  private final AtomicReference<Node<T>> next;

  public Node(T head) {
    this.head = head;
    this.countDownLatch = new CountDownLatch(1);
    this.next = new AtomicReference<>();
  }

  public boolean add(Node<T> next) {
    var cas = this.next.compareAndSet(null, next);
    if (cas)
      this.countDownLatch.countDown();
    return cas;
  }

  public T head() {
    return head;
  }

  public Try<Node<T>> tail() {
    return Try.of(() -> {
      this.countDownLatch.await();
      return this.next.get();
    });
  }
}
