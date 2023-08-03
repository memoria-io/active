package io.memoria.reactive.eventsourcing.repo;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.reactive.core.msg.stream.MsgStream;
import io.vavr.control.Try;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface EventRepo<E extends Event> {

  Try<E> pub(String topic, int partition, E e);

  Try<E> sub(String topic, int partition);

  Try<E> last(String topic, int partition);

  static <E extends Event> EventRepo<E> msgStream(MsgRepo msgRepo, Class<E> cClass, TextTransformer transformer) {
    return new MsgEventRepo<>(msgRepo, cClass, transformer);
  }

  static <E extends Event> EventRepo<E> inMemory(Class<E> cClass) {
    return EventRepo.msgStream(MsgRepo.inMemory(), cClass, new SerializableTransformer());
  }

  static <E extends Event> EventRepo<E> inMemory(int history, Class<E> cClass) {
    return EventRepo.msgStream(MsgRepo.inMemory(history), cClass, new SerializableTransformer());
  }
}

