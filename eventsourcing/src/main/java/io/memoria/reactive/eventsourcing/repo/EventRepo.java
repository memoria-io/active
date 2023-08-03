package io.memoria.reactive.eventsourcing.repo;

import io.memoria.active.core.repo.msg.MsgRepo;
import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Event;
import io.vavr.collection.List;
import io.vavr.control.Try;

public interface EventRepo<E extends Event> {

  Try<E> append(String topic, int seqId, E e);

  Try<List<E>> getAll(String topic, String aggId);

  Try<Integer> size(String topic, String aggId);

  static <E extends Event> EventRepo<E> msgStream(MsgRepo msgRepo, Class<E> cClass, TextTransformer transformer) {
    return new MsgEventRepo<>(msgRepo, cClass, transformer);
  }

  static <E extends Event> EventRepo<E> inMemory(Class<E> cClass) {
    return EventRepo.msgStream(MsgRepo.inMemory(), cClass, new SerializableTransformer());
  }
}

