package io.memoria.reactive.eventsourcing.repo;

import io.memoria.active.core.repo.msg.RepoException;
import io.memoria.active.core.repo.msg.SeqMsgRepo;
import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Event;
import io.vavr.collection.Stream;
import io.vavr.control.Try;

public interface EventRepo<E extends Event> {

  Try<E> append(String topic, int seqId, E e);

  Stream<Try<E>> fetch(String topic, String aggId) throws RepoException;

  static <E extends Event> EventRepo<E> msgStream(SeqMsgRepo seqMsgRepo, Class<E> cClass, TextTransformer transformer) {
    return new MsgEventRepo<>(seqMsgRepo, cClass, transformer);
  }

  static <E extends Event> EventRepo<E> inMemory(Class<E> cClass) {
    return EventRepo.msgStream(SeqMsgRepo.inMemory(), cClass, new SerializableTransformer());
  }
}

