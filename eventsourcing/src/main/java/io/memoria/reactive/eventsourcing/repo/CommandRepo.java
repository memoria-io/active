package io.memoria.reactive.eventsourcing.repo;

import io.memoria.active.core.repo.msg.MsgRepo;
import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Command;
import io.vavr.collection.List;
import io.vavr.control.Try;

public interface CommandRepo<C extends Command> {

  Try<C> append(String topic, int seqId, C c);

  Try<List<C>> getAll(String topic, String aggId);

  static <C extends Command> CommandRepo<C> msgStream(MsgRepo msgRepo, Class<C> cClass, TextTransformer transformer) {
    return new MsgCommandRepo<>(msgRepo, cClass, transformer);
  }

  static <C extends Command> CommandRepo<C> inMemory(Class<C> cClass) {
    return CommandRepo.msgStream(MsgRepo.inMemory(), cClass, new SerializableTransformer());
  }
}

