package io.memoria.reactive.eventsourcing.repo;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Command;
import io.memoria.reactive.core.msg.stream.MsgStream;
import io.vavr.control.Try;

public interface CommandRepo<C extends Command> {
  Try<C> pub(String topic, int partition, C c);

  Try<C> sub(String topic, int partition);

  static <C extends Command> CommandRepo<C> msgStream(MsgRepo msgRepo,
                                                      Class<C> cClass,
                                                      TextTransformer transformer) {
    return new MsgCommandRepo<>(msgRepo, cClass, transformer);
  }

  static <C extends Command> CommandRepo<C> inMemory(Class<C> cClass) {
    return CommandRepo.msgStream(MsgRepo.inMemory(), cClass, new SerializableTransformer());
  }

  static <C extends Command> CommandRepo<C> inMemory(int history, Class<C> cClass) {
    return CommandRepo.msgStream(MsgRepo.inMemory(history), cClass, new SerializableTransformer());
  }
}

