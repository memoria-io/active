package io.memoria.reactive.eventsourcing.repo;

import io.memoria.active.core.repo.msg.Msg;
import io.memoria.active.core.repo.msg.MsgRepo;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.AtomUtils;
import io.vavr.collection.List;
import io.vavr.control.Try;

class MsgEventRepo<E extends Event> implements EventRepo<E> {

  private final MsgRepo msgRepo;
  private final Class<E> cClass;
  private final TextTransformer transformer;

  public MsgEventRepo(MsgRepo msgRepo, Class<E> cClass, TextTransformer transformer) {
    this.msgRepo = msgRepo;
    this.cClass = cClass;
    this.transformer = transformer;
  }

  @Override
  public Try<E> append(String topic, int seqId, E cmd) {
    return toMsg(seqId, cmd).flatMap(msg -> msgRepo.append(topic, msg)).map(msg -> cmd);
  }

  @Override
  public Try<List<E>> getAll(String topic, String aggId) {
    return msgRepo.fetch(topic, aggId).map(list -> list.map(this::toCmd)).flatMap(AtomUtils::toListOfTry);
  }

  @Override
  public Try<Integer> size(String topic, String aggId) {
    return msgRepo.size(topic, aggId);
  }

  Try<Msg> toMsg(int seqId, E cmd) {
    return transformer.serialize(cmd).map(value -> new Msg(cmd.stateId().id().value(), seqId, value));
  }

  Try<E> toCmd(Msg msg) {
    return transformer.deserialize(msg.value(), cClass);
  }
}
