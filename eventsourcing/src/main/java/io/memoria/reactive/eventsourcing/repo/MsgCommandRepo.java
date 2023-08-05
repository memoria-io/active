package io.memoria.reactive.eventsourcing.repo;

import io.memoria.active.core.repo.msg.Msg;
import io.memoria.active.core.repo.msg.MsgRepo;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.AtomUtils;
import io.vavr.collection.List;
import io.vavr.control.Try;

class MsgCommandRepo<C extends Command> implements CommandRepo<C> {

  private final MsgRepo msgRepo;
  private final Class<C> cClass;
  private final TextTransformer transformer;

  public MsgCommandRepo(MsgRepo msgRepo, Class<C> cClass, TextTransformer transformer) {
    this.msgRepo = msgRepo;
    this.cClass = cClass;
    this.transformer = transformer;
  }

  @Override
  public Try<C> append(String topic, int seqId, C cmd) {
    return toMsg(seqId, cmd).flatMap(msg -> msgRepo.append(topic, msg)).map(msg -> cmd);
  }

  @Override
  public Try<List<C>> getAll(String topic, String aggId) {
    return msgRepo.fetch(topic, aggId).map(list -> list.map(this::toCmd)).flatMap(AtomUtils::toListOfTry);
  }

  Try<Msg> toMsg(int seqId, C cmd) {
    return transformer.serialize(cmd).map(value -> new Msg(cmd.stateId().id().value(), seqId, value));
  }

  Try<C> toCmd(Msg msg) {
    return transformer.deserialize(msg.value(), cClass);
  }
}
