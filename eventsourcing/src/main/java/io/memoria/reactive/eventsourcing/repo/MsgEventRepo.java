package io.memoria.reactive.eventsourcing.repo;

import io.memoria.active.core.repo.msg.RepoException;
import io.memoria.active.core.repo.seq.SeqRow;
import io.memoria.active.core.repo.msg.SeqMsgRepo;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Event;
import io.vavr.collection.Stream;
import io.vavr.control.Try;

class MsgEventRepo<E extends Event> implements EventRepo<E> {

  private final SeqMsgRepo seqMsgRepo;
  private final Class<E> cClass;
  private final TextTransformer transformer;

  public MsgEventRepo(SeqMsgRepo seqMsgRepo, Class<E> cClass, TextTransformer transformer) {
    this.seqMsgRepo = seqMsgRepo;
    this.cClass = cClass;
    this.transformer = transformer;
  }

  @Override
  public Try<E> append(String topic, int seqId, E cmd) {
    return toMsg(seqId, cmd).flatMap(seqRow -> seqMsgRepo.append(topic, seqRow)).map(seqMsg -> cmd);
  }

  @Override
  public Stream<Try<E>> fetch(String topic, String aggId) throws RepoException {
    return seqMsgRepo.fetch(topic, aggId).map(this::toCmd);
  }

  Try<SeqRow> toMsg(int seqId, E cmd) {
    return transformer.serialize(cmd).map(value -> new SeqRow(cmd.stateId().id().value(), seqId, value));
  }

  Try<E> toCmd(SeqRow seqRow) {
    return transformer.deserialize(seqRow.value(), cClass);
  }
}
