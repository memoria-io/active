package io.memoria.reactive.eventsourcing.repo;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Command;
import io.memoria.reactive.core.msg.stream.Msg;
import io.memoria.reactive.core.msg.stream.MsgStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.memoria.reactive.core.reactor.ReactorUtils.tryToMono;

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
  public Mono<C> pub(String topic, int partition, C cmd) {
    return toMsg(cmd).flatMap(msg -> msgRepo.pub(topic, partition, msg)).map(msg -> cmd);
  }

  @Override
  public Flux<C> sub(String topic, int partition) {
    return msgRepo.sub(topic, partition).concatMap(this::toCmd);
  }

  Mono<Msg> toMsg(C cmd) {
    return tryToMono(() -> transformer.serialize(cmd)).map(value -> new Msg(cmd.commandId().id().value(), value));
  }

  Mono<C> toCmd(Msg msg) {
    return tryToMono(() -> transformer.deserialize(msg.value(), cClass));
  }
}
