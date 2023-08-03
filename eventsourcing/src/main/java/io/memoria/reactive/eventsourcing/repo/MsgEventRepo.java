package io.memoria.reactive.eventsourcing.repo;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.reactive.core.msg.stream.Msg;
import io.memoria.reactive.core.msg.stream.MsgStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static io.memoria.reactive.core.reactor.ReactorUtils.tryToMono;

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
  public Mono<E> last(String topic, int partition) {
    return msgRepo.last(topic, partition).flatMap(this::toCmd);
  }

  @Override
  public Mono<E> pub(String topic, int partition, E cmd) {
    return toMsg(cmd).flatMap(msg -> msgRepo.pub(topic, partition, msg)).map(msg -> cmd);
  }

  @Override
  public Flux<E> sub(String topic, int partition) {
    return msgRepo.sub(topic, partition).concatMap(this::toCmd);
  }

  Mono<Msg> toMsg(E cmd) {
    return tryToMono(() -> transformer.serialize(cmd)).map(value -> new Msg(cmd.commandId().id().value(), value));
  }

  Mono<E> toCmd(Msg msg) {
    return tryToMono(() -> transformer.deserialize(msg.value(), cClass));
  }
}
