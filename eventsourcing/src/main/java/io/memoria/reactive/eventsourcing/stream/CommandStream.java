package io.memoria.reactive.eventsourcing.stream;

import io.memoria.active.core.stream.BlockingStream;
import io.memoria.active.core.stream.Msg;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Command;
import io.vavr.collection.Stream;
import io.vavr.control.Try;

public class CommandStream<C extends Command> {
  private final BlockingStream stream;
  private final Class<C> cClass;
  private final TextTransformer transformer;

  public CommandStream(BlockingStream stream, Class<C> cClass, TextTransformer transformer) {
    this.stream = stream;
    this.cClass = cClass;
    this.transformer = transformer;
  }

  public Try<C> append(String topic, int partition, C cmd) {
    return toMsg(cmd).flatMap(msg -> stream.publish(topic, partition, msg)).map(str -> cmd);
  }

  public Stream<Try<C>> stream(String topic, int partition) {
    var result = stream.fetch(topic, partition).map(tr -> tr.map(this::toCmd));
    if (result.isSuccess()) {
      return result.get();
    } else {
      return Stream.of(Try.failure(result.getCause()));
    }
  }

  Try<C> toCmd(Msg msg) {
    return transformer.deserialize(msg.value(), cClass);
  }

  Try<Msg> toMsg(C cmd) {
    return transformer.serialize(cmd).map(value -> new Msg(cmd.meta().commandId().value(), value));
  }
}

