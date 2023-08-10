package io.memoria.reactive.eventsourcing.stream;

import io.memoria.active.core.stream.BlockingStream;
import io.memoria.active.core.stream.Msg;
import io.memoria.active.core.stream.MsgResult;
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
    return toMsg(cmd).flatMap(msg -> stream.append(topic, partition, msg)).map(str -> cmd);
  }

  public Stream<Try<CommandResult>> stream(String topic, int partition) {
    return stream.stream(topic, partition).map(tr -> tr.flatMap(this::toCmd));
  }

  Try<CommandResult> toCmd(MsgResult result) {
    return transformer.deserialize(result.msg().value(), cClass).map(cmd -> new CommandResult(cmd, result.ack()));
  }

  Try<Msg> toMsg(C cmd) {
    return transformer.serialize(cmd).map(value -> new Msg(cmd.meta().commandId().value(), value));
  }
}

