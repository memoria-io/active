package io.memoria.active.kafka;

import io.memoria.active.core.stream.Msg;
import io.memoria.atom.core.stream.BlockingStreamPublisher;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Command;
import io.vavr.control.Try;

public class CommandPublisher {
  private final String topic;
  private final int totalPartitions;
  private final BlockingStreamPublisher repo;
  private final TextTransformer transformer;

  public CommandPublisher(String topic,
                          int totalPartitions,
                          BlockingStreamPublisher repo,
                          TextTransformer transformer) {
    if (topic == null || topic.isEmpty()) {
      throw new IllegalArgumentException("Topic can't be null or empty");
    }
    if (totalPartitions < 1) {
      throw new IllegalArgumentException("Partitions count can't be less than 1");
    }
    this.topic = topic;
    this.totalPartitions = totalPartitions;
    this.repo = repo;
    this.transformer = transformer;
  }

  public Try<Command> publish(Command command) {
    return toMsg(command).flatMap(msg -> repo.publish(topic, command.partition(totalPartitions), msg))
                         .map(_ -> command);
  }

  private Try<Msg> toMsg(Command command) {
    return transformer.serialize(command).map(commandStr -> new Msg(command.meta().commandId().value(), commandStr));
  }
}
