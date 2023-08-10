package io.memoria.reactive.eventsourcing.stream;

import io.memoria.active.core.stream.Ack;
import io.memoria.atom.eventsourcing.Command;

public record CommandResult(Command command, Ack ack) {}
