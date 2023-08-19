package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.eventsourcing.Command;

public record CommandResult(Command command, Runnable acknowledge) {}
