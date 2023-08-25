package io.memoria.reactive.eventsourcing.stream;

import io.memoria.atom.eventsourcing.Command;

public record CommandResult<C extends Command>(C command, Runnable acknowledge) {}
