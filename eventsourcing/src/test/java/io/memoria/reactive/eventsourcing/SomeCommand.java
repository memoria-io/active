package io.memoria.reactive.eventsourcing;

import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.CommandMeta;

record SomeCommand(CommandMeta meta) implements Command {}
