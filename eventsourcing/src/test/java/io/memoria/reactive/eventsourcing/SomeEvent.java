package io.memoria.reactive.eventsourcing;

import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.EventMeta;

record SomeEvent(EventMeta meta) implements Event {}