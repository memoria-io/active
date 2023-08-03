package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.State;
import io.memoria.atom.eventsourcing.rule.Evolver;

public interface Aggregate<S extends State,C extends Command,E extends Event> extends Evolver<S,E>{

}
