package io.memoria.reactive.eventsourcing;

import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.State;
import io.memoria.atom.eventsourcing.rule.Evolver;
import io.vavr.control.Option;

public class PipelineUtils {

  public static <S extends State, E extends Event> Option<S> applyOpt(Evolver<S, E> evolver,
                                                                      Option<S> optState,
                                                                      E event) {
    return optState.map(s -> evolver.apply(s, event)).orElse(() -> Option.some(evolver.apply(event)));
  }

  private PipelineUtils() {}
}
