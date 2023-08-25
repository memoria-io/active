package io.memoria.reactive.eventsourcing;

import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.State;
import io.memoria.atom.eventsourcing.rule.Evolver;
import io.vavr.collection.Stream;
import io.vavr.control.Option;

public class Utils {
  private Utils() {}

  public static <S extends State, E extends Event> Option<S> reduce(Evolver<S, E> evolver, Stream<E> events) {
    return events.foldLeft(Option.none(), (Option<S> o, E e) -> applyOpt(evolver, o, e));
  }

  static <S extends State, E extends Event> Option<S> applyOpt(Evolver<S, E> evolver, Option<S> optState, E event) {
    return optState.map(s -> evolver.apply(s, event)).orElse(() -> Option.some(evolver.apply(event)));
  }
}
