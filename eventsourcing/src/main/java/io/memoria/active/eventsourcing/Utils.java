package io.memoria.active.eventsourcing;

import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.State;
import io.memoria.atom.eventsourcing.rule.Evolver;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.control.Option;

public class Utils {
  private Utils() {}

  public static Option<State> reduce(Evolver evolver, List<Event> events) {
    return events.foldLeft(Option.none(), (Option<State> o, Event e) -> applyOpt(evolver, o, e));
  }

  public static Option<State> reduce(Evolver evolver, Stream<Event> events) {
    return events.foldLeft(Option.none(), (Option<State> o, Event e) -> applyOpt(evolver, o, e));
  }

  static Option<State> applyOpt(Evolver evolver, Option<State> optState, Event event) {
    return optState.map(s -> evolver.apply(s, event)).orElse(() -> Option.some(evolver.apply(event)));
  }
}
