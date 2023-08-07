package io.memoria.reactive.eventsourcing;

import io.vavr.collection.List;
import io.vavr.control.Option;
import io.vavr.control.Try;

import java.util.function.Function;

public class AtomUtils {

  private AtomUtils() {}

  public static <T, E> Function<Option<T>, Try<Option<E>>> optToTryOpt(Function<T, Try<E>> fn) {
    return opt -> optToTryOpt(opt, fn);
  }

  public static <T, E> Try<Option<E>> optToTryOpt(Option<T> option, Function<T, Try<E>> fn) {
    if (option.isEmpty()) {
      return Try.success(Option.none());
    } else {
      return fn.apply(option.get()).map(Option::some);
    }
  }

  public static <T> Try<List<T>> toListOfTry(List<Try<T>> list) {
    return Try.of(() -> list.map(Try::get));
  }
}
