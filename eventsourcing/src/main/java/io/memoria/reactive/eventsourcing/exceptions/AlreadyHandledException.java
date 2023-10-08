package io.memoria.reactive.eventsourcing.exceptions;

import io.memoria.atom.eventsourcing.Command;

import java.util.NoSuchElementException;

public class AlreadyHandledException extends NoSuchElementException {
  public static AlreadyHandledException of(Command cmd) {
    return new AlreadyHandledException(message(cmd));
  }

  private AlreadyHandledException(String msg) {
    super(msg);
  }

  private static String message(Command cmd) {
    return STR. """
            \{ cmd.meta() } has already been handled""" ;
  }
}
