package io.memoria.active.core.repo;

import io.memoria.atom.core.id.Id;
import io.vavr.control.Try;

public interface IdRepo {
  Try<Id> add(Id id);

  Try<Boolean> exists(Id id);
}
