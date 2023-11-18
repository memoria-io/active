package io.memoria.active.web;

import io.helidon.http.Status;

public interface Response {
  String payload();

  Status status();

  static Response of(Status status, String payload) {
    return new DefaultResponse(status, payload);
  }
}
