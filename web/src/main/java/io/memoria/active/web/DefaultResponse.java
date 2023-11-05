package io.memoria.active.web;

import io.helidon.http.Status;

record DefaultResponse(Status status, String payload) implements Response {}
