package io.memoria.active.web;

import io.helidon.common.http.Http.Status;

record DefaultResponse(Status status, String payload) implements Response {}
