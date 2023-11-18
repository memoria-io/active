package io.memoria.active.web.auth;

import io.helidon.nima.webserver.http.ServerRequest;
import io.memoria.active.web.HttpUtils;
import io.vavr.control.Option;
import io.vavr.control.Try;

import static io.helidon.common.http.Http.Header.AUTHORIZATION;

public class HelidonAuthUtils {

  private HelidonAuthUtils() {}

  public static Option<Credential> credential(ServerRequest req) {
    return Try.of(() -> req.headers().get(AUTHORIZATION).value()).flatMap(HttpUtils::credential).toOption();
  }
}