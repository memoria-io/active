package io.memoria.active.web.auth;

import io.helidon.common.http.Http.Header;
import io.helidon.nima.webserver.http.ServerRequest;
import io.memoria.active.web.HttpUtils;
import io.vavr.control.Option;
import io.vavr.control.Try;

public class NimaAuthUtils {

  private NimaAuthUtils() {}

  public static Option<Credential> credential(ServerRequest req) {
    return Try.of(() -> req.headers().get(Header.AUTHORIZATION).value()).flatMap(HttpUtils::credential).toOption();
  }
}