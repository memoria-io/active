package io.memoria.active.web;

import io.memoria.active.web.auth.BasicCredential;
import io.memoria.active.web.auth.Credential;
import io.memoria.active.web.auth.Token;
import io.vavr.control.Try;

import java.util.Base64;
import java.util.NoSuchElementException;

public final class HttpUtils {

  private HttpUtils() {}

  public static Try<Credential> credential(String header) {
    var trimmedHeader = header.trim();
    if (trimmedHeader.contains("Basic")) {
      return Try.of(() -> {
        String content = trimmedHeader.split(" ")[1].trim();
        String[] basic = new String(Base64.getDecoder().decode(content)).split(":");
        return new BasicCredential(basic[0], basic[1]);
      });
    }
    if (trimmedHeader.contains("Bearer")) {
      return Try.of(() -> new Token(header.split(" ")[1].trim()));
    } else {
      return Try.failure(new NoSuchElementException());
    }
  }
}
