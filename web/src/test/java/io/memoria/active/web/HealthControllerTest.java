package io.memoria.active.web;

import io.helidon.http.Method;
import io.helidon.webclient.http1.Http1Client;
import io.helidon.webserver.WebServer;
import io.vavr.control.Try;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class HealthControllerTest {
  private static final String ERROR_MSG = "The flip was false!";
  private static final String endpoint = "/health";
  private static final String host = "localhost";
  private static final int port = 8081;
  private static final AtomicBoolean serverCheckToggle = new AtomicBoolean(true);
  private static final HealthController healthController;
  private static final WebServer server;
  private static final Http1Client client;

  static {
    healthController = new HealthController(check());
    server = WebServer.builder()
                      .host(host)
                      .port(port)
                      .routing(routing -> routing.register("/health", healthController))
                      .build();
    client = Http1Client.builder().baseUri(URI.create("http://" + host + ":" + port)).build();
  }

  @BeforeAll
  static void beforeAll() {
    server.start();
  }

  @AfterAll
  static void afterAll() {
    server.stop();
  }

  @Test
  void failure() {
    // Given
    serverCheckToggle.set(false);
    // When
    try (var resp = client.method(Method.GET).path(endpoint).request()) {
      // Then
      assertThat(resp.status().code()).isEqualTo(500);
    }
  }

  @Test
  void success() {
    /// Given
    serverCheckToggle.set(true);
    // When
    try (var resp = client.method(Method.GET).uri("/health").request()) {
      // Then
      assertThat(resp.status().code()).isEqualTo(200);
    }
  }

  private static Supplier<Try<String>> check() {
    return () -> Try.of(serverCheckToggle::get)
                    .flatMap(b -> (b) ? Try.success("ok") : Try.failure(new Exception(ERROR_MSG)));
  }
}
