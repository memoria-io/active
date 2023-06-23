package io.memoria.active.web;

import io.helidon.common.http.Http.Status;
import io.helidon.nima.webserver.http.*;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class HealthController implements HttpService {
  private static final Logger log = LoggerFactory.getLogger(HealthController.class.getName());
  private final Supplier<Try<String>> checkMessage;

  public HealthController(Supplier<Try<String>> checkMessage) {
    this.checkMessage = checkMessage;
  }

  @Override
  public void routing(HttpRules httpRules) {
    httpRules.get("/", this::health);
  }

  private void health(ServerRequest req, ServerResponse res) {
    var msgTry = checkMessage.get();
    if (msgTry.isSuccess()) {
      log.info("Health check succeeded: %s".formatted(msgTry.get()));
      res.status(Status.OK_200).send();
    } else {
      log.error("Health check failed: %s".formatted(msgTry.getCause().getMessage()));
      log.debug("Health check failed:", msgTry.getCause());
      res.status(Status.INTERNAL_SERVER_ERROR_500).send();
    }
  }
}
