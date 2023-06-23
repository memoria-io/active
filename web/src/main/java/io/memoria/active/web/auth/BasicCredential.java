package io.memoria.active.web.auth;

public record BasicCredential(String username, String password) implements Credential {
  public BasicCredential {
    if (username == null || username.isEmpty() || password == null || password.isEmpty()) {
      throw new IllegalArgumentException("Invalid credentials, null or empty");
    }
  }
}
