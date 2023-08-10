package io.memoria.active.core.stream;

public record MsgResult(Msg msg, Ack ack) {
  public MsgResult(String key, String value, Ack ack) {
    this(new Msg(key, value), ack);
  }
}
