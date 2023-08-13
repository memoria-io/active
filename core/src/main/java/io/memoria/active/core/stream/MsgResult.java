package io.memoria.active.core.stream;

public final class MsgResult {
  private final Msg msg;
  private final Runnable acknowledge;

  public MsgResult(Msg msg, Runnable acknowledge) {
    this.msg = msg;
    this.acknowledge = acknowledge;
  }

  public MsgResult(String key, String value, Runnable acknowledge) {
    this.msg = new Msg(key, value);
    this.acknowledge = acknowledge;
  }

  public void ack() {
    acknowledge.run();
  }

  public Msg msg() {
    return msg;
  }
}
