package io.memoria.active.core.stream;

record DefaultAck(Runnable acknowledge) implements Ack {}
