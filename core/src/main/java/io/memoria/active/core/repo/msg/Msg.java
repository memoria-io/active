package io.memoria.active.core.repo.msg;

import java.io.Serializable;

public record Msg(String aggId, int seqId, String value) implements Serializable {}
