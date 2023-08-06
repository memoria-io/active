package io.memoria.active.core.repo.seq;

import java.io.Serializable;

public record SeqRow(String aggId, int seqId, String value) implements Serializable {}
