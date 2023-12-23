package io.memoria.active.core.repo.stack;

import java.io.Serializable;

public record StackItem(StackItemId stackItemId, StackId stackId, int index, String value) implements Serializable {}
