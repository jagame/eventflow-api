package com.jagame.common;

import java.util.concurrent.CompletionException;

public class SlimCompletionException extends CompletionException {
    public SlimCompletionException(Throwable cause) {
        super(cause instanceof SlimCompletionException ? cause.getCause() : cause);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        // For a light wrapper we don't need the stack trace
        return this;
    }
}
