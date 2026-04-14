package com.jagame.common;

public class LightRuntimeException extends RuntimeException {
    public LightRuntimeException(Throwable cause) {
        super(cause);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        // For a light wrapper we don't need the stack trace
        return this;
    }
}
