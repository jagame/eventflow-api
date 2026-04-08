package com.jagame.eventflow;

public class MessagingRuntimeException extends RuntimeException {
    public MessagingRuntimeException(String message) {
        super(message);
    }

    public MessagingRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public MessagingRuntimeException(Throwable cause) {
        super(cause);
    }
}
