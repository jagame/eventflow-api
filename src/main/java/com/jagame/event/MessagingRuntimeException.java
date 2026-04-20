package com.jagame.event;

import java.util.concurrent.CompletionException;

public class MessagingRuntimeException extends CompletionException {
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
