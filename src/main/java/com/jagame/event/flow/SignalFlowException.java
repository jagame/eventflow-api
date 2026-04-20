package com.jagame.event.flow;

import com.jagame.event.MessagingException;

public class SignalFlowException extends MessagingException {
    public SignalFlowException(String message) {
        super(message);
    }

    public SignalFlowException(String message, Throwable cause) {
        super(message, cause);
    }

    public SignalFlowException(Throwable cause) {
        super(cause);
    }
}
