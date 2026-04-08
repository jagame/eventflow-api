package com.jagame.eventflow.signal.flow;

import com.jagame.eventflow.MessagingException;

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
