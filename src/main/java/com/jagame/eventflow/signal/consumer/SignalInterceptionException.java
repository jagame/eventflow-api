package com.jagame.eventflow.signal.consumer;

import com.jagame.eventflow.MessagingRuntimeException;

public class SignalInterceptionException extends MessagingRuntimeException {
    public SignalInterceptionException(Throwable cause) {
        super(cause);
    }
}
