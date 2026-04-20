package com.jagame.event.flow;

import com.jagame.event.MessagingRuntimeException;

public class SignalInterceptionException extends MessagingRuntimeException {
    public SignalInterceptionException(Throwable cause) {
        super(cause);
    }
}
