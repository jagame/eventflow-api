package com.jagame.event.broker.driver;

import com.jagame.event.MessagingException;

public class BrokerException extends MessagingException {

    public BrokerException(Throwable cause) {
        super(cause);
    }

    public BrokerException(String message) {
        super(message);
    }

    public BrokerException(String message, Throwable cause) {
        super(message, cause);
    }
}
