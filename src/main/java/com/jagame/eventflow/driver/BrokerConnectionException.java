package com.jagame.eventflow.driver;

import com.jagame.eventflow.MessagingException;

public class BrokerConnectionException extends MessagingException {

    public BrokerConnectionException(Throwable cause) {
        super(cause);
    }

    public BrokerConnectionException(String message) {
        super(message);
    }

    public BrokerConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
