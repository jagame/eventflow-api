package com.jagame.eventflow.driver;

import com.jagame.eventflow.MessagingSession;
import io.cloudevents.CloudEvent;

import java.util.concurrent.CompletableFuture;

public interface BrokerMessagingSession extends MessagingSession<CloudEvent, CloudEvent> {

    void beginTransaction() throws BrokerConnectionException;

    CompletableFuture<CloudEvent> next(String topic);

    void commit() throws BrokerConnectionException;

    void rollback();

    @Override
    void close();
}
