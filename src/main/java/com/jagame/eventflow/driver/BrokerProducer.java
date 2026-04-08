package com.jagame.eventflow.driver;

import com.jagame.eventflow.MessageProducer;
import io.cloudevents.CloudEvent;

import java.util.concurrent.CompletableFuture;

public interface BrokerProducer extends MessageProducer<CloudEvent> {

    void beginTransaction() throws BrokerConnectionException;

    CompletableFuture<Void> send(String topic, CloudEvent cloudEvent) throws BrokerConnectionException;

    void commit() throws BrokerConnectionException;

    void rollback();

    @Override
    void close();
}
