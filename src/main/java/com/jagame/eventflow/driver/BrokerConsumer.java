package com.jagame.eventflow.driver;

import com.jagame.eventflow.MessageConsumer;
import io.cloudevents.CloudEvent;

public interface BrokerConsumer extends MessageConsumer<CloudEvent> {

    void beginTransaction() throws BrokerConnectionException;

    CloudEvent next(String topic) throws BrokerConnectionException;

    void commit() throws BrokerConnectionException;

    void rollback();

    @Override
    void close();
}
