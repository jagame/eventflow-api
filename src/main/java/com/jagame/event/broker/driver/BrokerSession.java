package com.jagame.event.broker.driver;

import com.jagame.event.ExchangeContext;
import com.jagame.event.broker.consumer.BrokerConsumer;
import com.jagame.event.broker.producer.BrokerProducer;
import io.cloudevents.CloudEvent;

public interface BrokerSession extends ExchangeContext<CloudEvent, CloudEvent> {

    @Override
    void beginTransaction() throws BrokerException;

    @Override
    BrokerProducer produce(String vessel) throws BrokerException;

    @Override
    BrokerConsumer consume(String vessel) throws BrokerException;

    @Override
    void commit() throws BrokerException;

}
