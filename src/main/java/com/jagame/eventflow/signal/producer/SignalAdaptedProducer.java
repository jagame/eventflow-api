package com.jagame.eventflow.signal.producer;

import com.jagame.eventflow.MessageProducer;
import com.jagame.eventflow.driver.BrokerConnectionException;
import com.jagame.eventflow.driver.BrokerProducer;
import com.jagame.eventflow.signal.Signal;
import io.cloudevents.CloudEvent;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class SignalAdaptedProducer<T extends Signal> implements MessageProducer<T> {

    private final BrokerProducer realProducer;
    private final Function<T, CloudEvent> toCloudEventMapper;

    public SignalAdaptedProducer(BrokerProducer realProducer, Function<T, CloudEvent> toCloudEventMapper) {
        this.realProducer = realProducer;
        this.toCloudEventMapper = toCloudEventMapper;
    }

    @Override
    public void beginTransaction() throws BrokerConnectionException {
        realProducer.beginTransaction();
    }

    @Override
    public CompletableFuture<Void> send(String topic, T event) throws BrokerConnectionException {
        return realProducer.send(topic, toCloudEventMapper.apply(event));
    }

    @Override
    public void commit() throws BrokerConnectionException {
        realProducer.commit();
    }

    @Override
    public void rollback() {
        realProducer.rollback();
    }

    @Override
    public void close() {
        realProducer.close();
    }
}
