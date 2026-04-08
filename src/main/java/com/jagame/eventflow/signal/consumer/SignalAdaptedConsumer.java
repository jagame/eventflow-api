package com.jagame.eventflow.signal.consumer;

import com.jagame.eventflow.MessageConsumer;
import com.jagame.eventflow.driver.BrokerConnectionException;
import com.jagame.eventflow.driver.BrokerConsumer;
import com.jagame.eventflow.signal.Signal;
import io.cloudevents.CloudEvent;

import java.util.function.Function;

public class SignalAdaptedConsumer<R extends Signal> implements MessageConsumer<R> {

    private final BrokerConsumer realConsumer;
    private final Function<CloudEvent, R> toSignalMapper;

    public SignalAdaptedConsumer(BrokerConsumer realConsumer, Function<CloudEvent, R> toSignalMapper) {
        this.realConsumer = realConsumer;
        this.toSignalMapper = toSignalMapper;
    }

    @Override
    public void beginTransaction() throws BrokerConnectionException {
        realConsumer.beginTransaction();
    }

    @Override
    public R next(String topic) throws BrokerConnectionException {
        return toSignalMapper.apply(realConsumer.next(topic));
    }

    @Override
    public void commit() throws BrokerConnectionException {
        realConsumer.commit();
    }

    @Override
    public void rollback() {
        realConsumer.rollback();
    }

    @Override
    public void close() {
        realConsumer.close();
    }
}
