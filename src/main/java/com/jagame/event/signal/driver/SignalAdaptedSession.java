package com.jagame.event.signal.driver;

import com.jagame.event.ExchangeContext;
import com.jagame.event.broker.driver.BrokerException;
import com.jagame.event.broker.driver.BrokerSession;
import com.jagame.event.signal.Signal;
import com.jagame.event.signal.consumer.SignalAdaptedConsumer;
import com.jagame.event.signal.producer.SignalAdaptedProducer;
import io.cloudevents.CloudEvent;

import java.util.function.Function;

public class SignalAdaptedSession<T extends Signal, R extends Signal> implements ExchangeContext<T, R> {

    private final BrokerSession brokerSession;
    private final Function<T, CloudEvent> toCloudEventMapper;
    private final Function<CloudEvent, R> toSignalAdaptedMapper;

    public SignalAdaptedSession(BrokerSession brokerSession, Function<T, CloudEvent> toCloudEventMapper, Function<CloudEvent, R> toSignalAdaptedMapper) {
        this.brokerSession = brokerSession;
        this.toCloudEventMapper = toCloudEventMapper;
        this.toSignalAdaptedMapper = toSignalAdaptedMapper;
    }

    @Override
    public void beginTransaction() throws BrokerException {
        brokerSession.beginTransaction();
    }

    @Override
    public SignalAdaptedProducer<T> produce(String vessel) throws BrokerException {
        return new SignalAdaptedProducer<>(brokerSession.produce(vessel), toCloudEventMapper);
    }

    @Override
    public SignalAdaptedConsumer<R> consume(String vessel) throws BrokerException {
        return new SignalAdaptedConsumer<>(brokerSession.consume(vessel), toSignalAdaptedMapper);
    }

    @Override
    public void commit() throws BrokerException {
        brokerSession.commit();
    }

    @Override
    public void rollback() {
        brokerSession.rollback();
    }

    @Override
    public void close() {
        brokerSession.close();
    }
}
