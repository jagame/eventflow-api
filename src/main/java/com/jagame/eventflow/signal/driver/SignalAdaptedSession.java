package com.jagame.eventflow.signal.driver;

import com.jagame.eventflow.MessagingException;
import com.jagame.eventflow.MessagingSession;
import com.jagame.eventflow.VesselConsumer;
import com.jagame.eventflow.VesselProducer;
import com.jagame.eventflow.driver.BrokerSession;
import com.jagame.eventflow.signal.Signal;
import com.jagame.eventflow.signal.consumer.SignalAdaptedConsumer;
import com.jagame.eventflow.signal.producer.SignalAdaptedProducer;
import io.cloudevents.CloudEvent;

import java.util.function.Function;

public class SignalAdaptedSession<T extends Signal, R extends Signal> implements MessagingSession<T, R> {

    private final BrokerSession brokerSession;
    private final Function<T, CloudEvent> toCloudEventMapper;
    private final Function<CloudEvent, R> toSignalAdaptedMapper;

    public SignalAdaptedSession(BrokerSession brokerSession, Function<T, CloudEvent> toCloudEventMapper, Function<CloudEvent, R> toSignalAdaptedMapper) {
        this.brokerSession = brokerSession;
        this.toCloudEventMapper = toCloudEventMapper;
        this.toSignalAdaptedMapper = toSignalAdaptedMapper;
    }

    @Override
    public void beginTransaction() throws MessagingException {
        brokerSession.beginTransaction();
    }

    @Override
    public SignalAdaptedProducer<T> produce(String vessel) throws MessagingException {
        return new SignalAdaptedProducer<>(brokerSession.produce(vessel), toCloudEventMapper);
    }

    @Override
    public SignalAdaptedConsumer<R> consume(String vessel) throws MessagingException {
        return new SignalAdaptedConsumer<>(brokerSession.consume(vessel), toSignalAdaptedMapper);
    }

    @Override
    public void commit() throws MessagingException {
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
