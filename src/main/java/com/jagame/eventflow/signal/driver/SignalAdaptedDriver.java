package com.jagame.eventflow.signal.driver;

import com.jagame.eventflow.MessagingDriver;
import com.jagame.eventflow.driver.BrokerConnectionException;
import com.jagame.eventflow.driver.BrokerDriver;
import com.jagame.eventflow.signal.Signal;
import com.jagame.eventflow.signal.consumer.SignalAdaptedConsumer;
import com.jagame.eventflow.signal.producer.SignalAdaptedProducer;
import io.cloudevents.CloudEvent;

import java.util.Properties;
import java.util.function.Function;

public class SignalAdaptedDriver<T extends Signal, R extends Signal> implements MessagingDriver<T, R> {

    private final BrokerDriver realDriver;
    private final Function<T, CloudEvent> toCloudEventMapper;
    private final Function<CloudEvent, R> toSignalMapper;

    public SignalAdaptedDriver(
            BrokerDriver realDriver,
            Function<T, CloudEvent> toCloudEventMapper,
            Function<CloudEvent, R> toSignalMapper
    ) {
        this.realDriver = realDriver;
        this.toCloudEventMapper = toCloudEventMapper;
        this.toSignalMapper = toSignalMapper;
    }

    @Override
    public SignalAdaptedProducer<T> producer(Properties properties) throws BrokerConnectionException {
        return new SignalAdaptedProducer<>(realDriver.producer(properties), toCloudEventMapper);
    }

    @Override
    public SignalAdaptedConsumer<R> consumer(Properties properties) throws BrokerConnectionException {
        return new SignalAdaptedConsumer<>(realDriver.consumer(properties), toSignalMapper);
    }

}
