package com.jagame.event.signal.driver;

import com.jagame.event.EventDriver;
import com.jagame.event.MessagingException;
import com.jagame.event.broker.driver.BrokerDriver;
import com.jagame.event.signal.Signal;
import io.cloudevents.CloudEvent;

import java.util.Properties;
import java.util.function.Function;

public class SignalAdaptedDriver<T extends Signal, R extends Signal> implements EventDriver<T, R> {

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
    public SignalAdaptedSource<T, R> connect(Properties properties) throws MessagingException {
        return new SignalAdaptedSource<>(
                realDriver.connect(properties),
                toCloudEventMapper,
                toSignalMapper
        );
    }
}
