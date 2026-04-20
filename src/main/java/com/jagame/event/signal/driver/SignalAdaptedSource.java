package com.jagame.event.signal.driver;

import com.jagame.event.EventSource;
import com.jagame.event.broker.driver.BrokerClient;
import com.jagame.event.signal.Signal;
import io.cloudevents.CloudEvent;

import java.util.function.Function;

public class SignalAdaptedSource<T extends Signal, R extends Signal> implements EventSource<T, R> {

    private final BrokerClient brokerClient;
    private final Function<T, CloudEvent> toCloudEventMapper;
    private final Function<CloudEvent, R> toSignalAdaptedMapper;

    public SignalAdaptedSource(
            BrokerClient brokerClient,
            Function<T, CloudEvent> toCloudEventMapper,
            Function<CloudEvent, R> toSignalAdaptedMapper
    ) {
        this.brokerClient = brokerClient;
        this.toCloudEventMapper = toCloudEventMapper;
        this.toSignalAdaptedMapper = toSignalAdaptedMapper;
    }

    @Override
    public SignalAdaptedSession<T, R> newSingleSession() {
        return new SignalAdaptedSession<>(
                brokerClient.newSingleSession(),
                toCloudEventMapper,
                toSignalAdaptedMapper
        );
    }

    @Override
    public SignalAdaptedSession<T, R> newGroupSession() {
        return new SignalAdaptedSession<>(
                brokerClient.newGroupSession(),
                toCloudEventMapper,
                toSignalAdaptedMapper
        );
    }

    @Override
    public SignalAdaptedSession<T, R> newGroupSession(String groupId) {
        return new SignalAdaptedSession<>(
                brokerClient.newGroupSession(groupId),
                toCloudEventMapper,
                toSignalAdaptedMapper
        );
    }

    @Override
    public void close() {
        brokerClient.close();
    }
}
