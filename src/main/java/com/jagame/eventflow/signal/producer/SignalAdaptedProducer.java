package com.jagame.eventflow.signal.producer;

import com.jagame.eventflow.VesselProducer;
import com.jagame.eventflow.driver.BrokerProducer;
import com.jagame.eventflow.signal.Signal;
import io.cloudevents.CloudEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SignalAdaptedProducer<T extends Signal> implements VesselProducer<T> {

    private final BrokerProducer realProducer;
    private final Function<T, CloudEvent> toCloudEventMapper;

    public SignalAdaptedProducer(BrokerProducer realProducer, Function<T, CloudEvent> toCloudEventMapper) {
        this.realProducer = realProducer;
        this.toCloudEventMapper = toCloudEventMapper;
    }

    @Override
    public String vessel() {
        return "";
    }

    @Override
    public CompletableFuture<Void> send(T events) {
        return realProducer.send(toCloudEventMapper.apply(events));
    }

    @Override
    public CompletableFuture<Void> send(List<T> events) {
        return realProducer.send(events
                .stream()
                .map(toCloudEventMapper)
                .collect(Collectors.toList())
        );
    }

}
