package com.jagame.event.signal.producer;

import com.jagame.event.OutboundStream;
import com.jagame.event.broker.producer.BrokerProducer;
import com.jagame.event.signal.Signal;
import io.cloudevents.CloudEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SignalAdaptedProducer<T extends Signal> implements OutboundStream<T> {

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
