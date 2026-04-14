package com.jagame.eventflow.signal.consumer;

import com.jagame.eventflow.VesselConsumer;
import com.jagame.eventflow.signal.Signal;
import io.cloudevents.CloudEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SignalAdaptedConsumer<R extends Signal> implements VesselConsumer<R> {

    private final VesselConsumer<CloudEvent> realConsumer;
    private final Function<CloudEvent, R> toSignalMapper;

    public SignalAdaptedConsumer(VesselConsumer<CloudEvent> realConsumer, Function<CloudEvent, R> toSignalMapper) {
        this.realConsumer = realConsumer;
        this.toSignalMapper = toSignalMapper;
    }

    @Override
    public String vessel() {
        return realConsumer.vessel();
    }

    @Override
    public CompletableFuture<R> next() {
        return realConsumer.next().thenApply(toSignalMapper);
    }

    @Override
    public CompletableFuture<List<R>> next(int numMessages) {
        return realConsumer.next(numMessages).thenApply(cloudEvents ->
                cloudEvents
                        .stream()
                        .map(toSignalMapper)
                        .collect(Collectors.toList())
        );
    }
}
