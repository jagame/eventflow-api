package com.jagame.eventflow.signal.flow;

import com.jagame.common.LightRuntimeException;
import com.jagame.eventflow.VesselConsumer;
import com.jagame.eventflow.signal.Signal;
import com.jagame.eventflow.signal.consumer.SignalInterceptionException;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class FlowSignalInterceptor<R extends Signal> {

    private final VesselConsumer<R> consumer;

    public FlowSignalInterceptor(VesselConsumer<R> signalConsumer) {
        this.consumer = Objects.requireNonNull(signalConsumer);
    }

    public CompletableFuture<R> next() {
        return next(null);
    }

    public CompletableFuture<R> next(String flowId) {
        return lightNext(flowId).handle((res, ex) -> {
            if(res != null) {
                return res;
            }
            if(ex instanceof CancellationException) {
                throw (CancellationException) ex;
            }
            if (ex instanceof LightRuntimeException) {
                throw new SignalInterceptionException(ex.getCause());
            }
            // unknown, that's not supposed to happen
            throw new SignalInterceptionException(ex);
        });
    }

    protected CompletableFuture<R> lightNext(String flowId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                R signal;

                do {
                    signal = consumer.next().get();
                } while (flowId != null && flowId.equals(signal.flowId()));

                return signal;
            } catch (ExecutionException e) {
                throw new LightRuntimeException(e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new LightRuntimeException(e);
            }
        });
    }

}
