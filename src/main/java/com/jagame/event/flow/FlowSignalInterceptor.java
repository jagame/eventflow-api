package com.jagame.event.flow;

import com.jagame.common.SlimCompletionException;
import com.jagame.event.InboundStream;
import com.jagame.event.signal.Signal;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class FlowSignalInterceptor<R extends Signal> {

    private final InboundStream<R> consumer;

    public FlowSignalInterceptor(InboundStream<R> signalConsumer) {
        this.consumer = Objects.requireNonNull(signalConsumer);
    }

    public CompletableFuture<R> next() {
        return next(null);
    }

    public CompletableFuture<R> next(String flowId) {
        return slimNext(flowId).handle((res, ex) -> {
            if(res != null) {
                return res;
            }
            if(ex instanceof CancellationException) {
                throw (CancellationException) ex;
            }
            if (ex instanceof SlimCompletionException) {
                throw new SignalInterceptionException(ex.getCause());
            }
            // unknown, that's not supposed to happen
            throw new SignalInterceptionException(ex);
        });
    }

    protected CompletableFuture<R> slimNext(String flowId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                R signal;

                do {
                    signal = consumer.next().get();
                } while (flowId != null && flowId.equals(signal.flowId()));

                return signal;
            } catch (ExecutionException e) {
                throw new SlimCompletionException(e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SlimCompletionException(e);
            }
        });
    }

}
