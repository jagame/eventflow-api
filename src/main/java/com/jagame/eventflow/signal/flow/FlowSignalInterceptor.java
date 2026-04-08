package com.jagame.eventflow.signal.flow;

import com.jagame.eventflow.MessageConsumer;
import com.jagame.eventflow.MessagingException;
import com.jagame.eventflow.signal.Signal;
import com.jagame.eventflow.signal.consumer.SignalInterceptionException;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class FlowSignalInterceptor<R extends Signal> implements AutoCloseable {

    private final String consumptionTopic;
    private final MessageConsumer<R> consumer;

    public FlowSignalInterceptor(String consumptionTopic, MessageConsumer<R> signalConsumer) {
        this.consumptionTopic = consumptionTopic;
        this.consumer = Objects.requireNonNull(signalConsumer);
    }

    public CompletableFuture<R> next() {
        return next(null);
    }

    public CompletableFuture<R> next(String flowId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                R signal;

                do {
                    signal = consumer.next(consumptionTopic).get();
                } while (flowId != null && flowId.equals(signal.flowId()));

                return signal;
            } catch (MessagingException e) {
                throw new SignalInterceptionException(e);
            } catch (ExecutionException e) {
                throw new SignalInterceptionException(e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SignalInterceptionException(e);
            }
        });
    }

    @Override
    public void close() {
        consumer.close();
    }
}
