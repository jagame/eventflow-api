package com.jagame.eventflow.signal.consumer;

import com.jagame.eventflow.MessageConsumer;
import com.jagame.eventflow.MessagingException;
import com.jagame.eventflow.signal.Signal;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class SignalInterceptor<R extends Signal> implements AutoCloseable {

    private final String consumptionTopic;
    private final MessageConsumer<R> consumer;

    public SignalInterceptor(String consumptionTopic, MessageConsumer<R> signalConsumer) {
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
                    signal = consumer.next(consumptionTopic);
                } while (flowId != null && flowId.equals(signal.flowId()));

                return signal;
            } catch (MessagingException e) {
                throw new SignalInterceptionException(e);
            }
        });
    }

    @Override
    public void close() {
        consumer.close();
    }
}
