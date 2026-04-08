package com.jagame.eventflow.signal.flow;

import com.jagame.eventflow.MessagingException;
import com.jagame.eventflow.driver.BrokerConnectionException;
import com.jagame.eventflow.signal.consumer.SignalInterceptionException;
import com.jagame.eventflow.signal.producer.SignalAdaptedProducer;

import java.util.concurrent.*;

public class SignalFlowExecutor<T extends StartFlowSignal, R extends EndFlowSignal> implements AutoCloseable {

    private final SignalFlowConfiguration<T, R> context;
    private final SignalAdaptedProducer<T> producer;

    SignalFlowExecutor(
            SignalFlowConfiguration<T, R> context,
            SignalAdaptedProducer<T> startSignalProducer
    ) {
        this.context = context;
        this.producer = startSignalProducer;
    }

    public static <T extends StartFlowSignal, R extends EndFlowSignal> SignalFlowExecutor<T, R> withConfiguration(
            SignalFlowConfiguration<T, R> context
    ) throws BrokerConnectionException {
        SignalAdaptedProducer<T> producer = context.newAdaptedProducer();
        return new SignalFlowExecutor<>(context, producer);
    }

    public R run(T startSignal) throws SignalFlowException, TimeoutException {
        try {
            return runAsync(startSignal).get(5, TimeUnit.MINUTES);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SignalFlowRuntimeException) {
                cause = cause.getCause();
            }
            throw new SignalFlowException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SignalFlowException(e.getCause());
        }
    }

    public CompletableFuture<R> runAsync(T startSignal) {
        return CompletableFuture.supplyAsync(() -> {
            try (FlowSignalInterceptor<R> signalInterceptor = newSignalInterceptor()) {
                CompletableFuture<R> endSignalRequest = signalInterceptor.next(startSignal.flowId());

                producer.send(context.productionTopic(), startSignal).get();
                return endSignalRequest.get();
            } catch (MessagingException e) {
                throw new SignalFlowRuntimeException(e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof SignalInterceptionException) {
                    cause = cause.getCause();
                }
                throw new SignalFlowRuntimeException(cause);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SignalFlowRuntimeException(e);
            }
        });
    }

    private FlowSignalInterceptor<R> newSignalInterceptor() throws MessagingException {
        return new FlowSignalInterceptor<>(context.consumptionTopic(), context.newAdaptedConsumer());
    }

    @Override
    public void close() {
        producer.close();
    }
}
