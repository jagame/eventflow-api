package com.jagame.eventflow.signal.flow;

import com.jagame.common.LightRuntimeException;
import com.jagame.eventflow.MessagingException;
import com.jagame.eventflow.driver.BrokerConnectionException;
import com.jagame.eventflow.signal.consumer.SignalAdaptedConsumer;
import com.jagame.eventflow.signal.driver.SignalAdaptedClient;
import com.jagame.eventflow.signal.driver.SignalAdaptedSession;
import com.jagame.eventflow.signal.producer.SignalAdaptedProducer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SignalFlowExecutor<T extends StartFlowSignal, R extends EndFlowSignal> implements AutoCloseable {

    private final SignalFlowConfiguration<T, R> configuration;
    private final SignalAdaptedClient<T, R> client;

    SignalFlowExecutor(
            SignalFlowConfiguration<T, R> configuration,
            SignalAdaptedClient<T, R> client
    ) {
        this.configuration = configuration;
        this.client = client;
    }

    public static <T extends StartFlowSignal, R extends EndFlowSignal> SignalFlowExecutor<T, R> withFlowConfiguration(
            SignalFlowConfiguration<T, R> configuration
    ) throws BrokerConnectionException {
        SignalAdaptedClient<T, R> adaptedClient = configuration.newAdaptedClient();
        return new SignalFlowExecutor<>(configuration, adaptedClient);
    }

    public R run(T startSignal) throws SignalFlowException, TimeoutException {
        try {
            return lightRun(startSignal);
        } catch (LightRuntimeException e) {
            throw new SignalFlowException(e.getCause());
        }
    }

    public CompletableFuture<R> runAsync(T startSignal) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return lightRun(startSignal);
            } catch (LightRuntimeException e) {
                throw new SignalFlowRuntimeException(e.getCause());
            } catch (TimeoutException e) {
                throw new SignalFlowRuntimeException(e);
            }
        });
    }

    protected R lightRun(T startSignal) throws TimeoutException {
        try (SignalAdaptedSession<T, R> session = client.newSingleSession()) {
            SignalAdaptedConsumer<R> consumer = session.consume(configuration.consumptionTopic());
            FlowSignalInterceptor<R> signalInterceptor = new FlowSignalInterceptor<>(consumer);
            SignalAdaptedProducer<T> producer = session.produce(configuration.productionTopic());

            CompletableFuture<R> endSignalRequest = signalInterceptor.lightNext(startSignal.flowId());
            producer.send(startSignal)
                    .whenComplete((unused, ex) -> {
                        if(ex != null) {
                            endSignalRequest.completeExceptionally(ex);
                        }
                    });

            return endSignalRequest.get(5, TimeUnit.MINUTES);
        } catch (MessagingException e) {
            throw new LightRuntimeException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof LightRuntimeException) {
                throw (LightRuntimeException) cause;
            }
            throw new LightRuntimeException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LightRuntimeException(e);
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
