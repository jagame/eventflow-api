package com.jagame.event.flow;

import com.jagame.common.SlimCompletionException;
import com.jagame.event.broker.driver.BrokerException;
import com.jagame.event.signal.consumer.SignalAdaptedConsumer;
import com.jagame.event.signal.driver.SignalAdaptedSource;
import com.jagame.event.signal.driver.SignalAdaptedSession;
import com.jagame.event.signal.producer.SignalAdaptedProducer;

import java.util.concurrent.*;

public class SignalFlowExecutor<T extends StartFlowSignal, R extends EndFlowSignal> implements AutoCloseable {

    private final SignalFlowConfiguration<T, R> configuration;
    private final SignalAdaptedSource<T, R> client;

    SignalFlowExecutor(
            SignalFlowConfiguration<T, R> configuration,
            SignalAdaptedSource<T, R> client
    ) {
        this.configuration = configuration;
        this.client = client;
    }

    public static <T extends StartFlowSignal, R extends EndFlowSignal> SignalFlowExecutor<T, R> withFlowConfiguration(
            SignalFlowConfiguration<T, R> configuration
    ) throws BrokerException {
        SignalAdaptedSource<T, R> adaptedClient = configuration.newAdaptedClient();
        return new SignalFlowExecutor<>(configuration, adaptedClient);
    }

    public R run(T startSignal, long timeout, TimeUnit timeUnit)
            throws SignalFlowException, TimeoutException, InterruptedException {
        try {
            return runSlimAsync(startSignal).get(timeout, timeUnit);
        } catch (ExecutionException e) {
            Throwable cause = unwrap(e);
            if(cause instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw (InterruptedException) cause;
            }
            throw new SignalFlowException(cause);
        }
    }

    private Throwable unwrap(ExecutionException ex) {
        Throwable cause = ex.getCause();
        if(cause instanceof SlimCompletionException) {
            cause = cause.getCause();
        }
        return cause;
    }

    public CompletableFuture<R> runAsync(T startSignal) {
        return runSlimAsync(startSignal).handle((res, ex) -> {
            if (ex == null) {
                return res;
            }
            if(ex instanceof SlimCompletionException) {
                throw new SignalFlowRuntimeException(ex.getCause());
            }
            if(ex instanceof Error) {
                throw (Error) ex;
            }
            throw new IllegalStateException(ex);
        });
    }

    protected CompletableFuture<R> runSlimAsync(T startSignal) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return eternalRun(startSignal);
            } catch (BrokerException | RuntimeException e) {
                throw new SlimCompletionException(e);
            } catch (ExecutionException e) {
                throw new SlimCompletionException(e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SlimCompletionException(e);
            }
        });
    }

    protected R eternalRun(T startSignal) throws
            ExecutionException, InterruptedException, BrokerException
    {
        try (SignalAdaptedSession<T, R> session = client.newSingleSession()) {
            SignalAdaptedConsumer<R> consumer = session.consume(configuration.consumptionTopic());
            FlowSignalInterceptor<R> signalInterceptor = new FlowSignalInterceptor<>(consumer);
            SignalAdaptedProducer<T> producer = session.produce(configuration.productionTopic());

            CompletableFuture<R> endSignalRequest = signalInterceptor.slimNext(startSignal.flowId());
            producer.send(startSignal)
                    .whenComplete((unused, ex) -> {
                        if(ex != null) {
                            endSignalRequest.completeExceptionally(ex);
                        }
                    });

            return endSignalRequest.get();
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
