package com.jagame.eventflow.driver;

import com.jagame.common.LightRuntimeException;
import com.jagame.eventflow.MessagingDriver;
import com.jagame.eventflow.MessagingException;
import io.cloudevents.CloudEvent;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BrokerDriver implements MessagingDriver<CloudEvent, CloudEvent> {

    private static final Map<Properties, CompletableFuture<BrokerClient>> HIDDEN_CLIENTS = new ConcurrentHashMap<>();

    protected BrokerDriver() {
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                HIDDEN_CLIENTS.forEach((k, v) -> v.whenComplete((client, e) -> {
                    if(client != null) {
                        client.close();
                    }
                }))
        ));
    }

    @Override
    public BrokerSession newSingleSession(Properties properties) throws BrokerConnectionException {
        return computeHiddenClientIfAbsent(properties).newSingleSession();
    }

    @Override
    public BrokerSession newGroupSession(Properties properties) throws BrokerConnectionException {
        return computeHiddenClientIfAbsent(properties).newGroupSession();
    }

    @Override
    public BrokerSession newGroupSession(Properties properties, String groupId)
            throws MessagingException {
        return computeHiddenClientIfAbsent(properties).newGroupSession(groupId);
    }

    private BrokerClient computeHiddenClientIfAbsent(Properties properties) throws BrokerConnectionException {
        try {
            return HIDDEN_CLIENTS.computeIfAbsent(
                    properties, prop -> CompletableFuture.supplyAsync(() -> {
                        try {
                            return createClient(prop);
                        } catch (BrokerConnectionException e) {
                            throw new LightRuntimeException(e);
                        }
                    })
            ).join();
        } catch (CompletionException | CancellationException e) {
            HIDDEN_CLIENTS.remove(properties);
            Throwable cause = e.getCause();
            if(cause instanceof LightRuntimeException) {
                throw (BrokerConnectionException) cause.getCause();
            }
            throw new BrokerConnectionException(cause);
        }
    }

    @Override
    public abstract BrokerClient createClient(Properties properties) throws BrokerConnectionException;

    protected abstract boolean isCompliant(Properties properties);

}
