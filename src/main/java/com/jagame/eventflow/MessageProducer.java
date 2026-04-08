package com.jagame.eventflow;

import java.util.concurrent.CompletableFuture;

public interface MessageProducer<T> extends AutoCloseable {

    void beginTransaction() throws MessagingException;

    CompletableFuture<Void> send(String topic, T event) throws MessagingException;

    void commit() throws MessagingException;

    void rollback();

    @Override
    void close();

}
