package com.jagame.eventflow;

import java.util.concurrent.CompletableFuture;

public interface MessageConsumer<R> extends AutoCloseable {

    void beginTransaction() throws MessagingException;

    CompletableFuture<R> next(String topic) throws MessagingException;

    void commit() throws MessagingException;

    void rollback();

    @Override
    void close();

}
