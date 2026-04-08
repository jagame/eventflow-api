package com.jagame.eventflow;

public interface MessageConsumer<R> extends AutoCloseable {

    void beginTransaction() throws MessagingException;

    R next(String topic) throws MessagingException;

    void commit() throws MessagingException;

    void rollback();

    @Override
    void close();

}
