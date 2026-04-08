package com.jagame.eventflow;

public interface MessageProducer<T> extends AutoCloseable {

    void beginTransaction() throws MessagingException;

    void send(String topic, T event) throws MessagingException;

    void commit() throws MessagingException;

    void rollback();

    @Override
    void close();

}
