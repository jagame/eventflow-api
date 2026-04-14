package com.jagame.eventflow;

public interface MessagingSession<T, R> extends AutoCloseable {

    void beginTransaction() throws MessagingException;

    VesselProducer<T> produce(String vessel) throws MessagingException;

    VesselConsumer<R> consume(String vessel) throws MessagingException;

    void commit() throws MessagingException;

    void rollback();

    @Override
    void close();

}
