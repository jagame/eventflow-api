package com.jagame.event;

public interface ExchangeContext<T, R> extends AutoCloseable {

    void beginTransaction() throws MessagingException;

    OutboundStream<T> produce(String vessel) throws MessagingException;

    InboundStream<R> consume(String vessel) throws MessagingException;

    void commit() throws MessagingException;

    void rollback();

    @Override
    void close();

}
