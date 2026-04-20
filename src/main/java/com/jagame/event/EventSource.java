package com.jagame.event;

import java.io.Closeable;

public interface EventSource<T, R> extends Closeable {

    default OutboundStream<T> produce(String vessel) throws MessagingException {
        return newSingleSession().produce(vessel);
    }

    default InboundStream<R> consume(String vessel) throws MessagingException {
        return newSingleSession().consume(vessel);
    }

    ExchangeContext<T, R> newSingleSession();

    ExchangeContext<T, R> newGroupSession();

    ExchangeContext<T, R> newGroupSession(String groupId);

    /**
     * Close this EventSource and all his active contexts
     */
    @Override
    void close();

}
