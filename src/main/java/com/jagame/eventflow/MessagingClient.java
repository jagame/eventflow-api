package com.jagame.eventflow;

import java.io.Closeable;

public interface MessagingClient<T, R> extends Closeable {

    MessagingSession<T, R> newSingleSession();

    MessagingSession<T, R> newGroupSession();

    MessagingSession<T, R> newGroupSession(String groupId);

    @Override
    void close();
}
