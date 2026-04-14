package com.jagame.eventflow;

import java.util.Properties;

public interface MessagingDriver<T, R> {

    MessagingClient<T, R> createClient(Properties properties) throws MessagingException;

    default MessagingSession<T, R> newSingleSession(Properties properties) throws MessagingException {
        return createClient(properties).newSingleSession();
    }

    default MessagingSession<T, R> newGroupSession(Properties properties) throws MessagingException {
        return createClient(properties).newGroupSession();
    }

    default MessagingSession<T, R> newGroupSession(Properties properties, String groupId) throws MessagingException {
        return createClient(properties).newGroupSession(groupId);
    }

}
