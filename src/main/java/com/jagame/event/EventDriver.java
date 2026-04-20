package com.jagame.event;

import java.util.Properties;

public interface EventDriver<T, R> {

    EventSource<T, R> connect(Properties properties) throws MessagingException;

}
