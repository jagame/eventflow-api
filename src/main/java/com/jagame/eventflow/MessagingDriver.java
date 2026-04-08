package com.jagame.eventflow;

import java.util.Properties;

public interface MessagingDriver<T, R> {

    MessageProducer<T> producer(Properties properties) throws MessagingException;

    MessageConsumer<R> consumer(Properties properties) throws MessagingException;

}
