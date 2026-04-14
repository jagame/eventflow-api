package com.jagame.eventflow.driver;

import com.jagame.eventflow.MessagingException;
import com.jagame.eventflow.MessagingSession;
import io.cloudevents.CloudEvent;

public interface BrokerSession extends MessagingSession<CloudEvent, CloudEvent> {

    @Override
    BrokerProducer produce(String vessel) throws MessagingException;

    @Override
    BrokerConsumer consume(String vessel) throws MessagingException;
}
