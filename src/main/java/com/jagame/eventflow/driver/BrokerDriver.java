package com.jagame.eventflow.driver;

import com.jagame.eventflow.MessagingDriver;
import io.cloudevents.CloudEvent;

import java.util.Properties;

public abstract class BrokerDriver implements MessagingDriver<CloudEvent, CloudEvent> {

    public abstract BrokerProducer producer(Properties properties) throws BrokerConnectionException;

    public abstract BrokerConsumer consumer(Properties properties) throws BrokerConnectionException;

    protected abstract boolean isCompliant(Properties properties);

}
