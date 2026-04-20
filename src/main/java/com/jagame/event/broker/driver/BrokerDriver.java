package com.jagame.event.broker.driver;

import com.jagame.event.EventDriver;
import io.cloudevents.CloudEvent;

import java.util.Properties;

public abstract class BrokerDriver implements EventDriver<CloudEvent, CloudEvent> {

    @Override
    public abstract BrokerClient connect(Properties properties) throws BrokerException;

    protected abstract boolean isCompliant(Properties properties);

}
