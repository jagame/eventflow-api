package com.jagame.eventflow.driver;

import com.jagame.eventflow.VesselConsumer;
import io.cloudevents.CloudEvent;

public interface BrokerConsumer extends VesselConsumer<CloudEvent> {
}
