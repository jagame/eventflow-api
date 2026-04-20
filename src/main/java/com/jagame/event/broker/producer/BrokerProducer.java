package com.jagame.event.broker.producer;

import com.jagame.event.OutboundStream;
import io.cloudevents.CloudEvent;

public interface BrokerProducer extends OutboundStream<CloudEvent> {
}
