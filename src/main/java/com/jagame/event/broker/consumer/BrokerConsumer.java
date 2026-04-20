package com.jagame.event.broker.consumer;

import com.jagame.event.InboundStream;
import io.cloudevents.CloudEvent;

public interface BrokerConsumer extends InboundStream<CloudEvent> {
}
