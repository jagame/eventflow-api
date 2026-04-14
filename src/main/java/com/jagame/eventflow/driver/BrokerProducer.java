package com.jagame.eventflow.driver;

import com.jagame.eventflow.VesselProducer;
import io.cloudevents.CloudEvent;

import java.util.concurrent.CompletableFuture;

public interface BrokerProducer extends VesselProducer<CloudEvent> {
}
