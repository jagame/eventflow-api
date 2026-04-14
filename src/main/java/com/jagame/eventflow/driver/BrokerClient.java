package com.jagame.eventflow.driver;

import com.jagame.eventflow.MessagingClient;
import io.cloudevents.CloudEvent;

public interface BrokerClient extends MessagingClient<CloudEvent, CloudEvent> {

    @Override
    BrokerSession newSingleSession();

    @Override
    BrokerSession newGroupSession();

    @Override
    BrokerSession newGroupSession(String groupId);
}
