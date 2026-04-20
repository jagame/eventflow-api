package com.jagame.event.broker.driver;

import com.jagame.event.EventSource;
import com.jagame.event.MessagingException;
import com.jagame.event.broker.consumer.BrokerConsumer;
import com.jagame.event.broker.producer.BrokerProducer;
import io.cloudevents.CloudEvent;

public interface BrokerClient extends EventSource<CloudEvent, CloudEvent> {

    @Override
    default BrokerProducer produce(String vessel) throws MessagingException {
        return newSingleSession().produce(vessel);
    }

    @Override
    default BrokerConsumer consume(String vessel) throws MessagingException {
        return newSingleSession().consume(vessel);
    }

    @Override
    BrokerSession newSingleSession();

    @Override
    BrokerSession newGroupSession();

    @Override
    BrokerSession newGroupSession(String groupId);

}
