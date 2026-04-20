package com.jagame.event.flow;

import com.jagame.event.broker.driver.BrokerException;
import com.jagame.event.broker.driver.BrokerDriverManager;
import com.jagame.event.signal.Signal;
import com.jagame.event.signal.driver.SignalAdaptedSource;
import io.cloudevents.CloudEvent;

import java.util.Properties;
import java.util.function.Function;

public class SignalFlowConfiguration<T extends Signal, R extends Signal> {

    private final String productionTopic;
    private final Function<T, CloudEvent> producedSignalMapper;
    private final String consumptionTopic;
    private final Properties connectionProperties;
    private final Function<CloudEvent, R> consumedSignalMapper;

    private SignalFlowConfiguration(
            String productionTopic,
            Function<T, CloudEvent> producedSignalMapper,
            String consumptionTopic,
            Function<CloudEvent, R> consumedSignalMapper,
            Properties connectionProperties
    ) {
        this.productionTopic = productionTopic;
        this.producedSignalMapper = producedSignalMapper;
        this.consumptionTopic = consumptionTopic;
        this.consumedSignalMapper = consumedSignalMapper;
        this.connectionProperties = connectionProperties;
    }

    public String consumptionTopic() {
        return consumptionTopic;
    }

    public String productionTopic() {
        return productionTopic;
    }

    public SignalAdaptedSource<T, R> newAdaptedClient() throws BrokerException {
        return new SignalAdaptedSource<>(
                BrokerDriverManager.connect(connectionProperties),
                producedSignalMapper,
                consumedSignalMapper
        );
    }

    public static <T extends Signal, R extends Signal> Builder<T, R> builder() {
        return new Builder<>();
    }

    public static class Builder<T extends Signal, R extends Signal> {

        private static final Function<?, ?> NO_PRODUCER_MAPPER = ignore -> {throw new UnsupportedOperationException("No produced signal mapper is set");};
        private static final Function<?, ?> NO_CONSUMER_MAPPER = ignore -> {throw new UnsupportedOperationException("No consumed signal mapper is set");};

        private String consumptionTopic;
        private Function<CloudEvent, R> consumedSignalMapper;
        private String productionTopic;
        private Function<T, CloudEvent> producedSignalMapper;
        private Properties connectionProperties;

        private Builder() {}

        public Builder<T, R> productionTopic(String producerTopic) {
            this.productionTopic = producerTopic;
            return this;
        }

        public Builder<T, R> producedSignalMapper(Function<T, CloudEvent> producedSignalMapper) {
            this.producedSignalMapper = producedSignalMapper;
            return this;
        }

        public Builder<T, R> consumptionTopic(String consumerTopic) {
            this.consumptionTopic = consumerTopic;
            return this;
        }

        public Builder<T, R> consumedSignalMapper(Function<CloudEvent, R> consumedSignalMapper) {
            this.consumedSignalMapper = consumedSignalMapper;
            return this;
        }

        public Builder<T, R> connectionProperties(Properties connectionProperties) {
            this.connectionProperties = connectionProperties;
            return this;
        }

        public SignalFlowConfiguration<T, R> build() {
            if(producedSignalMapper == null) {
                @SuppressWarnings("unchecked")
                Function<T, CloudEvent> castedMapper = (Function<T, CloudEvent>) NO_PRODUCER_MAPPER;
                producedSignalMapper = castedMapper;
            }
            if(consumedSignalMapper == null) {
                @SuppressWarnings("unchecked")
                Function<CloudEvent, R> castedMapper = (Function<CloudEvent, R>) NO_CONSUMER_MAPPER;
                consumedSignalMapper = castedMapper;
            }
            return new SignalFlowConfiguration<>(
                    productionTopic,
                    producedSignalMapper,
                    consumptionTopic,
                    consumedSignalMapper,
                    connectionProperties
            );
        }

    }

}
