package com.jagame.event.broker.driver;

import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BrokerDriverManager {

    private static final List<BrokerDriver> BROKER_DRIVERS;

    static {
        BROKER_DRIVERS = StreamSupport.stream(ServiceLoader.load(BrokerDriver.class).spliterator(), false)
                .collect(Collectors.toList());
    }

    private BrokerDriverManager() {}

    public static BrokerClient connect(Properties properties) throws BrokerException {
        return compliantDriver(properties).connect(properties);
    }

    public static BrokerDriver compliantDriver(Properties properties) {
        return BROKER_DRIVERS.stream()
                .filter(testedDriver -> testedDriver.isCompliant(properties))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No driver for the specified properties was found"));
    }

}
