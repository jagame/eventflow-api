package com.jagame.eventflow.driver;

import com.jagame.eventflow.MessagingException;

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

    public static BrokerClient createClient(Properties properties) throws BrokerConnectionException {
        return compliantDriver(properties).createClient(properties);
    }

    public static BrokerSession newSingleSession(Properties properties) throws BrokerConnectionException {
        return createClient(properties).newSingleSession();
    }

    public static BrokerSession newGroupSession(Properties properties) throws BrokerConnectionException {
        return createClient(properties).newGroupSession();
    }

    public static BrokerSession newGroupSession(Properties properties, String groupId) throws BrokerConnectionException {
        return createClient(properties).newGroupSession(groupId);
    }

    public static BrokerDriver compliantDriver(Properties properties) {
        return BROKER_DRIVERS.stream()
                .filter(testedDriver -> testedDriver.isCompliant(properties))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No driver for the specified properties was found"));
    }

}
