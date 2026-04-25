package com.fooddelivery.config;

import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;

/**
 * Static holder that owns the lifecycle of the in-process Kafka broker.
 * It is started from {@code main()} before Spring boots so the broker's
 * bootstrap-servers are already available when the Spring auto-configuration
 * for Kafka kicks in.
 */
public final class EmbeddedKafkaHolder {

    private static volatile EmbeddedKafkaBroker BROKER;

    private EmbeddedKafkaHolder() {
        // utility class
    }

    public static synchronized EmbeddedKafkaBroker start(String topic, int partitions) {
        if (BROKER != null) {
            return BROKER;
        }
        EmbeddedKafkaZKBroker broker = new EmbeddedKafkaZKBroker(1, true, partitions, topic);
        broker.brokerProperty("listeners", "PLAINTEXT://localhost:0");
        broker.brokerProperty("port", "0");
        broker.afterPropertiesSet();

        // Make Spring Boot Kafka auto-config pick this up.
        System.setProperty("spring.kafka.bootstrap-servers", broker.getBrokersAsString());

        BROKER = broker;
        return broker;
    }

    public static EmbeddedKafkaBroker getBroker() {
        return BROKER;
    }
}
