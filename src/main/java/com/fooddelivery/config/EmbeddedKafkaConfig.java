package com.fooddelivery.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

/**
 * Exposes the singleton {@link EmbeddedKafkaBroker} as a Spring bean so it can
 * be injected wherever needed and cleanly shut down by the container. The
 * broker itself must already have been started (see
 * {@code FoodDeliveryOrderTrackingApplication#main}) so its bootstrap servers
 * are available to Spring Boot's Kafka auto-configuration as the context
 * starts.
 */
@Configuration
public class EmbeddedKafkaConfig {

    public static final String TOPIC = "order-events";

    @Bean(destroyMethod = "destroy")
    public EmbeddedKafkaBroker embeddedKafkaBroker() {
        EmbeddedKafkaBroker broker = EmbeddedKafkaHolder.getBroker();
        if (broker == null) {
            throw new IllegalStateException(
                "Embedded Kafka broker has not been started. " +
                "Make sure FoodDeliveryOrderTrackingApplication.main() is the entry point.");
        }
        return broker;
    }
}
