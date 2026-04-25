package com.fooddelivery;

import com.fooddelivery.config.EmbeddedKafkaConfig;
import com.fooddelivery.config.EmbeddedKafkaHolder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FoodDeliveryOrderTrackingApplication {

    public static void main(String[] args) {
        // Boot the in-process Kafka broker BEFORE Spring starts so that the
        // dynamically-allocated bootstrap servers are available to Spring
        // Boot's Kafka auto-configuration via system property.
        EmbeddedKafkaHolder.start(EmbeddedKafkaConfig.TOPIC, 3);

        SpringApplication.run(FoodDeliveryOrderTrackingApplication.class, args);
    }
}
