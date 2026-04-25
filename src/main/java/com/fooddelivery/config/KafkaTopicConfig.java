package com.fooddelivery.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the Kafka topic used by the order tracking service. Spring's
 * {@code KafkaAdmin} (auto-configured) will create the topic on startup using
 * the embedded broker.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.topic:order-events}")
    private String topic;

    @Value("${app.kafka.partitions:3}")
    private int partitions;

    @Value("${app.kafka.replication-factor:1}")
    private short replicationFactor;

    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(topic)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }
}

