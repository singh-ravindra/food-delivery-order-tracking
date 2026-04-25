package com.fooddelivery.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

/** Enables {@code @KafkaListener} processing in the application. */
@Configuration
@EnableKafka
public class KafkaListenerEnabler {
}

