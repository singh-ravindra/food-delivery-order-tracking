package com.fooddelivery.consumer;

import com.fooddelivery.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Restaurant Service consumer: accepts orders and prepares them.
 * Reacts to PLACED, ACCEPTED, PREPARING events.
 */
@Service
public class RestaurantService {

    private static final Logger log = LoggerFactory.getLogger(RestaurantService.class);

    @KafkaListener(
            topics = "${app.kafka.topic:order-events}",
            groupId = "restaurant-service",
            containerFactory = "kafkaListenerContainerFactory")
    public void onOrderEvent(OrderEvent event) {
        switch (event.getState()) {
            case PLACED -> log.info("[Restaurant]   New order received: {}. Reviewing...", event.getOrderId());
            case ACCEPTED -> log.info("[Restaurant]   Order {} accepted. Queuing for prep.", event.getOrderId());
            case PREPARING -> log.info("[Restaurant]   Cooking order {} now.", event.getOrderId());
            default -> log.debug("[Restaurant]   Ignoring state {} for order {}", event.getState(), event.getOrderId());
        }
    }
}
