package com.fooddelivery.consumer;

import com.fooddelivery.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Delivery Service consumer: assigns drivers and tracks GPS once an order
 * leaves the restaurant.
 */
@Service
public class DeliveryService {

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);

    @KafkaListener(
            topics = "${app.kafka.topic:order-events}",
            groupId = "delivery-service",
            containerFactory = "kafkaListenerContainerFactory")
    public void onOrderEvent(OrderEvent event) {
        switch (event.getState()) {
            case PREPARING ->
                    log.info("[Delivery]     Pre-assigning driver for order {}.", event.getOrderId());
            case OUT_FOR_DELIVERY ->
                    log.info("[Delivery]     Driver picked up order {}. GPS tracking started.", event.getOrderId());
            case DELIVERED ->
                    log.info("[Delivery]     Order {} delivered. Closing trip.", event.getOrderId());
            default ->
                    log.debug("[Delivery]     Ignoring state {} for order {}", event.getState(), event.getOrderId());
        }
    }
}

