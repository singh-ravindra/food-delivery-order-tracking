package com.fooddelivery.consumer;

import com.fooddelivery.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Notification Service consumer: sends push / email / SMS updates to the
 * customer for every state transition.
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    @KafkaListener(
            topics = "${app.kafka.topic:order-events}",
            groupId = "notification-service",
            containerFactory = "kafkaListenerContainerFactory")
    public void onOrderEvent(OrderEvent event) {
        String message = switch (event.getState()) {
            case PLACED          -> "Thanks! Your order has been placed.";
            case ACCEPTED        -> "Good news - the restaurant accepted your order.";
            case PREPARING       -> "Your food is being prepared.";
            case OUT_FOR_DELIVERY -> "Your order is on the way!";
            case DELIVERED       -> "Enjoy your meal! Order delivered.";
        };
        log.info("[Notification] -> customer of order {}: \"{}\"", event.getOrderId(), message);
    }
}

