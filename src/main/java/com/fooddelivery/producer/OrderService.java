package com.fooddelivery.producer;

import com.fooddelivery.OrderEvent;
import com.fooddelivery.OrderState;
import com.fooddelivery.config.EmbeddedKafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Order Producer ("Order Service"). Owns the lifecycle state machine for an
 * order and publishes a Kafka event for every state transition. The orderId
 * is used as the message key so that all events for the same order land on
 * the same partition (preserving ordering per order).
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    /** Defines the legal next state for each current state. */
    private static final Map<OrderState, OrderState> NEXT_STATE = new EnumMap<>(OrderState.class);
    static {
        NEXT_STATE.put(OrderState.PLACED, OrderState.ACCEPTED);
        NEXT_STATE.put(OrderState.ACCEPTED, OrderState.PREPARING);
        NEXT_STATE.put(OrderState.PREPARING, OrderState.OUT_FOR_DELIVERY);
        NEXT_STATE.put(OrderState.OUT_FOR_DELIVERY, OrderState.DELIVERED);
    }

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderService(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /** Starts a new order at the {@code PLACED} state. */
    public OrderEvent placeOrder(String orderId) {
        return publish(orderId, null, OrderState.PLACED);
    }

    /** Advances an order from {@code current} to its successor state. */
    public OrderEvent advance(String orderId, OrderState current) {
        OrderState next = NEXT_STATE.get(current);
        if (next == null) {
            throw new IllegalStateException("No transition defined from " + current);
        }
        return publish(orderId, current, next);
    }

    private OrderEvent publish(String orderId, OrderState previous, OrderState next) {
        OrderEvent event = new OrderEvent(orderId, previous, next, Instant.now());

        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(EmbeddedKafkaConfig.TOPIC, orderId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[Producer] Failed to publish {}: {}", event, ex.getMessage(), ex);
            } else {
                log.info("[Producer] Published {} -> partition={} offset={}",
                        event,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return event;
    }
}

