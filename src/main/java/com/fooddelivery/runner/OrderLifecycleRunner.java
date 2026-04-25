package com.fooddelivery.runner;

import com.fooddelivery.OrderState;
import com.fooddelivery.producer.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ApplicationArguments;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * On startup, drives a few sample orders through the complete lifecycle so
 * the Kafka pipeline can be observed end-to-end in the application logs.
 */
@Component
public class OrderLifecycleRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(OrderLifecycleRunner.class);

    private final OrderService orderService;
    private final KafkaListenerEndpointRegistry listenerRegistry;

    public OrderLifecycleRunner(OrderService orderService,
                                KafkaListenerEndpointRegistry listenerRegistry) {
        this.orderService = orderService;
        this.listenerRegistry = listenerRegistry;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        // Wait until every consumer has a partition assignment so the very
        // first events aren't published before any group is listening.
        awaitConsumerAssignment();

        List<String> orderIds = List.of(
                "order-" + shortId(),
                "order-" + shortId(),
                "order-" + shortId()
        );

        for (String orderId : orderIds) {
            log.info("======================================================");
            log.info(" Starting lifecycle for {}", orderId);
            log.info("======================================================");

            orderService.placeOrder(orderId);
            sleep(400);

            OrderState current = OrderState.PLACED;
            while (current != OrderState.DELIVERED) {
                orderService.advance(orderId, current);
                current = nextOf(current);
                sleep(400);
            }
        }

        // Give consumers a moment to flush the last messages to the logs.
        sleep(1500);
        log.info("All sample orders processed. Press Ctrl+C to exit.");
    }

    private void awaitConsumerAssignment() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            boolean allAssigned = true;
            for (MessageListenerContainer c : listenerRegistry.getListenerContainers()) {
                if (c.getAssignedPartitions() == null || c.getAssignedPartitions().isEmpty()) {
                    allAssigned = false;
                    break;
                }
            }
            if (allAssigned && !listenerRegistry.getListenerContainers().isEmpty()) {
                return;
            }
            Thread.sleep(200);
        }
        log.warn("Proceeding without confirmed consumer assignment (timed out).");
    }

    private static OrderState nextOf(OrderState s) {
        return switch (s) {
            case PLACED -> OrderState.ACCEPTED;
            case ACCEPTED -> OrderState.PREPARING;
            case PREPARING -> OrderState.OUT_FOR_DELIVERY;
            case OUT_FOR_DELIVERY -> OrderState.DELIVERED;
            case DELIVERED -> OrderState.DELIVERED;
        };
    }

    private static String shortId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}

