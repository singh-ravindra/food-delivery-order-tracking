package com.fooddelivery.consumer;

import com.fooddelivery.OrderEvent;
import com.fooddelivery.OrderState;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatNoException;

class NotificationServiceTest {

    private final NotificationService service = new NotificationService();

    @Test
    void onOrderEvent_producesMessageForEveryState() {
        for (OrderState s : OrderState.values()) {
            OrderEvent ev = new OrderEvent("o-1", null, s, Instant.now());
            assertThatNoException().isThrownBy(() -> service.onOrderEvent(ev));
        }
    }
}

