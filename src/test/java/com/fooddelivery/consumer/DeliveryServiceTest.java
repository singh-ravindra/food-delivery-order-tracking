package com.fooddelivery.consumer;

import com.fooddelivery.OrderEvent;
import com.fooddelivery.OrderState;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatNoException;

class DeliveryServiceTest {

    private final DeliveryService service = new DeliveryService();

    @Test
    void onOrderEvent_handlesEveryStateWithoutError() {
        for (OrderState s : OrderState.values()) {
            OrderEvent ev = new OrderEvent("o-1", null, s, Instant.now());
            assertThatNoException().isThrownBy(() -> service.onOrderEvent(ev));
        }
    }
}

