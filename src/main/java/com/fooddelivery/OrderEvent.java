package com.fooddelivery;

import java.time.Instant;

/**
 * Domain event published every time an order transitions from one state to
 * another in the lifecycle:
 *
 *   PLACED -> ACCEPTED -> PREPARING -> OUT_FOR_DELIVERY -> DELIVERED
 */
public class OrderEvent {

    private String orderId;
    private OrderState previousState;
    private OrderState state;
    private Instant timestamp;

    public OrderEvent() {}

    public OrderEvent(String orderId, OrderState previousState, OrderState state, Instant timestamp) {
        this.orderId = orderId;
        this.previousState = previousState;
        this.state = state;
        this.timestamp = timestamp;
    }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public OrderState getPreviousState() { return previousState; }
    public void setPreviousState(OrderState previousState) { this.previousState = previousState; }

    public OrderState getState() { return state; }
    public void setState(OrderState state) { this.state = state; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", previousState=" + previousState +
                ", state=" + state +
                ", timestamp=" + timestamp +
                '}';
    }
}
